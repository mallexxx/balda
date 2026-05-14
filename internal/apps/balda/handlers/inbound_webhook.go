package handlers

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	relaychannel "github.com/normahq/balda/internal/apps/balda/channel"
	relaytelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	relaysession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	"google.golang.org/adk/runner"
)

const (
	defaultInboundWebhookListenAddr = "127.0.0.1:8090"
	defaultInboundWebhookPath       = "/inbound/webhook"

	inboundWebhookReadHeaderTimeout = 5 * time.Second
	inboundWebhookReadTimeout       = 10 * time.Second
	inboundWebhookWriteTimeout      = 30 * time.Second
	inboundWebhookIdleTimeout       = 60 * time.Second
	inboundWebhookMaxBodyBytes      = 1 << 20
)

const (
	inboundWebhookStatusAccepted = "accepted"
	inboundWebhookStatusError    = "error"

	inboundWebhookCodeUnauthorized      = "unauthorized"
	inboundWebhookCodeInvalidMethod     = "invalid_method"
	inboundWebhookCodeInvalidJSON       = "invalid_json"
	inboundWebhookCodeInvalidPayload    = "invalid_payload"
	inboundWebhookCodeUnsupportedTarget = "unsupported_target"
	inboundWebhookCodeSessionNotFound   = "session_not_found"
	inboundWebhookCodeQueueFull         = "queue_full"
	inboundWebhookCodeDispatchFailed    = "dispatch_failed"
)

type inboundWebhookSessionManager interface {
	GetSession(locator relaysession.SessionLocator) (*relaysession.TopicSession, error)
	GetSessionInfo(ctx context.Context, sessionID string) (relaysession.TopicSessionInfo, error)
	RestoreSession(ctx context.Context, sessionCtx relaysession.SessionContext) (*relaysession.TopicSession, error)
}

type inboundTurnExecutor interface {
	runTurnTask(
		ctx context.Context,
		text string,
		r *runner.Runner,
		userID string,
		sessionID string,
		agentSessionID string,
		locator relaysession.SessionLocator,
		messageID int,
		topicID int,
		progressPolicy relaychannel.ProgressPolicy,
	) error
}

type inboundWebhookParams struct {
	fx.In

	LC         fx.Lifecycle
	Enabled    bool   `name:"relay_inbound_webhooks_enabled"`
	ListenAddr string `name:"relay_inbound_webhooks_listen_addr"`
	Path       string `name:"relay_inbound_webhooks_path"`
	AuthToken  string `name:"relay_inbound_webhooks_auth_token"`
	Sessions   *relaysession.Manager
	Dispatcher *TurnDispatcher
	Relay      *RelayHandler
	Logger     zerolog.Logger
}

// InboundWebhookReceiver receives authenticated inbound webhook prompts and dispatches them into session turns.
type InboundWebhookReceiver struct {
	enabled    bool
	listenAddr string
	path       string
	authToken  string
	sessions   inboundWebhookSessionManager
	dispatch   turnQueue
	relay      inboundTurnExecutor
	logger     zerolog.Logger

	metrics inboundWebhookMetrics

	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
	started  bool
}

type inboundWebhookMetrics struct {
	accepted     atomic.Uint64
	unauthorized atomic.Uint64
	invalid      atomic.Uint64
	notFound     atomic.Uint64
	queueFull    atomic.Uint64
	dispatchErr  atomic.Uint64
}

type inboundWebhookRequest struct {
	RequestID string               `json:"request_id,omitempty"`
	Prompt    string               `json:"prompt"`
	Target    inboundWebhookTarget `json:"target"`
}

type inboundWebhookTarget struct {
	SessionID string                 `json:"session_id,omitempty"`
	Locator   *inboundWebhookLocator `json:"locator,omitempty"`
}

type inboundWebhookLocator struct {
	ChannelType string `json:"channel_type"`
	AddressKey  string `json:"address_key"`
	AddressJSON string `json:"address_json"`
	SessionID   string `json:"session_id"`
}

type inboundWebhookAcceptedResponse struct {
	Status        string `json:"status"`
	RequestID     string `json:"request_id"`
	SessionID     string `json:"session_id"`
	QueuePosition int    `json:"queue_position"`
}

type inboundWebhookErrorResponse struct {
	Status    string                    `json:"status"`
	RequestID string                    `json:"request_id"`
	Error     inboundWebhookErrorDetail `json:"error"`
}

type inboundWebhookErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type inboundWebhookHTTPError struct {
	status  int
	code    string
	message string
	cause   error
}

func (e *inboundWebhookHTTPError) Error() string {
	if e == nil {
		return ""
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.message
}

func newInboundWebhookHTTPError(status int, code, message string, cause error) *inboundWebhookHTTPError {
	return &inboundWebhookHTTPError{
		status:  status,
		code:    code,
		message: message,
		cause:   cause,
	}
}

func NewInboundWebhookReceiver(params inboundWebhookParams) (*InboundWebhookReceiver, error) {
	receiver := &InboundWebhookReceiver{
		enabled:    params.Enabled,
		listenAddr: normalizeInboundWebhookListenAddr(params.ListenAddr),
		path:       normalizeInboundWebhookPath(params.Path),
		authToken:  strings.TrimSpace(params.AuthToken),
		sessions:   params.Sessions,
		dispatch:   params.Dispatcher,
		relay:      params.Relay,
		logger:     params.Logger.With().Str("component", "balda.inbound_webhook").Logger(),
	}

	if !receiver.enabled {
		return receiver, nil
	}
	if receiver.sessions == nil {
		return nil, fmt.Errorf("balda session manager is required for inbound webhooks")
	}
	if receiver.dispatch == nil {
		return nil, fmt.Errorf("balda turn dispatcher is required for inbound webhooks")
	}
	if receiver.relay == nil {
		return nil, fmt.Errorf("balda relay handler is required for inbound webhooks")
	}
	if receiver.authToken == "" {
		return nil, fmt.Errorf("balda.inbound_webhooks.auth_token is required when inbound webhooks are enabled")
	}

	params.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return receiver.start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return receiver.stop(ctx)
		},
	})

	return receiver, nil
}

func normalizeInboundWebhookListenAddr(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultInboundWebhookListenAddr
	}
	return trimmed
}

func normalizeInboundWebhookPath(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultInboundWebhookPath
	}
	if !strings.HasPrefix(trimmed, "/") {
		return "/" + trimmed
	}
	return trimmed
}

func (r *InboundWebhookReceiver) start(_ context.Context) error {
	if !r.enabled {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return nil
	}

	listener, err := net.Listen("tcp", r.listenAddr)
	if err != nil {
		return fmt.Errorf("listen inbound webhook on %q: %w", r.listenAddr, err)
	}

	mux := http.NewServeMux()
	mux.Handle(r.path, http.HandlerFunc(r.handleInboundWebhook))
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: inboundWebhookReadHeaderTimeout,
		ReadTimeout:       inboundWebhookReadTimeout,
		WriteTimeout:      inboundWebhookWriteTimeout,
		IdleTimeout:       inboundWebhookIdleTimeout,
	}

	r.listener = listener
	r.server = server
	r.started = true

	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			r.logger.Error().Err(serveErr).Msg("inbound webhook server failed")
		}
	}()

	r.logger.Info().
		Str("listen_addr", listener.Addr().String()).
		Str("path", r.path).
		Msg("inbound webhook server started")
	return nil
}

func (r *InboundWebhookReceiver) stop(ctx context.Context) error {
	r.mu.Lock()
	server := r.server
	r.server = nil
	r.listener = nil
	r.started = false
	r.mu.Unlock()

	if server == nil {
		return nil
	}
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown inbound webhook server: %w", err)
	}
	return nil
}

func (r *InboundWebhookReceiver) handleInboundWebhook(w http.ResponseWriter, req *http.Request) {
	requestID := strings.TrimSpace(req.Header.Get("X-Request-Id"))
	if req.Method != http.MethodPost {
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusMethodNotAllowed,
			inboundWebhookCodeInvalidMethod,
			"method must be POST",
			nil,
		))
		return
	}

	if !validBearerToken(req.Header.Get("Authorization"), r.authToken) {
		r.metrics.unauthorized.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusUnauthorized,
			inboundWebhookCodeUnauthorized,
			"unauthorized",
			nil,
		))
		return
	}

	payload, err := decodeInboundWebhookRequest(req.Body)
	if err != nil {
		r.metrics.invalid.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidJSON,
			"invalid JSON payload",
			err,
		))
		return
	}
	if requestID == "" {
		requestID = strings.TrimSpace(payload.RequestID)
	}
	if requestID == "" {
		requestID = fmt.Sprintf("inbound-%d", time.Now().UnixNano())
	}

	prompt := strings.TrimSpace(payload.Prompt)
	if prompt == "" {
		r.metrics.invalid.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidPayload,
			"prompt is required",
			nil,
		))
		return
	}

	locator, topicID, ts, resolveErr := r.resolveInboundWebhookTarget(req.Context(), payload.Target)
	if resolveErr != nil {
		if resolveErr.status == http.StatusNotFound {
			r.metrics.notFound.Add(1)
		} else {
			r.metrics.invalid.Add(1)
		}
		r.writeInboundWebhookError(w, requestID, resolveErr)
		return
	}

	position, enqueueErr := r.dispatch.Enqueue(TurnTask{
		SessionID: ts.GetSessionID(),
		Run: func(runCtx context.Context) error {
			if _, getErr := r.sessions.GetSession(locator); getErr != nil {
				r.logger.Debug().
					Str("request_id", requestID).
					Str("session_id", locator.SessionID).
					Msg("dropping inbound webhook turn for inactive session")
				return nil
			}

			return r.relay.runTurnTask(
				runCtx,
				prompt,
				ts.GetRunner(),
				ts.GetUserID(),
				ts.GetSessionID(),
				ts.GetAgentSessionID(),
				locator,
				0,
				topicID,
				inboundWebhookProgressPolicy(),
			)
		},
	})
	if enqueueErr != nil {
		if errors.Is(enqueueErr, ErrTurnQueueFull) {
			r.metrics.queueFull.Add(1)
			r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
				http.StatusTooManyRequests,
				inboundWebhookCodeQueueFull,
				"turn queue is full",
				enqueueErr,
			))
			return
		}

		r.metrics.dispatchErr.Add(1)
		r.writeInboundWebhookError(w, requestID, newInboundWebhookHTTPError(
			http.StatusInternalServerError,
			inboundWebhookCodeDispatchFailed,
			"failed to dispatch inbound turn",
			enqueueErr,
		))
		return
	}

	r.metrics.accepted.Add(1)
	r.logger.Info().
		Str("request_id", requestID).
		Str("session_id", locator.SessionID).
		Str("channel_type", locator.ChannelType).
		Str("address_key", locator.AddressKey).
		Int("queue_position", position).
		Msg("inbound webhook accepted")

	writeInboundWebhookJSON(w, http.StatusAccepted, inboundWebhookAcceptedResponse{
		Status:        inboundWebhookStatusAccepted,
		RequestID:     requestID,
		SessionID:     locator.SessionID,
		QueuePosition: position,
	})
}

func decodeInboundWebhookRequest(body io.ReadCloser) (inboundWebhookRequest, error) {
	defer func() { _ = body.Close() }()

	reader := io.LimitReader(body, inboundWebhookMaxBodyBytes)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var payload inboundWebhookRequest
	if err := decoder.Decode(&payload); err != nil {
		return inboundWebhookRequest{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return inboundWebhookRequest{}, fmt.Errorf("extra trailing JSON values")
		}
		return inboundWebhookRequest{}, err
	}
	return payload, nil
}

func (r *InboundWebhookReceiver) resolveInboundWebhookTarget(
	ctx context.Context,
	target inboundWebhookTarget,
) (relaysession.SessionLocator, int, *relaysession.TopicSession, *inboundWebhookHTTPError) {
	sessionID := strings.TrimSpace(target.SessionID)
	locator, err := locatorFromInboundTarget(target.Locator)
	if err != nil {
		return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidPayload,
			err.Error(),
			err,
		)
	}
	if sessionID == "" && locator.SessionID == "" {
		return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidPayload,
			"target.session_id or target.locator is required",
			nil,
		)
	}

	var info relaysession.TopicSessionInfo
	if sessionID != "" {
		info, err = r.sessions.GetSessionInfo(ctx, sessionID)
		if err != nil {
			return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
				http.StatusNotFound,
				inboundWebhookCodeSessionNotFound,
				fmt.Sprintf("session %q not found", sessionID),
				err,
			)
		}
		if locator.SessionID == "" {
			locator = info.Locator
		}
	}

	if locator.SessionID == "" {
		return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidPayload,
			"target locator session_id is required",
			nil,
		)
	}
	if sessionID != "" && locator.SessionID != sessionID {
		return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidPayload,
			"target.session_id does not match target.locator.session_id",
			nil,
		)
	}
	if strings.TrimSpace(locator.ChannelType) != relaytelegram.NewLocator(0, 0).ChannelType {
		return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeUnsupportedTarget,
			fmt.Sprintf("unsupported channel type %q", locator.ChannelType),
			nil,
		)
	}

	ts, err := r.sessions.GetSession(locator)
	if err != nil {
		if strings.TrimSpace(info.SessionID) == "" {
			info, err = r.sessions.GetSessionInfo(ctx, locator.SessionID)
			if err != nil {
				return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
					http.StatusNotFound,
					inboundWebhookCodeSessionNotFound,
					fmt.Sprintf("session %q not found", locator.SessionID),
					err,
				)
			}
		}
		userID := strings.TrimSpace(info.UserID)
		if userID == "" {
			return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
				http.StatusBadRequest,
				inboundWebhookCodeInvalidPayload,
				fmt.Sprintf("session %q has no user id for restore", locator.SessionID),
				nil,
			)
		}
		ts, err = r.sessions.RestoreSession(ctx, relaysession.SessionContext{
			Locator: locator,
			UserID:  userID,
		})
		if err != nil {
			return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
				http.StatusNotFound,
				inboundWebhookCodeSessionNotFound,
				fmt.Sprintf("session %q restore failed", locator.SessionID),
				err,
			)
		}
	}

	address, ok, err := relaytelegram.DecodeLocator(locator)
	if err != nil {
		return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeInvalidPayload,
			"invalid locator address payload",
			err,
		)
	}
	if !ok {
		return relaysession.SessionLocator{}, 0, nil, newInboundWebhookHTTPError(
			http.StatusBadRequest,
			inboundWebhookCodeUnsupportedTarget,
			fmt.Sprintf("unsupported channel type %q", locator.ChannelType),
			nil,
		)
	}

	return locator, address.TopicID, ts, nil
}

func locatorFromInboundTarget(raw *inboundWebhookLocator) (relaysession.SessionLocator, error) {
	if raw == nil {
		return relaysession.SessionLocator{}, nil
	}
	return relaysession.NewSessionLocator(raw.ChannelType, raw.AddressKey, raw.AddressJSON, raw.SessionID)
}

func (r *InboundWebhookReceiver) writeInboundWebhookError(
	w http.ResponseWriter,
	requestID string,
	handlerErr *inboundWebhookHTTPError,
) {
	if handlerErr == nil {
		handlerErr = newInboundWebhookHTTPError(
			http.StatusInternalServerError,
			inboundWebhookCodeDispatchFailed,
			"internal error",
			nil,
		)
	}

	evt := r.logger.Warn().
		Str("request_id", requestID).
		Str("error_code", handlerErr.code).
		Int("status_code", handlerErr.status)
	if handlerErr.cause != nil {
		evt = evt.Err(handlerErr.cause)
	}
	evt.Msg("inbound webhook rejected")

	writeInboundWebhookJSON(w, handlerErr.status, inboundWebhookErrorResponse{
		Status:    inboundWebhookStatusError,
		RequestID: requestID,
		Error: inboundWebhookErrorDetail{
			Code:    handlerErr.code,
			Message: handlerErr.message,
		},
	})
}

func writeInboundWebhookJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		return
	}
}

func validBearerToken(headerValue string, expectedToken string) bool {
	expected := strings.TrimSpace(expectedToken)
	if expected == "" {
		return false
	}

	parts := strings.Fields(strings.TrimSpace(headerValue))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	actual := strings.TrimSpace(parts[1])
	if len(actual) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

func inboundWebhookProgressPolicy() relaychannel.ProgressPolicy {
	return relaychannel.ProgressPolicy{
		Typing:   false,
		Thinking: false,
	}
}
