package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	relaychannel "github.com/normahq/balda/internal/apps/balda/channel"
	relaytelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	relaysession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
	"google.golang.org/adk/runner"
)

func TestNewInboundWebhookReceiver_RequiresAuthTokenWhenEnabled(t *testing.T) {
	t.Parallel()

	_, err := NewInboundWebhookReceiver(inboundWebhookParams{
		Enabled:    true,
		Sessions:   &relaysession.Manager{},
		Dispatcher: &TurnDispatcher{},
		Relay:      &RelayHandler{},
		Logger:     zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("NewInboundWebhookReceiver() error = nil, want auth token validation error")
	}
}

func TestInboundWebhookReceiver_Unauthorized(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest()
	req := httptest.NewRequest(http.MethodPost, "/inbound/webhook", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusUnauthorized, inboundWebhookCodeUnauthorized)
}

func TestInboundWebhookReceiver_InvalidJSON(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest()
	req := httptest.NewRequest(http.MethodPost, "/inbound/webhook", bytes.NewBufferString(`{"prompt":`))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusBadRequest, inboundWebhookCodeInvalidJSON)
}

func TestInboundWebhookReceiver_InvalidPayload(t *testing.T) {
	t.Parallel()

	receiver := newInboundWebhookReceiverForTest()
	req := httptest.NewRequest(http.MethodPost, "/inbound/webhook", bytes.NewBufferString(`{"prompt":"","target":{}}`))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusBadRequest, inboundWebhookCodeInvalidPayload)
}

func TestInboundWebhookReceiver_SessionNotFound(t *testing.T) {
	t.Parallel()

	sessionMgr := &fakeInboundSessionManager{
		infoErr: errors.New("missing session"),
	}
	receiver := newInboundWebhookReceiverForTest()
	receiver.sessions = sessionMgr

	body := `{"prompt":"hello","target":{"session_id":"tg-9001-77"}}`
	req := httptest.NewRequest(http.MethodPost, "/inbound/webhook", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusNotFound, inboundWebhookCodeSessionNotFound)
}

func TestInboundWebhookReceiver_QueueFull(t *testing.T) {
	t.Parallel()

	locator := relaytelegram.NewLocator(9001, 77)
	ts := newSchedulerTopicSession(t, locator, "tg-101", locator.SessionID, nil)

	sessionMgr := &fakeInboundSessionManager{
		session: ts,
	}
	queue := &fakeInboundTurnQueue{
		enqueueErr: ErrTurnQueueFull,
	}
	receiver := newInboundWebhookReceiverForTest()
	receiver.sessions = sessionMgr
	receiver.dispatch = queue

	payload, err := json.Marshal(inboundWebhookRequest{
		Prompt: "hello",
		Target: inboundWebhookTarget{
			Locator: &inboundWebhookLocator{
				ChannelType: locator.ChannelType,
				AddressKey:  locator.AddressKey,
				AddressJSON: locator.AddressJSON,
				SessionID:   locator.SessionID,
			},
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/inbound/webhook", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	assertInboundWebhookError(t, rec, http.StatusTooManyRequests, inboundWebhookCodeQueueFull)
}

func TestInboundWebhookReceiver_AcceptsAndDispatches(t *testing.T) {
	t.Parallel()

	locator := relaytelegram.NewLocator(9001, 99)
	ts := newSchedulerTopicSession(t, locator, "tg-101", locator.SessionID, nil)

	sessionMgr := &fakeInboundSessionManager{
		session:       ts,
		getErrOnce:    errors.New("not in memory"),
		info:          relaysession.TopicSessionInfo{SessionID: locator.SessionID, Locator: locator, UserID: "tg-101"},
		restoreResult: ts,
	}
	queue := &fakeInboundTurnQueue{runImmediately: true}
	executor := &fakeInboundTurnExecutor{}
	receiver := newInboundWebhookReceiverForTest()
	receiver.sessions = sessionMgr
	receiver.dispatch = queue
	receiver.relay = executor

	body := `{"request_id":"req-1","prompt":"hello from webhook","target":{"session_id":"` + locator.SessionID + `"}}`
	req := httptest.NewRequest(http.MethodPost, "/inbound/webhook", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()

	receiver.handleInboundWebhook(rec, req)

	if got, want := rec.Code, http.StatusAccepted; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
	var response inboundWebhookAcceptedResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, want := response.Status, inboundWebhookStatusAccepted; got != want {
		t.Fatalf("status body = %q, want %q", got, want)
	}
	if got, want := response.SessionID, locator.SessionID; got != want {
		t.Fatalf("session_id = %q, want %q", got, want)
	}
	if got := len(queue.tasks); got != 1 {
		t.Fatalf("queued tasks = %d, want 1", got)
	}
	if got := executor.calls; got != 1 {
		t.Fatalf("executor calls = %d, want 1", got)
	}
	if got, want := executor.prompt, "hello from webhook"; got != want {
		t.Fatalf("executor prompt = %q, want %q", got, want)
	}
	if got := sessionMgr.restoreCalls; got != 1 {
		t.Fatalf("restore calls = %d, want 1", got)
	}
}

type fakeInboundSessionManager struct {
	session       *relaysession.TopicSession
	getErrOnce    error
	getCalls      int
	info          relaysession.TopicSessionInfo
	infoErr       error
	restoreResult *relaysession.TopicSession
	restoreErr    error
	restoreCalls  int
}

func (f *fakeInboundSessionManager) GetSession(_ relaysession.SessionLocator) (*relaysession.TopicSession, error) {
	f.getCalls++
	if f.getErrOnce != nil {
		err := f.getErrOnce
		f.getErrOnce = nil
		return nil, err
	}
	if f.session == nil {
		return nil, errors.New("session missing")
	}
	return f.session, nil
}

func (f *fakeInboundSessionManager) GetSessionInfo(_ context.Context, _ string) (relaysession.TopicSessionInfo, error) {
	if f.infoErr != nil {
		return relaysession.TopicSessionInfo{}, f.infoErr
	}
	return f.info, nil
}

func (f *fakeInboundSessionManager) RestoreSession(
	_ context.Context,
	_ relaysession.SessionContext,
) (*relaysession.TopicSession, error) {
	f.restoreCalls++
	if f.restoreErr != nil {
		return nil, f.restoreErr
	}
	if f.restoreResult != nil {
		f.session = f.restoreResult
		return f.restoreResult, nil
	}
	return nil, errors.New("no restore result")
}

type fakeInboundTurnQueue struct {
	tasks          []TurnTask
	enqueueErr     error
	runImmediately bool
}

func (f *fakeInboundTurnQueue) Enqueue(task TurnTask) (int, error) {
	if f.enqueueErr != nil {
		return 0, f.enqueueErr
	}
	f.tasks = append(f.tasks, task)
	position := len(f.tasks) - 1
	if f.runImmediately {
		_ = task.Run(context.Background())
	}
	return position, nil
}

func (*fakeInboundTurnQueue) CancelSession(relaysession.SessionLocator, bool) (bool, int, error) {
	return false, 0, nil
}

type fakeInboundTurnExecutor struct {
	calls  int
	prompt string
}

func (f *fakeInboundTurnExecutor) runTurnTask(
	_ context.Context,
	text string,
	_ *runner.Runner,
	_ string,
	_ string,
	_ string,
	_ relaysession.SessionLocator,
	_ int,
	_ int,
	_ relaychannel.ProgressPolicy,
) error {
	f.calls++
	f.prompt = text
	return nil
}

func newInboundWebhookReceiverForTest() *InboundWebhookReceiver {
	return &InboundWebhookReceiver{
		enabled:   true,
		authToken: "test-token",
		sessions:  &fakeInboundSessionManager{},
		dispatch:  &fakeInboundTurnQueue{},
		relay:     &fakeInboundTurnExecutor{},
		logger:    zerolog.Nop(),
	}
}

func assertInboundWebhookError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()

	if got := rec.Code; got != wantStatus {
		t.Fatalf("status = %d, want %d", got, wantStatus)
	}
	var payload inboundWebhookErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got := payload.Status; got != inboundWebhookStatusError {
		t.Fatalf("status body = %q, want %q", got, inboundWebhookStatusError)
	}
	if got := payload.Error.Code; got != wantCode {
		t.Fatalf("error.code = %q, want %q", got, wantCode)
	}
}
