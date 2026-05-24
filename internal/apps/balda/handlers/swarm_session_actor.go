package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
)

type sessionTurnPayload struct {
	Text           string                      `json:"text"`
	Locator        baldasession.SessionLocator `json:"locator"`
	UserID         string                      `json:"user_id,omitempty"`
	AgentSessionID string                      `json:"agent_session_id,omitempty"`
	MessageID      int                         `json:"message_id,omitempty"`
	TopicID        int                         `json:"topic_id,omitempty"`
	ProgressPolicy baldachannel.ProgressPolicy `json:"progress_policy,omitempty"`
	Deliver        bool                        `json:"deliver"`
}

func (h *BaldaHandler) submitSessionTurn(ctx context.Context, payload sessionTurnPayload) (int, error) {
	if h.swarmCoordinator != nil {
		return h.submitSessionTurnToSwarm(ctx, payload)
	}
	return h.enqueueSessionTurnDirect(payload)
}

func (h *BaldaHandler) submitSessionTurnToSwarm(ctx context.Context, payload sessionTurnPayload) (int, error) {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return 0, fmt.Errorf("session id is required")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("encode session turn payload: %w", err)
	}
	submitted, err := h.swarmCoordinator.Submit(ctx, swarm.Envelope{
		Target:  swarm.ActorAddress{Target: swarm.ActorTypeSession, Key: payload.Locator.SessionID},
		Content: string(data),
	})
	if err != nil {
		if swarm.IsMailboxFull(err) {
			return 0, ErrTurnQueueFull
		}
		return 0, err
	}
	return submitted.QueuePosition, nil
}

func (h *BaldaHandler) enqueueSessionTurnDirect(payload sessionTurnPayload) (int, error) {
	if h.turnDispatcher == nil {
		return 0, fmt.Errorf("balda turn dispatcher is required")
	}
	return h.turnDispatcher.Enqueue(TurnTask{
		SessionID: payload.Locator.SessionID,
		Run: func(runCtx context.Context) error {
			return h.runSessionTurnPayload(runCtx, payload)
		},
	})
}

func (h *BaldaHandler) runSessionTurnPayload(ctx context.Context, payload sessionTurnPayload) error {
	ts, err := h.sessionManager.GetSession(payload.Locator)
	if err != nil {
		h.logger.Debug().
			Str("session_id", payload.Locator.SessionID).
			Str("address_key", payload.Locator.AddressKey).
			Msg("dropping queued turn for inactive session")
		return nil
	}
	userID := strings.TrimSpace(payload.UserID)
	if userID == "" {
		userID = ts.GetUserID()
	}
	agentSessionID := strings.TrimSpace(payload.AgentSessionID)
	if agentSessionID == "" {
		agentSessionID = ts.GetAgentSessionID()
	}
	return h.runTurnTaskWithDelivery(
		ctx,
		payload.Text,
		ts.GetRunner(),
		userID,
		ts.GetSessionID(),
		agentSessionID,
		payload.Locator,
		payload.MessageID,
		payload.TopicID,
		payload.ProgressPolicy,
		payload.Deliver,
	)
}

type sessionActorExecutor struct {
	handler *BaldaHandler
}

type sessionActorExecutorParams struct {
	fx.In

	Handler *BaldaHandler
}

func newSessionActorExecutor(params sessionActorExecutorParams) swarm.Executor {
	return &sessionActorExecutor{handler: params.Handler}
}

func (e *sessionActorExecutor) ActorType() string {
	return swarm.ActorTypeSession
}

func (e *sessionActorExecutor) Execute(ctx context.Context, env swarm.Envelope) error {
	var payload sessionTurnPayload
	if err := json.Unmarshal([]byte(env.Content), &payload); err != nil {
		return fmt.Errorf("decode session turn payload: %w", err)
	}
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		payload.Locator.SessionID = strings.TrimSpace(env.Target.Key)
	}
	if e.handler.turnDispatcher == nil {
		return e.handler.runSessionTurnPayload(ctx, payload)
	}

	done := make(chan error, 1)
	_, err := e.handler.turnDispatcher.Enqueue(TurnTask{
		SessionID: payload.Locator.SessionID,
		Run: func(runCtx context.Context) error {
			err := e.handler.runSessionTurnPayload(runCtx, payload)
			done <- err
			return err
		},
	})
	if err != nil {
		if errors.Is(err, ErrTurnQueueFull) {
			return err
		}
		return fmt.Errorf("enqueue session actor turn: %w", err)
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
