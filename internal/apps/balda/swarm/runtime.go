package swarm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type Executor interface {
	ActorType() string
	Execute(ctx context.Context, env Envelope) error
}

type Runtime struct {
	store     baldastate.MailboxMessageStore
	bus       WakeBus
	logger    zerolog.Logger
	executors map[string]Executor

	mu       sync.Mutex
	draining map[string]struct{}
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

type runtimeParams struct {
	fx.In

	LC            fx.Lifecycle
	StateProvider baldastate.Provider
	Bus           WakeBus
	Logger        zerolog.Logger
	Executors     []Executor `group:"balda_swarm_executors"`
}

func NewRuntime(params runtimeParams) (*Runtime, error) {
	if params.StateProvider == nil {
		return nil, fmt.Errorf("balda state provider is required")
	}
	if params.Bus == nil {
		return nil, fmt.Errorf("swarm wake bus is required")
	}
	executors := make(map[string]Executor, len(params.Executors))
	for _, executor := range params.Executors {
		if executor == nil {
			continue
		}
		actorType := strings.ToLower(strings.TrimSpace(executor.ActorType()))
		if actorType == "" {
			return nil, fmt.Errorf("swarm executor actor type is required")
		}
		executors[actorType] = executor
	}
	r := &Runtime{
		store:     params.StateProvider.Mailboxes(),
		bus:       params.Bus,
		logger:    params.Logger.With().Str("component", "balda.swarm.runtime").Logger(),
		executors: executors,
		draining:  make(map[string]struct{}),
	}
	params.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return r.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return r.Stop(ctx) },
	})
	return r, nil
}

func (r *Runtime) Start(ctx context.Context) error {
	if r.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	if _, err := r.store.ResetRunning(ctx); err != nil {
		cancel()
		return err
	}
	if err := r.bus.Subscribe(runCtx, func(ctx context.Context, mailboxID string) error {
		r.wake(ctx, mailboxID)
		return nil
	}); err != nil {
		cancel()
		return err
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.scanLoop(runCtx)
	}()
	return r.wakePending(ctx)
}

func (r *Runtime) Stop(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) wake(ctx context.Context, mailboxID string) {
	trimmed := strings.TrimSpace(mailboxID)
	if trimmed == "" {
		return
	}
	r.mu.Lock()
	if _, exists := r.draining[trimmed]; exists {
		r.mu.Unlock()
		return
	}
	r.draining[trimmed] = struct{}{}
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		defer func() {
			r.mu.Lock()
			delete(r.draining, trimmed)
			r.mu.Unlock()
		}()
		r.drain(ctx, trimmed)
	}()
}

func (r *Runtime) drain(ctx context.Context, mailboxID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		record, ok, err := r.store.ClaimNext(ctx, mailboxID, time.Now().UTC())
		if err != nil {
			r.logger.Warn().Err(err).Str("mailbox_id", mailboxID).Msg("failed to claim mailbox message")
			return
		}
		if !ok {
			return
		}
		if err := r.execute(ctx, record); err != nil {
			r.logger.Warn().Err(err).Str("mailbox_id", mailboxID).Str("message_id", record.MessageID).Msg("mailbox message failed")
			_ = r.store.Fail(context.Background(), record.MessageID, err)
			continue
		}
		if err := r.store.Complete(context.Background(), record.MessageID); err != nil {
			r.logger.Warn().Err(err).Str("mailbox_id", mailboxID).Str("message_id", record.MessageID).Msg("failed to complete mailbox message")
		}
	}
}

func (r *Runtime) execute(ctx context.Context, record baldastate.MailboxMessageRecord) error {
	env, err := DecodeEnvelope(record.PayloadJSON)
	if err != nil {
		return err
	}
	executor := r.executors[strings.ToLower(strings.TrimSpace(record.ActorType))]
	if executor == nil {
		return fmt.Errorf("no swarm executor registered for actor type %q", record.ActorType)
	}
	return executor.Execute(ctx, env)
}

func (r *Runtime) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.wakePending(ctx); err != nil {
				r.logger.Warn().Err(err).Msg("failed to wake pending mailboxes")
			}
		}
	}
}

func (r *Runtime) wakePending(ctx context.Context) error {
	mailboxes, err := r.store.ListPendingMailboxes(ctx, 100)
	if err != nil {
		return err
	}
	for _, mailboxID := range mailboxes {
		r.wake(ctx, mailboxID)
	}
	return nil
}
