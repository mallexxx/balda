package swarm

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

type recordingTaskCommandBus struct {
	mu       sync.Mutex
	subjects []string
	envs     []Envelope
}

func (*recordingTaskCommandBus) PublishCommand(context.Context, Envelope) (*CommandPublishResult, error) { return nil, nil }
func (b *recordingTaskCommandBus) PublishEvent(_ context.Context, subject string, env Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subjects = append(b.subjects, subject)
	b.envs = append(b.envs, env)
	return nil
}
func (*recordingTaskCommandBus) PublishDLQ(context.Context, Envelope, string) error { return nil }
func (*recordingTaskCommandBus) RunCommandConsumer(ctx context.Context, _ CommandHandler) error {
	<-ctx.Done()
	return ctx.Err()
}
func (*recordingTaskCommandBus) Drain(context.Context) error { return nil }

func TestTaskServiceAppendEventPublishesJetStreamEvent(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingTaskCommandBus{}
	service, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: bus})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentProgress, "agent:executor", "msg-1", map[string]any{"text": "working"}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if len(bus.subjects) != 1 || bus.subjects[0] != SubjectEventTaskUpdated {
		t.Fatalf("subjects = %+v, want %q", bus.subjects, SubjectEventTaskUpdated)
	}
	if len(bus.envs) != 1 || bus.envs[0].TaskID != "task-1" || bus.envs[0].Meta["event_type"] != TaskEventAgentProgress {
		t.Fatalf("envs = %+v, want task event envelope", bus.envs)
	}
}
