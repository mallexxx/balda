package swarm

import (
	"context"
	"fmt"
	"testing"
)

type testActor struct {
	address string
	err     error
	calls   int
}

func (a *testActor) Address() string { return a.address }
func (a *testActor) Handle(context.Context, Envelope) error {
	a.calls++
	return a.err
}

type testCommandMessage struct{ env Envelope }

func (m testCommandMessage) Envelope() Envelope { return m.env }
func (m testCommandMessage) Subject() string    { return SubjectForEnvelope(m.env) }
func (m testCommandMessage) InProgress(context.Context) error { return nil }

type recordingCommandBus struct{ events []string }

func (b *recordingCommandBus) PublishCommand(context.Context, Envelope) (*CommandPublishResult, error) { return nil, nil }
func (b *recordingCommandBus) PublishEvent(_ context.Context, subject string, _ Envelope) error {
	b.events = append(b.events, subject)
	return nil
}
func (b *recordingCommandBus) PublishDLQ(context.Context, Envelope, string) error { return nil }
func (b *recordingCommandBus) RunCommandConsumer(ctx context.Context, _ CommandHandler) error {
	<-ctx.Done()
	return ctx.Err()
}
func (b *recordingCommandBus) Drain(context.Context) error { return nil }

func TestRuntime_HandleCommandDispatchesActor(t *testing.T) {
	bus := &recordingCommandBus{}
	actor := &testActor{address: WildcardAddress(ActorTypeSession)}
	registry := NewRegistry()
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	runtime := newRuntimeForTest(bus, registry)
	if err := runtime.HandleCommand(context.Background(), testCommandMessage{env: runtimeTestEnvelope("ok", ActorAddress{Target: ActorTypeSession, Key: "s-1"})}); err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
	}
	if actor.calls != 1 {
		t.Fatalf("actor calls = %d, want 1", actor.calls)
	}
}

func TestRuntime_UnknownActorReturnsPermanent(t *testing.T) {
	runtime := newRuntimeForTest(&recordingCommandBus{}, NewRegistry())
	err := runtime.HandleCommand(context.Background(), testCommandMessage{env: runtimeTestEnvelope("unknown", ActorAddress{Target: ActorTypeSession, Key: "s-1"})})
	if ClassifyError(err) != ErrorKindPermanent {
		t.Fatalf("ClassifyError(%v) = %s, want permanent", err, ClassifyError(err))
	}
}

func TestRuntime_ActorErrorPropagatesForJetStreamSettlement(t *testing.T) {
	actor := &testActor{address: WildcardAddress(ActorTypeSession), err: TransientError(fmt.Errorf("temporary"))}
	registry := NewRegistry()
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	runtime := newRuntimeForTest(&recordingCommandBus{}, registry)
	err := runtime.HandleCommand(context.Background(), testCommandMessage{env: runtimeTestEnvelope("retry", ActorAddress{Target: ActorTypeSession, Key: "s-1"})})
	if ClassifyError(err) != ErrorKindTransient {
		t.Fatalf("ClassifyError(%v) = %s, want transient", err, ClassifyError(err))
	}
}

func newRuntimeForTest(bus CommandBus, registry ActorRegistry) *Runtime {
	return &Runtime{bus: bus, registry: registry, scheduler: NewKeyedActorScheduler()}
}

func runtimeTestEnvelope(id string, to ActorAddress) Envelope {
	return Envelope{ID: id, Namespace: NamespaceHumanInbound, Kind: KindMessage, From: ActorAddress{Target: "test", Key: "source"}, To: to, SessionID: to.Key, PayloadJSON: `{"ok":true}`}
}
