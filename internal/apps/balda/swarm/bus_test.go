package swarm

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/fx/fxtest"
)

func TestEmbeddedBus_PublishSubscribeInProcess(t *testing.T) {
	lc := fxtest.NewLifecycle(t)
	bus, err := NewEmbeddedBus(embeddedBusParams{LC: lc, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("NewEmbeddedBus() error = %v", err)
	}
	defer func() { _ = bus.Close() }()

	ctx := context.Background()
	got := make(chan string, 1)
	if err := bus.Subscribe(ctx, func(_ context.Context, mailboxID string) error {
		got <- mailboxID
		return nil
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	addr := ActorAddress{Target: ActorTypeSession, Key: "tg-1.2"}
	if err := bus.Publish(ctx, addr); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case mailboxID := <-got:
		if mailboxID != "session:tg-1.2" {
			t.Fatalf("mailboxID = %q, want session:tg-1.2", mailboxID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for wake message")
	}
}

func TestWakeSubject_EncodesActorKeyAsSingleToken(t *testing.T) {
	subject, err := wakeSubject(ActorAddress{Target: ActorTypeSession, Key: "tg-1.2"})
	if err != nil {
		t.Fatalf("wakeSubject() error = %v", err)
	}
	if subject == "balda.actor.session.tg-1.2.wake" {
		t.Fatal("wakeSubject used raw actor key with dot")
	}
	if want := "balda.actor.session."; len(subject) <= len(want) || subject[:len(want)] != want {
		t.Fatalf("subject = %q, want prefix %q", subject, want)
	}
}
