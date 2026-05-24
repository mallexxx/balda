package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLiteMailboxMessageStore_EnqueueClaimCompleteFIFO(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Mailboxes()

	pos, err := store.Enqueue(ctx, mailboxRecord("m1", "session:alpha"), 20)
	if err != nil {
		t.Fatalf("Enqueue(m1) error = %v", err)
	}
	if pos != 0 {
		t.Fatalf("Enqueue(m1) position = %d, want 0", pos)
	}
	pos, err = store.Enqueue(ctx, mailboxRecord("m2", "session:alpha"), 20)
	if err != nil {
		t.Fatalf("Enqueue(m2) error = %v", err)
	}
	if pos != 1 {
		t.Fatalf("Enqueue(m2) position = %d, want 1", pos)
	}

	first, ok, err := store.ClaimNext(ctx, "session:alpha", time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext(first) error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimNext(first) found = false, want true")
	}
	if first.MessageID != "m1" || first.Status != MailboxMessageStatusRunning || first.Attempts != 1 {
		t.Fatalf("first claim = %+v, want m1 running attempts=1", first)
	}
	if first.ClaimedAt.IsZero() {
		t.Fatal("first claim ClaimedAt is zero")
	}
	if err := store.Complete(ctx, first.MessageID); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	second, ok, err := store.ClaimNext(ctx, "session:alpha", time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext(second) error = %v", err)
	}
	if !ok || second.MessageID != "m2" {
		t.Fatalf("second claim = %+v, found=%v, want m2", second, ok)
	}

	got, ok, err := store.GetByID(ctx, "m1")
	if err != nil {
		t.Fatalf("GetByID(m1) error = %v", err)
	}
	if !ok || got.Status != MailboxMessageStatusDone || got.CompletedAt.IsZero() {
		t.Fatalf("completed m1 = %+v, found=%v, want done with completed_at", got, ok)
	}
}

func TestSQLiteMailboxMessageStore_QueueLimit(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Mailboxes()

	if _, err := store.Enqueue(ctx, mailboxRecord("m1", "session:limited"), 1); err != nil {
		t.Fatalf("Enqueue(m1) error = %v", err)
	}
	_, err := store.Enqueue(ctx, mailboxRecord("m2", "session:limited"), 1)
	if !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("Enqueue(m2) error = %v, want ErrMailboxFull", err)
	}
}

func TestSQLiteMailboxMessageStore_CancelAndResetRunning(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Mailboxes()

	if _, err := store.Enqueue(ctx, mailboxRecord("m1", "session:cancel"), 20); err != nil {
		t.Fatalf("Enqueue(m1) error = %v", err)
	}
	if _, err := store.Enqueue(ctx, mailboxRecord("m2", "session:cancel"), 20); err != nil {
		t.Fatalf("Enqueue(m2) error = %v", err)
	}
	if _, ok, err := store.ClaimNext(ctx, "session:cancel", time.Now().UTC()); err != nil || !ok {
		t.Fatalf("ClaimNext() = found %v, err %v, want found", ok, err)
	}

	reset, err := store.ResetRunning(ctx)
	if err != nil {
		t.Fatalf("ResetRunning() error = %v", err)
	}
	if reset != 1 {
		t.Fatalf("ResetRunning() = %d, want 1", reset)
	}

	canceled, err := store.CancelMailbox(ctx, "session:cancel")
	if err != nil {
		t.Fatalf("CancelMailbox() error = %v", err)
	}
	if canceled != 2 {
		t.Fatalf("CancelMailbox() = %d, want 2", canceled)
	}
	_, ok, err := store.ClaimNext(ctx, "session:cancel", time.Now().UTC())
	if err != nil {
		t.Fatalf("ClaimNext(after cancel) error = %v", err)
	}
	if ok {
		t.Fatal("ClaimNext(after cancel) found = true, want false")
	}
}

func TestSQLiteMailboxMessageStore_ListPendingMailboxes(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Mailboxes()

	if _, err := store.Enqueue(ctx, mailboxRecord("m1", "session:a"), 20); err != nil {
		t.Fatalf("Enqueue(m1) error = %v", err)
	}
	if _, err := store.Enqueue(ctx, mailboxRecord("m2", "session:b"), 20); err != nil {
		t.Fatalf("Enqueue(m2) error = %v", err)
	}
	if _, err := store.Enqueue(ctx, mailboxRecord("m3", "session:a"), 20); err != nil {
		t.Fatalf("Enqueue(m3) error = %v", err)
	}

	mailboxes, err := store.ListPendingMailboxes(ctx, 10)
	if err != nil {
		t.Fatalf("ListPendingMailboxes() error = %v", err)
	}
	want := []string{"session:a", "session:b"}
	if len(mailboxes) != len(want) {
		t.Fatalf("mailboxes = %v, want %v", mailboxes, want)
	}
	for i := range want {
		if mailboxes[i] != want[i] {
			t.Fatalf("mailboxes = %v, want %v", mailboxes, want)
		}
	}
}

func mailboxRecord(messageID, mailboxID string) MailboxMessageRecord {
	return MailboxMessageRecord{
		MessageID:   messageID,
		MailboxID:   mailboxID,
		ActorType:   "session",
		ActorKey:    mailboxID,
		Subject:     "balda.actor.session.test.wake",
		PayloadJSON: `{"content":"hello"}`,
	}
}
