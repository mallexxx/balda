package handlers

import (
	"context"
	"strings"
	"testing"
)

const testLocatorTopicSessionID = "tg--1002667079342-8939"

func TestResolveEnvelopeTarget_AliasOwner(t *testing.T) {
	t.Parallel()

	target, err := resolveEnvelopeTarget(
		context.Background(),
		newOwnerStoreForTest(t, 101, 9001),
		envelopeTarget{Target: " alias ", Key: " owner "},
	)
	if err != nil {
		t.Fatalf("resolveEnvelopeTarget() error = %v", err)
	}
	if got, want := target.Locator.SessionID, "tg-9001-0"; got != want {
		t.Fatalf("session_id = %q, want %q", got, want)
	}
	if got, want := target.UserID, testTelegramUserID101; got != want {
		t.Fatalf("user_id = %q, want %q", got, want)
	}
	if got := target.TopicID; got != 0 {
		t.Fatalf("topic_id = %d, want 0", got)
	}
}

func TestResolveEnvelopeTarget_Locator(t *testing.T) {
	t.Parallel()

	target, err := resolveEnvelopeTarget(
		context.Background(),
		newOwnerStoreForTest(t, 101, 9001),
		envelopeTarget{Target: " locator ", Key: " telegram:-1002667079342:8939 "},
	)
	if err != nil {
		t.Fatalf("resolveEnvelopeTarget() error = %v", err)
	}
	if got, want := target.Locator.SessionID, testLocatorTopicSessionID; got != want {
		t.Fatalf("session_id = %q, want %q", got, want)
	}
	if got, want := target.Locator.AddressKey, "-1002667079342:8939"; got != want {
		t.Fatalf("address_key = %q, want %q", got, want)
	}
	if got := target.UserID; got != "" {
		t.Fatalf("user_id = %q, want empty", got)
	}
	if got := target.TopicID; got != 8939 {
		t.Fatalf("topic_id = %d, want 8939", got)
	}
}

func TestResolveEnvelopeTarget_RejectsUnknownAlias(t *testing.T) {
	t.Parallel()

	_, err := resolveEnvelopeTarget(
		context.Background(),
		newOwnerStoreForTest(t, 101, 9001),
		envelopeTarget{Target: "alias", Key: "vasya"},
	)
	if err == nil {
		t.Fatal("resolveEnvelopeTarget() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), `unsupported alias target "vasya"`) {
		t.Fatalf("resolveEnvelopeTarget() error = %v", err)
	}
}

func TestResolveEnvelopeTarget_RejectsInvalidLocator(t *testing.T) {
	t.Parallel()

	_, err := resolveEnvelopeTarget(
		context.Background(),
		newOwnerStoreForTest(t, 101, 9001),
		envelopeTarget{Target: "locator", Key: "telegram"},
	)
	if err == nil {
		t.Fatal("resolveEnvelopeTarget() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "<channel_type>:<address_key>") {
		t.Fatalf("resolveEnvelopeTarget() error = %v", err)
	}
}
