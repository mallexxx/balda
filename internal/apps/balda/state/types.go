package state

import (
	"context"
	"errors"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/tgbotkit/runtime/updatepoller"
	adksession "google.golang.org/adk/session"
)

// ErrMailboxFull means the target mailbox reached its active queue limit.
var ErrMailboxFull = errors.New("mailbox is full")

const (
	// NamespaceApp stores balda app internal state (for example owner auth).
	NamespaceApp = "balda.app"
	// NamespaceSessionMCP stores balda.state MCP key-value data.
	NamespaceSessionMCP = "balda.session_mcp"

	// SessionStatusActive marks a session that can be lazily restored.
	SessionStatusActive = "active"

	// ChannelTypeTelegram is the current balda channel type backed by Telegram.
	ChannelTypeTelegram = "telegram"

	// ScheduledJobStatusActive means the job is eligible for scheduler dispatch.
	ScheduledJobStatusActive = "active"
	// ScheduledJobStatusPaused means the job is persisted but not dispatched.
	ScheduledJobStatusPaused = "paused"

	// MailboxMessageStatusPending means the message is waiting for its actor.
	MailboxMessageStatusPending = "pending"
	// MailboxMessageStatusRunning means an actor claimed the message.
	MailboxMessageStatusRunning = "running"
	// MailboxMessageStatusDone means the actor completed the message.
	MailboxMessageStatusDone = "done"
	// MailboxMessageStatusFailed means the actor failed the message.
	MailboxMessageStatusFailed = "failed"
	// MailboxMessageStatusCanceled means the message was canceled before completion.
	MailboxMessageStatusCanceled = "canceled"
)

// Provider exposes balda state capabilities behind a backend-agnostic interface.
// This allows swapping SQLite with another provider later.
type Provider interface {
	AppKV() KVStore
	ADKSessions() adksession.Service
	SessionMCPKV() KVStore
	Sessions() SessionStore
	ScheduledJobs() ScheduledJobStore
	Mailboxes() MailboxMessageStore
	PollingOffsetStore() updatepoller.OffsetStore
	Collaborators() CollaboratorStore
	Close() error
}

// KVStore stores string and JSON key/value records.
type KVStore interface {
	Get(ctx context.Context, key string) (value string, ok bool, err error)
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Clear(ctx context.Context) error
	GetJSON(ctx context.Context, key string) (value any, ok bool, err error)
	SetJSON(ctx context.Context, key string, value any) error
	SetWithTTL(ctx context.Context, key string, value any, ttl time.Duration) error
	MergeJSON(ctx context.Context, key string, fields map[string]any) (merged map[string]any, err error)
}

// CollaboratorStore persists authorized collaborators.
type CollaboratorStore interface {
	AddCollaborator(ctx context.Context, c auth.Collaborator) error
	RemoveCollaborator(ctx context.Context, userID string) error
	GetCollaborator(ctx context.Context, userID string) (*auth.Collaborator, bool, error)
	ListCollaborators(ctx context.Context) ([]auth.Collaborator, error)
}

// SessionRecord persists balda session metadata for lazy restore.
type SessionRecord struct {
	SessionID    string
	UserID       string
	ChannelType  string
	AddressKey   string
	AddressJSON  string
	AgentName    string
	WorkspaceDir string
	BranchName   string
	Status       string
}

// SessionStore persists balda session metadata.
type SessionStore interface {
	Upsert(ctx context.Context, record SessionRecord) error
	GetByAddress(ctx context.Context, channelType, addressKey string) (SessionRecord, bool, error)
	GetBySessionID(ctx context.Context, sessionID string) (SessionRecord, bool, error)
	DeleteBySessionID(ctx context.Context, sessionID string) error
	List(ctx context.Context) ([]SessionRecord, error)
}

// ScheduledJobRecord persists locator-targeted recurring job metadata.
type ScheduledJobRecord struct {
	JobID           string
	SessionID       string
	ChannelType     string
	AddressKey      string
	AddressJSON     string
	Prompt          string
	ScheduleSpec    string
	Timezone        string
	Status          string
	MaxRetries      int
	RetryCount      int
	LastDispatchKey string
	NextRunAt       time.Time
	LastRunAt       time.Time
	LastError       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ScheduledJobStore persists scheduler jobs bound to canonical locators.
type ScheduledJobStore interface {
	Upsert(ctx context.Context, record ScheduledJobRecord) error
	GetByID(ctx context.Context, jobID string) (ScheduledJobRecord, bool, error)
	List(ctx context.Context) ([]ScheduledJobRecord, error)
	ListByAddress(ctx context.Context, channelType, addressKey string) ([]ScheduledJobRecord, error)
	ListDue(ctx context.Context, now time.Time, limit int) ([]ScheduledJobRecord, error)
	Delete(ctx context.Context, jobID string) error
}

// MailboxMessageRecord persists one actor mailbox message.
type MailboxMessageRecord struct {
	Sequence       int64
	MessageID      string
	MailboxID      string
	ActorType      string
	ActorKey       string
	Subject        string
	PayloadJSON    string
	Status         string
	IdempotencyKey string
	Attempts       int
	LastError      string
	AvailableAt    time.Time
	ClaimedAt      time.Time
	CompletedAt    time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// MailboxMessageStore persists actor mailbox messages.
type MailboxMessageStore interface {
	Enqueue(ctx context.Context, record MailboxMessageRecord, activeLimit int) (int, error)
	ClaimNext(ctx context.Context, mailboxID string, now time.Time) (MailboxMessageRecord, bool, error)
	Complete(ctx context.Context, messageID string) error
	Fail(ctx context.Context, messageID string, cause error) error
	CancelMailbox(ctx context.Context, mailboxID string) (int, error)
	ResetRunning(ctx context.Context) (int, error)
	ListPendingMailboxes(ctx context.Context, limit int) ([]string, error)
	GetByID(ctx context.Context, messageID string) (MailboxMessageRecord, bool, error)
}
