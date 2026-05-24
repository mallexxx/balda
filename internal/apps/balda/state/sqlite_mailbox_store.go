package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type sqliteMailboxMessageStore struct {
	db *sql.DB
}

func (s *sqliteMailboxMessageStore) Enqueue(
	ctx context.Context,
	record MailboxMessageRecord,
	activeLimit int,
) (int, error) {
	messageID := strings.TrimSpace(record.MessageID)
	if messageID == "" {
		return 0, fmt.Errorf("message_id is required")
	}
	mailboxID := strings.TrimSpace(record.MailboxID)
	if mailboxID == "" {
		return 0, fmt.Errorf("mailbox_id is required")
	}
	actorType := strings.TrimSpace(record.ActorType)
	if actorType == "" {
		return 0, fmt.Errorf("actor_type is required")
	}
	actorKey := strings.TrimSpace(record.ActorKey)
	if actorKey == "" {
		return 0, fmt.Errorf("actor_key is required")
	}
	payload := strings.TrimSpace(record.PayloadJSON)
	if payload == "" {
		return 0, fmt.Errorf("payload_json is required")
	}
	subject := strings.TrimSpace(record.Subject)
	status := strings.TrimSpace(record.Status)
	if status == "" {
		status = MailboxMessageStatusPending
	}
	if status != MailboxMessageStatusPending {
		return 0, fmt.Errorf("mailbox enqueue status must be %q", MailboxMessageStatusPending)
	}

	now := time.Now().UTC()
	availableAt := record.AvailableAt.UTC()
	if availableAt.IsZero() {
		availableAt = now
	}
	createdAt := record.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = now
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin mailbox enqueue: %w", err)
	}
	defer rollbackMailboxTx(tx)

	var activeBefore int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM balda_mailbox_messages
		WHERE mailbox_id = ? AND status IN (?, ?)`,
		mailboxID,
		MailboxMessageStatusPending,
		MailboxMessageStatusRunning,
	).Scan(&activeBefore); err != nil {
		return 0, fmt.Errorf("count active mailbox messages: %w", err)
	}
	if activeLimit > 0 && activeBefore >= activeLimit {
		return 0, ErrMailboxFull
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO balda_mailbox_messages (
			message_id, mailbox_id, actor_type, actor_key, subject, payload_json, status, idempotency_key,
			attempts, last_error, available_at, claimed_at, completed_at, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		messageID,
		mailboxID,
		actorType,
		actorKey,
		subject,
		payload,
		status,
		strings.TrimSpace(record.IdempotencyKey),
		nonNegative(record.Attempts),
		strings.TrimSpace(record.LastError),
		availableAt.Format(time.RFC3339),
		formatOptionalRFC3339(record.ClaimedAt),
		formatOptionalRFC3339(record.CompletedAt),
		createdAt.Format(time.RFC3339),
		now.Format(time.RFC3339),
	); err != nil {
		return 0, fmt.Errorf("insert mailbox message %q: %w", messageID, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit mailbox enqueue: %w", err)
	}

	if activeBefore > 0 {
		return activeBefore, nil
	}
	return 0, nil
}

func (s *sqliteMailboxMessageStore) ClaimNext(
	ctx context.Context,
	mailboxID string,
	now time.Time,
) (MailboxMessageRecord, bool, error) {
	trimmedMailboxID := strings.TrimSpace(mailboxID)
	if trimmedMailboxID == "" {
		return MailboxMessageRecord{}, false, fmt.Errorf("mailbox_id is required")
	}
	claimTime := now.UTC()
	if claimTime.IsZero() {
		claimTime = time.Now().UTC()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MailboxMessageRecord{}, false, fmt.Errorf("begin mailbox claim: %w", err)
	}
	defer rollbackMailboxTx(tx)

	var messageID string
	err = tx.QueryRowContext(ctx, `
		SELECT message_id
		FROM balda_mailbox_messages
		WHERE mailbox_id = ? AND status = ? AND available_at <= ?
		ORDER BY available_at ASC, sequence ASC
		LIMIT 1`,
		trimmedMailboxID,
		MailboxMessageStatusPending,
		claimTime.Format(time.RFC3339),
	).Scan(&messageID)
	if err != nil {
		if err == sql.ErrNoRows {
			if commitErr := tx.Commit(); commitErr != nil {
				return MailboxMessageRecord{}, false, fmt.Errorf("commit empty mailbox claim: %w", commitErr)
			}
			return MailboxMessageRecord{}, false, nil
		}
		return MailboxMessageRecord{}, false, fmt.Errorf("select next mailbox message: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE balda_mailbox_messages
		SET status = ?, attempts = attempts + 1, claimed_at = ?, updated_at = ?
		WHERE message_id = ? AND status = ?`,
		MailboxMessageStatusRunning,
		claimTime.Format(time.RFC3339),
		claimTime.Format(time.RFC3339),
		messageID,
		MailboxMessageStatusPending,
	); err != nil {
		return MailboxMessageRecord{}, false, fmt.Errorf("claim mailbox message %q: %w", messageID, err)
	}

	record, ok, err := scanMailboxMessage(tx.QueryRowContext(ctx, mailboxMessageSelectSQL+` WHERE message_id = ?`, messageID).Scan)
	if err != nil {
		return MailboxMessageRecord{}, false, err
	}
	if !ok {
		return MailboxMessageRecord{}, false, fmt.Errorf("claimed mailbox message %q not found", messageID)
	}

	if err := tx.Commit(); err != nil {
		return MailboxMessageRecord{}, false, fmt.Errorf("commit mailbox claim: %w", err)
	}
	return record, true, nil
}

func (s *sqliteMailboxMessageStore) Complete(ctx context.Context, messageID string) error {
	return s.finish(ctx, messageID, MailboxMessageStatusDone, nil)
}

func (s *sqliteMailboxMessageStore) Fail(ctx context.Context, messageID string, cause error) error {
	return s.finish(ctx, messageID, MailboxMessageStatusFailed, cause)
}

func (s *sqliteMailboxMessageStore) finish(ctx context.Context, messageID string, status string, cause error) error {
	trimmed := strings.TrimSpace(messageID)
	if trimmed == "" {
		return fmt.Errorf("message_id is required")
	}
	now := time.Now().UTC()
	lastError := ""
	if cause != nil {
		lastError = strings.TrimSpace(cause.Error())
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE balda_mailbox_messages
		SET status = ?, completed_at = ?, last_error = ?, updated_at = ?
		WHERE message_id = ? AND status <> ?`,
		status,
		now.Format(time.RFC3339),
		lastError,
		now.Format(time.RFC3339),
		trimmed,
		MailboxMessageStatusCanceled,
	); err != nil {
		return fmt.Errorf("finish mailbox message %q: %w", trimmed, err)
	}
	return nil
}

func (s *sqliteMailboxMessageStore) CancelMailbox(ctx context.Context, mailboxID string) (int, error) {
	trimmed := strings.TrimSpace(mailboxID)
	if trimmed == "" {
		return 0, fmt.Errorf("mailbox_id is required")
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE balda_mailbox_messages
		SET status = ?, completed_at = ?, updated_at = ?
		WHERE mailbox_id = ? AND status IN (?, ?)`,
		MailboxMessageStatusCanceled,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		trimmed,
		MailboxMessageStatusPending,
		MailboxMessageStatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("cancel mailbox %q: %w", trimmed, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count canceled mailbox messages: %w", err)
	}
	return int(count), nil
}

func (s *sqliteMailboxMessageStore) ResetRunning(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE balda_mailbox_messages
		SET status = ?, claimed_at = '', updated_at = ?
		WHERE status = ?`,
		MailboxMessageStatusPending,
		now.Format(time.RFC3339),
		MailboxMessageStatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("reset running mailbox messages: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count reset mailbox messages: %w", err)
	}
	return int(count), nil
}

func (s *sqliteMailboxMessageStore) ListPendingMailboxes(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT mailbox_id
		FROM balda_mailbox_messages
		WHERE status = ? AND available_at <= ?
		GROUP BY mailbox_id
		ORDER BY MIN(sequence) ASC
		LIMIT ?`,
		MailboxMessageStatusPending,
		time.Now().UTC().Format(time.RFC3339),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending mailboxes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]string, 0)
	for rows.Next() {
		var mailboxID string
		if err := rows.Scan(&mailboxID); err != nil {
			return nil, fmt.Errorf("scan pending mailbox: %w", err)
		}
		out = append(out, mailboxID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending mailboxes: %w", err)
	}
	return out, nil
}

func (s *sqliteMailboxMessageStore) GetByID(ctx context.Context, messageID string) (MailboxMessageRecord, bool, error) {
	record, ok, err := scanMailboxMessage(s.db.QueryRowContext(ctx, mailboxMessageSelectSQL+` WHERE message_id = ?`, strings.TrimSpace(messageID)).Scan)
	if err != nil {
		return MailboxMessageRecord{}, false, err
	}
	return record, ok, nil
}

const mailboxMessageSelectSQL = `
	SELECT sequence, message_id, mailbox_id, actor_type, actor_key, subject, payload_json, status, idempotency_key,
	       attempts, last_error, available_at, claimed_at, completed_at, created_at, updated_at
	FROM balda_mailbox_messages`

func scanMailboxMessage(scan func(dest ...any) error) (MailboxMessageRecord, bool, error) {
	var (
		record         MailboxMessageRecord
		availableAtRaw string
		claimedAtRaw   string
		completedAtRaw string
		createdAtRaw   string
		updatedAtRaw   string
	)
	err := scan(
		&record.Sequence,
		&record.MessageID,
		&record.MailboxID,
		&record.ActorType,
		&record.ActorKey,
		&record.Subject,
		&record.PayloadJSON,
		&record.Status,
		&record.IdempotencyKey,
		&record.Attempts,
		&record.LastError,
		&availableAtRaw,
		&claimedAtRaw,
		&completedAtRaw,
		&createdAtRaw,
		&updatedAtRaw,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return MailboxMessageRecord{}, false, nil
		}
		return MailboxMessageRecord{}, false, fmt.Errorf("scan mailbox message: %w", err)
	}

	availableAt, err := parseRequiredRFC3339(availableAtRaw)
	if err != nil {
		return MailboxMessageRecord{}, false, fmt.Errorf("parse available_at: %w", err)
	}
	claimedAt, err := parseOptionalRFC3339(claimedAtRaw)
	if err != nil {
		return MailboxMessageRecord{}, false, fmt.Errorf("parse claimed_at: %w", err)
	}
	completedAt, err := parseOptionalRFC3339(completedAtRaw)
	if err != nil {
		return MailboxMessageRecord{}, false, fmt.Errorf("parse completed_at: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return MailboxMessageRecord{}, false, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return MailboxMessageRecord{}, false, fmt.Errorf("parse updated_at: %w", err)
	}

	record.AvailableAt = availableAt
	record.ClaimedAt = claimedAt
	record.CompletedAt = completedAt
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	return record, true, nil
}

func rollbackMailboxTx(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

func nonNegative(v int) int {
	if v < 0 {
		return 0
	}
	return v
}
