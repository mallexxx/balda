package swarm

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	queueMetaMode = "queue_mode"
)

var ErrQueueFull = errors.New("swarm actor queue full")

type QueueFullError struct {
	Key string
	Cap int
}

func (e *QueueFullError) Error() string {
	return fmt.Sprintf("%s: actor key %q reached cap %d", ErrQueueFull, e.Key, e.Cap)
}

func (e *QueueFullError) Unwrap() error {
	return ErrQueueFull
}

type CollectBuffer struct {
	mu       sync.Mutex
	debounce time.Duration
	entries  map[string]*collectEntry
	flush    func([]Envelope) error
}

type collectEntry struct {
	envs  []Envelope
	timer *time.Timer
}

func NewCollectBuffer(debounce time.Duration, flush func([]Envelope) error) *CollectBuffer {
	if debounce <= 0 {
		debounce = time.Duration(defaultQueueDebounceMS) * time.Millisecond
	}
	return &CollectBuffer{
		debounce: debounce,
		entries:  make(map[string]*collectEntry),
		flush:    flush,
	}
}

func (b *CollectBuffer) Add(env Envelope) {
	if b == nil || b.flush == nil {
		return
	}
	key := collectKey(env)
	b.mu.Lock()
	entry := b.entries[key]
	if entry == nil {
		entry = &collectEntry{}
		b.entries[key] = entry
	}
	entry.envs = append(entry.envs, env)
	if entry.timer != nil {
		entry.timer.Stop()
	}
	entry.timer = time.AfterFunc(b.debounce, func() {
		_ = b.flushKey(key)
	})
	b.mu.Unlock()
}

func (b *CollectBuffer) FlushAll() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	keys := make([]string, 0, len(b.entries))
	for key := range b.entries {
		keys = append(keys, key)
	}
	b.mu.Unlock()

	for _, key := range keys {
		if err := b.flushKey(key); err != nil {
			return err
		}
	}
	return nil
}

func (b *CollectBuffer) flushKey(key string) error {
	b.mu.Lock()
	entry := b.entries[key]
	if entry == nil {
		b.mu.Unlock()
		return nil
	}
	delete(b.entries, key)
	if entry.timer != nil {
		entry.timer.Stop()
	}
	envs := append([]Envelope(nil), entry.envs...)
	b.mu.Unlock()

	return b.flush(envs)
}

func collectKey(env Envelope) string {
	to, _ := env.To.String()
	return strings.Join([]string{
		strings.TrimSpace(to),
		strings.TrimSpace(env.Namespace),
		strings.TrimSpace(env.Kind),
		strings.TrimSpace(env.SessionID),
		strings.TrimSpace(env.TaskID),
	}, "\x00")
}

func queueMode(env Envelope) string {
	if env.Meta == nil {
		return ""
	}
	return strings.TrimSpace(env.Meta[queueMetaMode])
}

func QueueModeOf(env Envelope) string {
	return queueMode(env)
}
