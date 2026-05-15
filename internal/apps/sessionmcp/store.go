package sessionmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Store is the interface for session state storage drivers.
// It wraps ADK's session.State with additional methods for MCP tools.
type Store interface {
	// Get retrieves a value by key. Returns empty string and false if not found.
	Get(ctx context.Context, key string) (value string, ok bool, err error)
	// Set stores a value by key.
	Set(ctx context.Context, key, value string) error
	// Delete removes a key. No-op if key doesn't exist.
	Delete(ctx context.Context, key string) error
	// List returns all keys, optionally filtered by prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// Clear removes all keys.
	Clear(ctx context.Context) error
	// GetJSON retrieves a value by key as parsed JSON.
	GetJSON(ctx context.Context, key string) (value interface{}, ok bool, err error)
	// SetJSON stores a value by key as JSON.
	SetJSON(ctx context.Context, key string, value interface{}) error
	// MergeJSON merges fields into an existing JSON object at key.
	MergeJSON(ctx context.Context, key string, fields map[string]interface{}) (merged map[string]interface{}, err error)
}

// Global shared state for in-process MemoryStore instances.
// All MemoryStore instances share this map so state is visible across agents
// connecting to the same embedded server.
var (
	sharedMemoryMu sync.Mutex
	sharedMemory   *memoryState
)

type memoryState struct {
	mu     sync.RWMutex
	values map[string]any
}

func newMemoryState() *memoryState {
	return &memoryState{values: make(map[string]any)}
}

func getSharedMemoryState() *memoryState {
	sharedMemoryMu.Lock()
	defer sharedMemoryMu.Unlock()
	if sharedMemory == nil {
		sharedMemory = newMemoryState()
	}
	return sharedMemory
}

// ResetSharedStore resets the global shared store for testing purposes.
// This allows tests to start with a clean state.
func ResetSharedStore() {
	sharedMemoryMu.Lock()
	defer sharedMemoryMu.Unlock()
	sharedMemory = newMemoryState()
}

// MemoryStore is an in-memory session state store.
// This is the default driver for embedded MCP state sharing.
//
// All MemoryStore instances share a single in-process map, so state set by one
// agent is visible to other agents connected to the same server.
type MemoryStore struct {
	state *memoryState
}

// NewMemoryStore creates a new in-memory store using the shared state map.
// Multiple calls return stores backed by the same underlying state.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		state: getSharedMemoryState(),
	}
}

func (s *MemoryStore) Get(_ context.Context, key string) (string, bool, error) {
	val, ok := s.get(key)
	if !ok {
		return "", false, nil
	}
	str, ok := val.(string)
	if !ok {
		b, err := json.Marshal(val)
		if err != nil {
			return "", false, fmt.Errorf("marshal value: %w", err)
		}
		return string(b), true, nil
	}
	return str, true, nil
}

func (s *MemoryStore) Set(_ context.Context, key, value string) error {
	s.set(key, value)
	return nil
}

func (s *MemoryStore) Delete(_ context.Context, key string) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	delete(s.state.values, strings.TrimSpace(key))
	return nil
}

func (s *MemoryStore) List(_ context.Context, prefix string) ([]string, error) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()

	trimmedPrefix := strings.TrimSpace(prefix)
	keys := make([]string, 0)
	for key := range s.state.values {
		if trimmedPrefix == "" || strings.HasPrefix(key, trimmedPrefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *MemoryStore) Clear(_ context.Context) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.values = make(map[string]any)
	return nil
}

// GetJSON retrieves a value by key and unmarshals it as JSON.
func (s *MemoryStore) GetJSON(_ context.Context, key string) (interface{}, bool, error) {
	val, ok := s.get(key)
	if !ok {
		return nil, false, nil
	}
	return val, true, nil
}

// SetJSON stores a value by key as JSON.
func (s *MemoryStore) SetJSON(_ context.Context, key string, value interface{}) error {
	s.set(key, value)
	return nil
}

// MergeJSON merges fields into an existing JSON object at key.
// If the key doesn't exist, it creates a new object with the provided fields.
func (s *MemoryStore) MergeJSON(_ context.Context, key string, fields map[string]interface{}) (map[string]interface{}, error) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, fmt.Errorf("key is required")
	}

	var existing map[string]interface{}

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	if val, ok := s.state.values[trimmedKey]; ok {
		switch v := val.(type) {
		case map[string]interface{}:
			existing = copyStringAnyMap(v)
		default:
			existing = make(map[string]interface{})
		}
	} else {
		existing = make(map[string]interface{})
	}

	for k, v := range fields {
		existing[k] = v
	}

	s.state.values[trimmedKey] = copyStringAnyMap(existing)

	return existing, nil
}

func (s *MemoryStore) get(key string) (any, bool) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, false
	}

	s.state.mu.RLock()
	defer s.state.mu.RUnlock()

	val, ok := s.state.values[trimmedKey]
	return val, ok
}

func (s *MemoryStore) set(key string, value any) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return
	}

	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.values[trimmedKey] = value
}

func copyStringAnyMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
