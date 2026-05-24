// Package swarm contains Balda's actor mailbox runtime primitives.
package swarm

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	ActorTypeSession  = "session"
	ActorTypeTask     = "task"
	ActorTypeAgent    = "agent"
	ActorTypeDelivery = "delivery"
	ActorTypeMemory   = "memory"
)

type ActorAddress struct {
	Target string `json:"target"`
	Key    string `json:"key"`
}

type Envelope struct {
	ID       string            `json:"id"`
	Target   ActorAddress      `json:"target"`
	Content  string            `json:"content"`
	ReportTo *ActorAddress     `json:"report_to,omitempty"`
	Meta     map[string]string `json:"meta,omitempty"`
}

func (a ActorAddress) MailboxID() (string, error) {
	target := strings.ToLower(strings.TrimSpace(a.Target))
	key := strings.TrimSpace(a.Key)
	if target == "" {
		return "", fmt.Errorf("actor target is required")
	}
	if key == "" {
		return "", fmt.Errorf("actor key is required")
	}
	return target + ":" + key, nil
}

func (e Envelope) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return fmt.Errorf("envelope id is required")
	}
	if _, err := e.Target.MailboxID(); err != nil {
		return err
	}
	return nil
}

func EncodeEnvelope(e Envelope) (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("encode envelope: %w", err)
	}
	return string(data), nil
}

func DecodeEnvelope(raw string) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &env); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}
