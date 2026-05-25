package swarm

import (
	"context"
	"fmt"
)

type Coordinator struct {
	bus CommandBus
	cfg Config
}

func NewCoordinator(bus CommandBus, cfg Config) *Coordinator {
	return &Coordinator{bus: bus, cfg: cfg}
}

func (c *Coordinator) Enabled() bool {
	return c != nil && c.cfg.Enabled && c.bus != nil
}

func (c *Coordinator) RuntimeEnabled() bool {
	return c.Enabled()
}

func (c *Coordinator) Submit(ctx context.Context, env Envelope) (*CommandPublishResult, error) {
	if c == nil || c.bus == nil {
		return nil, fmt.Errorf("jetstream command bus is required")
	}
	return c.bus.PublishCommand(ctx, env)
}

func (c *Coordinator) PublishEvent(ctx context.Context, subject string, env Envelope) error {
	if c == nil || c.bus == nil {
		return fmt.Errorf("jetstream event bus is required")
	}
	return c.bus.PublishEvent(ctx, subject, env)
}
