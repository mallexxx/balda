package swarm

import "context"

type Coordinator struct {
	mailboxes *MailboxService
}

func NewCoordinator(mailboxes *MailboxService) *Coordinator {
	return &Coordinator{mailboxes: mailboxes}
}

func (c *Coordinator) Submit(ctx context.Context, env Envelope) (SubmittedMessage, error) {
	return c.mailboxes.Submit(ctx, env)
}

func (c *Coordinator) Cancel(ctx context.Context, addr ActorAddress) (int, error) {
	return c.mailboxes.Cancel(ctx, addr)
}
