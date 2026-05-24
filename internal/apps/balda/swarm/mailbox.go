package swarm

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"go.uber.org/fx"
)

const DefaultMailboxActiveLimit = 20

type MailboxService struct {
	store       baldastate.MailboxMessageStore
	bus         WakeBus
	activeLimit int
}

type mailboxServiceParams struct {
	fx.In

	StateProvider baldastate.Provider
	Bus           WakeBus
}

func NewMailboxService(params mailboxServiceParams) (*MailboxService, error) {
	if params.StateProvider == nil {
		return nil, fmt.Errorf("balda state provider is required")
	}
	if params.Bus == nil {
		return nil, fmt.Errorf("swarm wake bus is required")
	}
	return &MailboxService{
		store:       params.StateProvider.Mailboxes(),
		bus:         params.Bus,
		activeLimit: DefaultMailboxActiveLimit,
	}, nil
}

type SubmittedMessage struct {
	MessageID     string
	MailboxID     string
	QueuePosition int
}

func (s *MailboxService) Submit(ctx context.Context, env Envelope) (SubmittedMessage, error) {
	if strings.TrimSpace(env.ID) == "" {
		env.ID = uuid.NewString()
	}
	if err := env.Validate(); err != nil {
		return SubmittedMessage{}, err
	}
	mailboxID, err := env.Target.MailboxID()
	if err != nil {
		return SubmittedMessage{}, err
	}
	subject, err := wakeSubject(env.Target)
	if err != nil {
		return SubmittedMessage{}, err
	}
	payload, err := EncodeEnvelope(env)
	if err != nil {
		return SubmittedMessage{}, err
	}
	position, err := s.store.Enqueue(ctx, baldastate.MailboxMessageRecord{
		MessageID:   env.ID,
		MailboxID:   mailboxID,
		ActorType:   strings.ToLower(strings.TrimSpace(env.Target.Target)),
		ActorKey:    strings.TrimSpace(env.Target.Key),
		Subject:     subject,
		PayloadJSON: payload,
	}, s.activeLimit)
	if err != nil {
		return SubmittedMessage{}, err
	}
	if err := s.bus.Publish(ctx, env.Target); err != nil {
		return SubmittedMessage{}, err
	}
	return SubmittedMessage{MessageID: env.ID, MailboxID: mailboxID, QueuePosition: position}, nil
}

func (s *MailboxService) Cancel(ctx context.Context, addr ActorAddress) (int, error) {
	mailboxID, err := addr.MailboxID()
	if err != nil {
		return 0, err
	}
	return s.store.CancelMailbox(ctx, mailboxID)
}

func IsMailboxFull(err error) bool {
	return errors.Is(err, baldastate.ErrMailboxFull)
}
