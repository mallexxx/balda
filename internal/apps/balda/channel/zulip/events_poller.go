package zulip

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

const eventsRegisterRetryDelay = 10 * time.Second

// EventsMessageHandler is called for each stream message from the Events API.
type EventsMessageHandler func(ctx context.Context, ev EventMessage)

// EventsPoller long-polls the Zulip Events API and delivers stream messages
// to the handler. It re-registers automatically when the queue expires.
type EventsPoller struct {
	client  *Client
	handler EventsMessageHandler
	logger  zerolog.Logger
}

// NewEventsPoller creates an EventsPoller.
func NewEventsPoller(
	client *Client,
	handler EventsMessageHandler,
	logger zerolog.Logger,
) *EventsPoller {
	return &EventsPoller{
		client:  client,
		handler: handler,
		logger:  logger.With().Str("component", "balda.zulip.events_poller").Logger(),
	}
}

// Run starts the long-poll loop. It blocks until ctx is cancelled.
func (p *EventsPoller) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		info, err := p.client.registerEventsQueue(ctx)
		if err != nil {
			p.logger.Warn().Err(err).Dur("retry", eventsRegisterRetryDelay).
				Msg("events register failed; retrying")
			select {
			case <-ctx.Done():
				return
			case <-time.After(eventsRegisterRetryDelay):
			}
			continue
		}
		p.logger.Info().Str("queue_id", info.QueueID).
			Int("last_event_id", info.LastEventID).
			Msg("zulip events queue registered")
		p.pollLoop(ctx, info)
	}
}

func (p *EventsPoller) pollLoop(ctx context.Context, info eventsQueueInfo) {
	lastEventID := info.LastEventID
	for {
		if ctx.Err() != nil {
			return
		}
		events, err := p.client.getEvents(ctx, info.QueueID, lastEventID)
		if err != nil {
			p.logger.Warn().Err(err).Msg("events get failed; re-registering")
			return
		}
		for _, ev := range events {
			if ev.ID > lastEventID {
				lastEventID = ev.ID
			}
			if ev.Type != "message" {
				continue
			}
			p.handler(ctx, ev)
		}
	}
}
