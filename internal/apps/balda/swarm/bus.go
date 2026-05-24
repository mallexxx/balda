package swarm

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type MessageHandler func(context.Context, string) error

type WakeBus interface {
	Publish(ctx context.Context, addr ActorAddress) error
	Subscribe(ctx context.Context, handler MessageHandler) error
	Close() error
}

type EmbeddedBus struct {
	srv    *server.Server
	conn   *nats.Conn
	logger zerolog.Logger
}

type embeddedBusParams struct {
	fx.In

	LC     fx.Lifecycle
	Logger zerolog.Logger
}

func NewEmbeddedBus(params embeddedBusParams) (*EmbeddedBus, error) {
	opts := &server.Options{
		ServerName: "balda-embedded-nats",
		Host:       "127.0.0.1",
		Port:       -1,
		DontListen: true,
		NoLog:      true,
		NoSigs:     true,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create embedded nats server: %w", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		return nil, fmt.Errorf("embedded nats server did not become ready")
	}

	conn, err := nats.Connect("", nats.InProcessServer(srv), nats.Name("balda-swarm"), nats.NoReconnect())
	if err != nil {
		srv.Shutdown()
		return nil, fmt.Errorf("connect embedded nats client: %w", err)
	}

	bus := &EmbeddedBus{
		srv:    srv,
		conn:   conn,
		logger: params.Logger.With().Str("component", "balda.swarm.bus").Logger(),
	}
	params.LC.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return bus.Close()
		},
	})
	return bus, nil
}

func (b *EmbeddedBus) Publish(ctx context.Context, addr ActorAddress) error {
	subject, err := wakeSubject(addr)
	if err != nil {
		return err
	}
	payload, err := mailboxWakePayload(addr)
	if err != nil {
		return err
	}
	if err := b.conn.Publish(subject, []byte(payload)); err != nil {
		return fmt.Errorf("publish wake subject %q: %w", subject, err)
	}
	if err := b.flush(ctx); err != nil {
		return err
	}
	return nil
}

func (b *EmbeddedBus) Subscribe(ctx context.Context, handler MessageHandler) error {
	_, err := b.conn.Subscribe(wakePattern(), func(msg *nats.Msg) {
		if err := handler(ctx, string(msg.Data)); err != nil {
			b.logger.Warn().Err(err).Str("subject", msg.Subject).Msg("swarm wake handler failed")
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe wake subjects: %w", err)
	}
	return b.flush(ctx)
}

func (b *EmbeddedBus) Close() error {
	var drainErr error
	if b.conn != nil {
		drainErr = b.conn.Drain()
		b.conn.Close()
	}
	if b.srv != nil {
		b.srv.Shutdown()
	}
	if drainErr != nil {
		return fmt.Errorf("drain embedded nats: %w", drainErr)
	}
	return nil
}

func (b *EmbeddedBus) flush(ctx context.Context) error {
	done := make(chan error, 1)
	go func() { done <- b.conn.Flush() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("flush embedded nats: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
