package natsbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gnats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type EmbeddedNATS struct {
	Server *server.Server
	Conn   *gnats.Conn
	JS     jetstream.JetStream
}

func StartEmbeddedNATS(ctx context.Context, cfg resolvedConfig) (*EmbeddedNATS, error) {
	opts := &server.Options{
		ServerName:         "balda-internal",
		Host:               cfg.NATS.Host,
		Port:               cfg.NATS.Port,
		NoLog:              true,
		NoSigs:             true,
		JetStream:          true,
		StoreDir:           cfg.StoreDir,
		JetStreamMaxMemory: cfg.MaxMemory,
		JetStreamMaxStore:  cfg.MaxStore,
		SyncAlways:         cfg.NATS.SyncAlways,
	}
	if cfg.NATS.ExposeMonitoring {
		opts.HTTPHost = cfg.NATS.Host
		opts.HTTPPort = -1
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create embedded nats server: %w", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		return nil, errors.New("embedded NATS not ready")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	const maxAttempts = 10
	const connectTimeout = 500 * time.Millisecond

	var (
		conn    *gnats.Conn
		lastErr error
	)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			srv.Shutdown()
			if lastErr != nil {
				return nil, fmt.Errorf("connect embedded nats client: %w", fmt.Errorf("%w: %w", ctx.Err(), lastErr))
			}
			return nil, fmt.Errorf("connect embedded nats client: %w", ctx.Err())
		default:
		}

		conn, err = gnats.Connect(
			srv.ClientURL(),
			gnats.Name("balda-worker"),
			gnats.Timeout(connectTimeout),
			gnats.NoReconnect(),
		)
		if err == nil {
			break
		}
		lastErr = err
		if attempt < maxAttempts {
			time.Sleep(25 * time.Millisecond)
		}
	}
	if conn == nil {
		srv.Shutdown()
		return nil, fmt.Errorf("connect embedded nats client: exceeded %d connection attempts: %w", maxAttempts, lastErr)
	}
	var js jetstream.JetStream
	js, err = jetstream.New(conn)
	if err != nil {
		conn.Close()
		srv.Shutdown()
		return nil, fmt.Errorf("create runtime client: %w", err)
	}
	return &EmbeddedNATS{Server: srv, Conn: conn, JS: js}, nil
}

func (n *EmbeddedNATS) Drain(ctx context.Context) error {
	if n == nil {
		return nil
	}
	if n.Conn != nil {
		done := make(chan error, 1)
		go func() { done <- n.Conn.Drain() }()
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("drain nats connection: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		n.Conn.Close()
	}
	if n.Server != nil {
		n.Server.Shutdown()
	}
	return nil
}
