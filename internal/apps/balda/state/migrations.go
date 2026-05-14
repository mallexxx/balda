package state

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var relayMigrationsFS embed.FS

func migrate(ctx context.Context, db *sql.DB) error {
	migrationsDir, err := fs.Sub(relayMigrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("open balda migrations fs: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrationsDir)
	if err != nil {
		return fmt.Errorf("create balda migration provider: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply balda migrations: %w", err)
	}
	return nil
}
