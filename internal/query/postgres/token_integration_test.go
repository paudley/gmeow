// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"testing"

	"blackcat.ca/gmeow/internal/filestore"
)

func TestBearerTokenRoundTrip(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })
	schema := createQueryTestSchema(t, ctx, dsn)
	t.Cleanup(func() { dropQueryTestSchema(t, dsn, schema) })

	config := Config{
		ConnString:     dsnWithSearchPath(dsn, schema),
		MigrationsDir:  migrationsDir,
		MigrationTable: schema + ".goose_db_version",
	}
	if err := Migrate(ctx, config); err != nil {
		t.Fatal(err)
	}
	index, err := New(ctx, config, filestore.NewFilesystemStore(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	token, err := index.CreateBearerToken(ctx, "client-x")
	if err != nil {
		t.Fatal(err)
	}
	if len(token) != 128 {
		t.Fatalf("token length = %d, want 128", len(token))
	}

	clientID, ok, err := index.ValidateBearerToken(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || clientID != "client-x" {
		t.Fatalf("validate ok=%t clientID=%q, want true client-x", ok, clientID)
	}

	if _, ok, err := index.ValidateBearerToken(ctx, "unknown-token"); err != nil || ok {
		t.Fatalf("unknown token validate ok=%t err=%v, want false nil", ok, err)
	}

	if _, ok, err := index.ValidateBearerToken(ctx, ""); err != nil || ok {
		t.Fatalf("empty token validate ok=%t err=%v, want false nil", ok, err)
	}
}
