// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

const (
	testPostgresDSNEnv     = "GMEOW_TEST_POSTGRES_DSN"
	testQueryMigrationsEnv = "GMEOW_TEST_QUERY_MIGRATIONS_DIR"
)

func TestPostgresProjectsFilestoreWithSourceCursorRebuild(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })
	schema := createQueryTestSchema(t, ctx, dsn)
	t.Cleanup(func() { dropQueryTestSchema(t, dsn, schema) })

	sourceName := "integration-" + randomHex(t, 8)
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello apollo"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: sourceName,
			ExternalID: "apollo-1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteSourceCursor(ctx, contracts.SourceCursor{
		SourceKind: contracts.JMAPMailboxCatalogSourceKind,
		SourceName: contracts.JMAPMailboxCatalogSourceName,
		Cursor: map[string]any{
			contracts.JMAPMailboxCatalogCursorKey: []contracts.JMAPMailbox{{
				MailboxID: "mbox-apollo",
				Name:      "Apollo",
				SortOrder: 80,
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	config := Config{
		ConnString:     dsnWithSearchPath(dsn, schema),
		MigrationsDir:  migrationsDir,
		MigrationTable: schema + ".goose_db_version",
	}
	if err := Migrate(ctx, config); err != nil {
		t.Fatal(err)
	}
	index, err := New(ctx, config, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(index.Close)

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}
	results, err := index.Search(ctx, contracts.SearchRequest{
		Query:  "apollo",
		Facets: []string{"file"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results.Total != 1 || results.Results[0].ObjectDigest != digest {
		t.Fatalf("unexpected search response: %#v", results)
	}

	report, err := index.RebuildReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Projected != 1 || report.Failed != 0 {
		t.Fatalf("unexpected rebuild report: %#v", report)
	}
	mailboxes, err := index.JMAPMailboxes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMailbox(mailboxes, "mbox-apollo") {
		t.Fatalf("rebuild did not project JMAP mailbox catalog: %#v", mailboxes)
	}
}

func TestJMAPMutableMailStateRecoversFromFilestoreRebuild(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })

	store := filestore.NewFilesystemStore(t.TempDir())
	messageDate := "Wed, 27 May 2026 09:15:00 -0600"
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello apollo"),
		MediaType: "message/rfc822",
		Facets: []contracts.Facet{{
			Kind: contracts.MailMessageFacetKind,
			Metadata: map[string]any{
				"rfc_message_id": "<apollo@example.test>",
				"thread_id":      "thread-apollo",
				"subject":        "Apollo update",
				"date":           messageDate,
				"label_ids":      []string{"INBOX", "UNREAD"},
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "jmap",
			ExternalID: "apollo-1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	firstIndex := newMigratedTestIndex(t, ctx, dsn, migrationsDir, store)
	projectStoredObject(t, ctx, firstIndex, store, digest)
	services, err := appsvc.New(appsvc.Options{
		Query:   firstIndex,
		Objects: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	mailboxResult, err := services.UpdateJMAPMailboxes(ctx, appsvc.JMAPMailboxMutation{
		Create: map[string]appsvc.JMAPMailboxCreate{
			"client-1": {Name: "Apollo"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	customMailbox := mailboxResult.Created["client-1"].MailboxID
	if customMailbox == "" {
		t.Fatalf("missing custom mailbox in result: %#v", mailboxResult)
	}
	if _, err := services.UpdateJMAPEmailState(ctx, appsvc.JMAPEmailMutation{
		ObjectDigest: digest,
		MailboxIDs: map[string]bool{
			"inbox":       false,
			customMailbox: true,
		},
		Keywords: map[string]bool{
			"$seen":    true,
			"$flagged": true,
		},
	}); err != nil {
		t.Fatal(err)
	}

	secondIndex := newMigratedTestIndex(t, ctx, dsn, migrationsDir, store)
	if err := secondIndex.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	mailboxes, err := secondIndex.JMAPMailboxes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMailbox(mailboxes, customMailbox) {
		t.Fatalf("rebuild did not recover custom mailbox %q: %#v", customMailbox, mailboxes)
	}
	states, err := secondIndex.JMAPEmailStates(ctx, []contracts.ObjectDigest{digest})
	if err != nil {
		t.Fatal(err)
	}
	state, ok := states[digest]
	if !ok {
		t.Fatalf("rebuild did not recover JMAP email state for %s", digest)
	}
	if !sameStrings(state.MailboxIDs, []string{"all", customMailbox}) ||
		!sameStrings(state.Keywords, []string{"$flagged", "$seen"}) {
		t.Fatalf("unexpected recovered JMAP state: %#v", state)
	}

	query, err := secondIndex.JMAPEmailQuery(ctx, contracts.JMAPEmailQueryRequest{
		After: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sameDigests(query.IDs, []contracts.ObjectDigest{digest}) {
		t.Fatalf("date-filtered JMAP query did not return recovered message: %#v", query)
	}
}

func TestMailArchiveMissingGmailReport(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })
	schema := createQueryTestSchema(t, ctx, dsn)
	t.Cleanup(func() { dropQueryTestSchema(t, dsn, schema) })

	store := filestore.NewFilesystemStore(t.TempDir())
	archiveOnly := putMailIdentityProjectionFixture(
		t,
		ctx,
		store,
		"<archive-only@example.test>",
		contracts.MailArchiveSourceKind,
		"deep",
		false,
	)
	membershipOnly := putMailMembershipProjectionFixture(
		t,
		ctx,
		store,
		"<archive-membership@example.test>",
		"deep",
	)
	_ = putMailIdentityProjectionFixture(
		t,
		ctx,
		store,
		"<in-gmail@example.test>",
		contracts.MailArchiveSourceKind,
		"deep",
		false,
	)
	_ = putMailIdentityProjectionFixture(
		t,
		ctx,
		store,
		"<in-gmail@example.test>",
		"gmail",
		"primary",
		false,
	)
	generated := putMailIdentityProjectionFixture(
		t,
		ctx,
		store,
		"<gmeow-generated-test@gmeow.local>",
		contracts.MailArchiveSourceKind,
		"deep",
		true,
	)

	config := Config{
		ConnString:     dsnWithSearchPath(dsn, schema),
		MigrationsDir:  migrationsDir,
		MigrationTable: schema + ".goose_db_version",
	}
	if err := Migrate(ctx, config); err != nil {
		t.Fatal(err)
	}
	index, err := New(ctx, config, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(index.Close)
	if err := index.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	report, err := index.MailArchiveMissingGmail(ctx, contracts.MailIdentityReportRequest{
		SourceNames: []string{"deep"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Total != 2 ||
		!containsDigest(report.Items, archiveOnly) ||
		!containsDigest(report.Items, membershipOnly) {
		t.Fatalf("unexpected missing Gmail report: %#v", report)
	}
	archiveItem, ok := itemForDigest(report.Items, archiveOnly)
	if !ok ||
		archiveItem.VersionCount != 1 ||
		archiveItem.MaxScale != contracts.VersionScaleMinor {
		t.Fatalf("missing version metadata in report: %#v", report.Items)
	}

	report, err = index.MailArchiveMissingGmail(ctx, contracts.MailIdentityReportRequest{
		SourceNames:      []string{"deep"},
		IncludeGenerated: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Total != 3 || !containsDigest(report.Items, generated) {
		t.Fatalf("expected generated archive identity when requested: %#v", report)
	}

	resolved, err := index.ResolveMailIdentity(ctx, contracts.MailIdentityResolveRequest{
		MessageID: "<archive-only@example.test>",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Total != 1 ||
		len(resolved.Digests) != 1 ||
		resolved.Digests[0] != archiveOnly {
		t.Fatalf("unexpected mail identity resolution: %#v", resolved)
	}
}

func putMailIdentityProjectionFixture(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
	messageID string,
	sourceKind string,
	sourceName string,
	generated bool,
) contracts.ObjectDigest {
	t.Helper()
	digest, err := store.PutCompound(ctx, filestore.CompoundPutRequest{
		ObjectID:     "fixture:" + sourceKind + ":" + sourceName + ":" + messageID,
		MediaType:    "application/vnd.gmeow.fixture+json",
		ContentRoles: []string{contracts.MailMessageContentRole},
		Facets: []contracts.Facet{{
			Kind: contracts.MailMessageFacetKind,
			Metadata: map[string]any{
				"rfc_message_id":       messageID,
				"generated_message_id": generated,
				"max_scale":            contracts.VersionScaleMinor,
				"version_count":        1,
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: sourceKind,
			SourceName: sourceName,
			ExternalID: messageID,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func putMailMembershipProjectionFixture(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
	messageID string,
	sourceName string,
) contracts.ObjectDigest {
	t.Helper()
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:       strings.NewReader(`{"membership":true}`),
		MediaType:    "application/vnd.gmeow.mail-archive-membership+json",
		SourceHint:   "membership",
		ContentRoles: []string{contracts.MailArchiveMembershipRole},
		Facets: []contracts.Facet{{
			Kind: contracts.MailArchiveMembershipFacetKind,
			Metadata: map[string]any{
				"rfc_message_id":       messageID,
				"generated_message_id": false,
				"max_scale":            contracts.VersionScaleTrivial,
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: contracts.MailArchiveSourceKind,
			SourceName: sourceName,
			ExternalID: messageID + ":membership",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func containsDigest(
	items []contracts.MailIdentityReportItem,
	digest contracts.ObjectDigest,
) bool {
	for _, item := range items {
		if item.CanonicalDigest == digest {
			return true
		}
	}
	return false
}

func itemForDigest(
	items []contracts.MailIdentityReportItem,
	digest contracts.ObjectDigest,
) (contracts.MailIdentityReportItem, bool) {
	for _, item := range items {
		if item.CanonicalDigest == digest {
			return item, true
		}
	}

	return contracts.MailIdentityReportItem{}, false
}

func queryIntegrationDSN(t *testing.T) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv(testPostgresDSNEnv))
	if dsn == "" {
		t.Skipf("%s is required for postgres integration tests", testPostgresDSNEnv)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse postgres integration DSN: %v", err)
	}
	database := strings.TrimPrefix(parsed.Path, "/")
	user := parsed.User.Username()
	if !strings.Contains(database, "test") || !strings.Contains(user, "test") {
		t.Fatalf(
			"refusing to run postgres integration test against non-test target database=%q user=%q",
			database,
			user,
		)
	}

	return dsn
}

func queryIntegrationMigrationsDir(t *testing.T) string {
	t.Helper()
	dir := strings.TrimSpace(os.Getenv(testQueryMigrationsEnv))
	if dir == "" {
		t.Skipf("%s is required for postgres integration tests", testQueryMigrationsEnv)
	}
	if !filepath.IsAbs(dir) {
		t.Fatalf("%s must be an absolute path outside the repository", testQueryMigrationsEnv)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("read working directory: %v", err)
	}
	rel, err := filepath.Rel(cwd, dir)
	if err == nil && rel != ".." &&
		!strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("%s must not point inside the repository: %s", testQueryMigrationsEnv, dir)
	}
	if strings.Contains(
		dir,
		string(filepath.Separator)+"data"+string(filepath.Separator),
	) {
		t.Fatalf("%s must not point inside a data directory: %s", testQueryMigrationsEnv, dir)
	}

	return dir
}

func createQueryTestSchema(t *testing.T, ctx context.Context, dsn string) string {
	t.Helper()
	schema := fmt.Sprintf("gmeow_test_%d", time.Now().UnixNano())
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres for test schema: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+quoteIdent(schema)); err != nil {
		t.Fatalf("create postgres test schema %s: %v", schema, err)
	}

	return schema
}

func dropQueryTestSchema(t *testing.T, dsn, schema string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres to drop test schema %s: %v", schema, err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(
		ctx,
		`DROP SCHEMA IF EXISTS `+quoteIdent(schema)+` CASCADE`,
	); err != nil {
		t.Fatalf("drop postgres test schema %s: %v", schema, err)
	}
}

func dsnWithSearchPath(dsn, schema string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	query := parsed.Query()
	query.Set("options", "-c search_path="+schema+",public")
	parsed.RawQuery = query.Encode()

	return parsed.String()
}

func newMigratedTestIndex(
	t *testing.T,
	ctx context.Context,
	dsn string,
	migrationsDir string,
	store *filestore.FilesystemStore,
) *Index {
	t.Helper()
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
	index, err := New(ctx, config, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(index.Close)

	return index
}

func projectStoredObject(
	t *testing.T,
	ctx context.Context,
	index *Index,
	store *filestore.FilesystemStore,
	digest contracts.ObjectDigest,
) {
	t.Helper()
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}
}

func acquireQueryIntegrationLock(
	t *testing.T,
	ctx context.Context,
	dsn string,
) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres for integration lock: %v", err)
	}
	if _, err := conn.Exec(
		ctx,
		"SELECT pg_advisory_lock($1)",
		int64(0x676d656f775154),
	); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("acquire postgres integration lock: %v", err)
	}

	return conn
}

func releaseQueryIntegrationLock(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	if conn == nil {
		return
	}
	ctx := context.Background()
	if _, err := conn.Exec(
		ctx,
		"SELECT pg_advisory_unlock($1)",
		int64(0x676d656f775154),
	); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("release postgres integration lock: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close postgres integration lock: %v", err)
	}
}

func quoteIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func randomHex(t *testing.T, bytes int) string {
	t.Helper()
	buffer := make([]byte, bytes)
	if _, err := rand.Read(buffer); err != nil {
		t.Fatalf("read random bytes: %v", err)
	}
	return hex.EncodeToString(buffer)
}

func hasMailbox(mailboxes []contracts.JMAPMailbox, mailboxID string) bool {
	for _, mailbox := range mailboxes {
		if mailbox.MailboxID == mailboxID {
			return true
		}
	}

	return false
}

func sameStrings(left, right []string) bool {
	slices.Sort(left)
	slices.Sort(right)

	return slices.Equal(left, right)
}

func sameDigests(left, right []contracts.ObjectDigest) bool {
	slices.Sort(left)
	slices.Sort(right)

	return slices.Equal(left, right)
}
