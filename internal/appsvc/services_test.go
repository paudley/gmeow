// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc_test

import (
	"context"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/query/memory"
	querypg "blackcat.ca/gmeow/internal/query/postgres"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/scheduler"
	"blackcat.ca/gmeow/internal/source"
)

func TestMailSearchUsesRealQueryAndGmailAdapter(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	index := openPostgresIndex(t, ctx, store)
	defer index.Close()

	indexedDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello bob indexed"),
		MediaType: "message/rfc822",
		Facets:    []contracts.Facet{{Kind: appsvc.MailMessageFacet}},
		Provenance: []contracts.Provenance{{
			SourceKind: "gmail",
			SourceName: "primary",
			ExternalID: "indexed",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupQueryObjects(t, indexedDigest) })

	manifest, err := store.ReadManifest(ctx, indexedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}

	filestoreClient, stopFilestore := startFilestoreRPC(t, ctx, store)
	defer stopFilestore()
	sourceService, err := source.NewService(filestoreClient)
	if err != nil {
		t.Fatal(err)
	}
	gmail, err := source.NewGmailAdapter("primary", gmailAPIMock{
		hits: []source.GmailSearchHit{
			{MessageID: "indexed"},
			{MessageID: "fresh", Version: "1"},
		},
		messages: map[string]source.GmailMessage{
			"fresh": {
				MessageID:    "fresh",
				Version:      "1",
				Subject:      "fresh bob",
				Body:         []byte("hello bob fresh"),
				BodyMediaTyp: "text/plain",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	services, err := appsvc.New(appsvc.Options{
		Query:   index,
		Objects: store,
		Sources: appsvc.NewStaticSourceRegistry(gmail),
		Ingest:  sourceService,
	})
	if err != nil {
		t.Fatal(err)
	}

	response, err := services.MailSearch(ctx, appsvc.SearchOptions{Query: "bob", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}

	if response.Total != 2 {
		t.Fatalf("expected indexed and hydrated Gmail results, got %#v", response)
	}
	if !hasPendingFreshResult(response.Results, "fresh") {
		t.Fatalf("expected hydrated live result with pending states: %#v", response.Results)
	}
}

func TestSourceActionUsesRealGmailAdapterAndRejectsWrongFacet(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	index := memoryIndex(t, ctx, store)
	gmail, err := source.NewGmailAdapter("primary", gmailAPIMock{})
	if err != nil {
		t.Fatal(err)
	}
	services, err := appsvc.New(appsvc.Options{
		Query:   index,
		Objects: store,
		Sources: appsvc.NewStaticSourceRegistry(gmail),
	})
	if err != nil {
		t.Fatal(err)
	}

	mailDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("mail"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: appsvc.MailMessageFacet}},
		Provenance: []contracts.Provenance{{
			SourceKind: "gmail",
			SourceName: "primary",
			ExternalID: "msg-1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	action, err := services.SourceAction(ctx, appsvc.SourceActionRequest{
		Digest: mailDigest,
		Action: "archive",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !action.Applied || action.Attributes["message_id"] != "msg-1" {
		t.Fatalf("unexpected source action result: %#v", action)
	}

	fileDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("file"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "gmail",
			SourceName: "primary",
			ExternalID: "msg-2",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := services.SourceAction(ctx, appsvc.SourceActionRequest{Digest: fileDigest, Action: "archive"}); err == nil {
		t.Fatal("expected non-mail object action rejection")
	}
}

func TestForceAnalysisUsesRealSchedulerService(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	index := memoryIndex(t, ctx, store)
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("force me"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := scheduler.NewService(
		store,
		scheduler.NewMemoryBroker(),
		[]contracts.AnalyzerSpec{{Name: "summary", Version: "1", WorkerKind: "go", Deterministic: true}},
		scheduler.Config{},
	)
	if err != nil {
		t.Fatal(err)
	}
	services, err := appsvc.New(appsvc.Options{
		Query:     index,
		Objects:   store,
		Scheduler: service,
	})
	if err != nil {
		t.Fatal(err)
	}

	response, err := services.ForceAnalysis(ctx, appsvc.ForceAnalysisRequest{
		Digest:      digest,
		Analyzers:   []string{"summary"},
		RequestedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued != 1 || response.Scanned != 1 {
		t.Fatalf("unexpected force response: %#v", response)
	}
}

type gmailAPIMock struct {
	hits     []source.GmailSearchHit
	messages map[string]source.GmailMessage
}

func (mock gmailAPIMock) Search(
	context.Context,
	string,
	int,
) ([]source.GmailSearchHit, error) {
	return append([]source.GmailSearchHit{}, mock.hits...), nil
}

func (mock gmailAPIMock) GetMessage(
	_ context.Context,
	messageID string,
) (source.GmailMessage, error) {
	return mock.messages[messageID], nil
}

func (mock gmailAPIMock) ModifyMessage(
	_ context.Context,
	messageID string,
	_ string,
	_ map[string]any,
) (map[string]any, error) {
	return map[string]any{"ok": true, "message_id": messageID}, nil
}

func memoryIndex(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
) appsvc.QueryReader {
	t.Helper()
	index := memory.New(store)
	if err := index.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	return index
}

func openPostgresIndex(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
) *querypg.Index {
	t.Helper()
	dsn := queryIntegrationDSN(t)
	queryConfig := querypg.Config{ConnString: dsn, MigrationsDir: "../../migrations/query"}
	if err := querypg.Migrate(ctx, queryConfig); err != nil {
		t.Fatal(err)
	}

	index, err := querypg.New(ctx, queryConfig, store)
	if err != nil {
		t.Fatal(err)
	}

	return index
}

func startFilestoreRPC(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
) (*rpc.FilestoreClient, func()) {
	t.Helper()
	serverCtx, cancel := context.WithCancel(ctx)
	endpoint := rpc.Endpoint{
		Network: "unix",
		Address: filepath.Join(t.TempDir(), "filestore.sock"),
	}
	errc := make(chan error, 1)
	go func() {
		errc <- rpc.Serve(serverCtx, endpoint, func(server *grpc.Server) {
			pb.RegisterFilestoreServiceServer(server, rpc.NewFilestoreServer(store))
		})
	}()

	var client *rpc.FilestoreClient
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		client, err = rpc.NewFilestoreClient(ctx, endpoint)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		cancel()
		t.Fatal(err)
	}

	return client, func() {
		_ = client.Close()
		cancel()
		<-errc
	}
}

func hasPendingFreshResult(results []appsvc.ObjectSearchResult, externalID string) bool {
	for _, result := range results {
		if result.Attributes["external_id"] == externalID &&
			result.AnalysisPending &&
			result.ProjectionPending {
			return true
		}
	}

	return false
}

func queryIntegrationDSN(t *testing.T) string {
	t.Helper()
	loaded, err := config.Load(config.Options{
		Path: filepath.Join("..", "..", "gmeow.toml"),
	})
	if err != nil {
		t.Fatalf("load integration config: %v", err)
	}

	return postgresDSN(loaded.Resolved.Postgres, loaded.Resolved.Postgres.Database)
}

func postgresDSN(postgres config.ResolvedPostgres, database string) string {
	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(postgres.User, postgres.Password),
		Host:   postgres.Host + ":" + strconv.Itoa(postgres.Port),
		Path:   database,
	}
	query := dsn.Query()
	query.Set("sslmode", postgres.SSLMode)
	dsn.RawQuery = query.Encode()

	return dsn.String()
}

func cleanupQueryObjects(t *testing.T, digests ...contracts.ObjectDigest) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, queryIntegrationDSN(t))
	if err != nil {
		t.Fatalf("connect cleanup postgres: %v", err)
	}
	defer conn.Close(ctx)

	for _, digest := range digests {
		if _, err := conn.Exec(ctx, "DELETE FROM query_objects WHERE object_digest = $1", string(digest)); err != nil {
			t.Fatalf("cleanup query object %s: %v", digest, err)
		}
	}
}
