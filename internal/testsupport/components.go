// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package testsupport

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	querypg "blackcat.ca/gmeow/internal/query/postgres"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/scheduler"
	schedmq "blackcat.ca/gmeow/internal/scheduler/rabbitmq"
)

const (
	testPostgresDSNEnv = "GMEOW_TEST_POSTGRES_DSN"
	testRabbitMQURLEnv = "GMEOW_TEST_RABBITMQ_URL"
)

type FilestoreService struct {
	Store  *filestore.FilesystemStore
	Client *rpc.FilestoreClient
	Root   string
	stop   func()
}

func StartFilestoreGRPC(t *testing.T, ctx context.Context) *FilestoreService {
	t.Helper()
	return StartFilestoreGRPCAt(t, ctx, t.TempDir())
}

func StartFilestoreGRPCAt(
	t *testing.T,
	ctx context.Context,
	root string,
) *FilestoreService {
	t.Helper()
	store := filestore.NewFilesystemStore(root)
	endpoint := unixEndpoint(t, "filestore.sock")
	serverCtx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		errc <- rpc.Serve(serverCtx, endpoint, func(server *grpc.Server) {
			pb.RegisterFilestoreServiceServer(server, rpc.NewFilestoreServer(store))
		})
	}()
	client := dialFilestore(t, ctx, endpoint, cancel)

	return &FilestoreService{
		Store:  store,
		Client: client,
		Root:   root,
		stop: func() {
			_ = client.Close()
			cancel()
			if err := <-errc; err != nil {
				t.Fatalf("stop filestore grpc: %v", err)
			}
		},
	}
}

func (service *FilestoreService) Close() {
	service.stop()
}

type QueryService struct {
	Index  *querypg.Index
	Client *rpc.QueryClient
	lock   *pgx.Conn
	stop   func()
}

func StartQueryGRPC(
	t *testing.T,
	_ context.Context,
	_ *filestore.FilesystemStore,
) *QueryService {
	t.Helper()
	t.Skip(
		"query integration helper requires migration fixtures; tests must not read repo or operator config paths",
	)
	return nil
}

func (service *QueryService) Close() {
	service.stop()
}

func CleanupQueryObjects(t *testing.T, digests ...contracts.ObjectDigest) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, QueryIntegrationDSN(t))
	if err != nil {
		t.Fatalf("connect cleanup postgres: %v", err)
	}
	defer conn.Close(ctx)
	for _, digest := range digests {
		if _, err := conn.Exec(
			ctx,
			"DELETE FROM query_objects WHERE object_digest = $1",
			string(digest),
		); err != nil {
			t.Fatalf("cleanup query object %s: %v", digest, err)
		}
	}
}

type SchedulerService struct {
	Broker *schedmq.Broker
	Client *rpc.SchedulerClient
	stop   func()
}

func StartSchedulerGRPC(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
	specs []contracts.AnalyzerSpec,
) *SchedulerService {
	t.Helper()
	rabbitURL := strings.TrimSpace(os.Getenv(testRabbitMQURLEnv))
	if rabbitURL == "" {
		t.Skipf("%s is required for RabbitMQ integration tests", testRabbitMQURLEnv)
	}
	cfg := schedmq.Config{URL: rabbitURL}
	cfg.QueuePrefix = fmt.Sprintf("gmeow.test.%d.", time.Now().UnixNano())
	cfg.RetryLimit = 1
	cfg.RetryBackoff = 50 * time.Millisecond
	broker, err := schedmq.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	service, err := scheduler.NewService(
		store,
		broker,
		specs,
		scheduler.Config{
			RetryBackoff: cfg.RetryBackoff,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := unixEndpoint(t, "scheduler.sock")
	serverCtx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		errc <- rpc.Serve(serverCtx, endpoint, func(server *grpc.Server) {
			pb.RegisterSchedulerServiceServer(server, rpc.NewSchedulerServer(service))
		})
	}()
	client := dialScheduler(t, ctx, endpoint, cancel)

	return &SchedulerService{
		Broker: broker,
		Client: client,
		stop: func() {
			_ = client.Close()
			cancel()
			if err := <-errc; err != nil {
				t.Fatalf("stop scheduler grpc: %v", err)
			}
			if err := broker.Close(); err != nil {
				t.Fatalf("close scheduler broker: %v", err)
			}
		},
	}
}

func (service *SchedulerService) Close() {
	service.stop()
}

func NewAnalysisJobSource(
	t *testing.T,
	ctx context.Context,
	queuePrefix string,
) *schedmq.AnalysisJobSource {
	t.Helper()
	rabbitURL := strings.TrimSpace(os.Getenv(testRabbitMQURLEnv))
	if rabbitURL == "" {
		t.Skipf("%s is required for RabbitMQ integration tests", testRabbitMQURLEnv)
	}
	source, err := schedmq.NewAnalysisJobSource(
		ctx,
		schedmq.AnalysisJobSourceConfig{
			URL:         rabbitURL,
			QueuePrefix: queuePrefix,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Fatalf("close scheduler analysis job source: %v", err)
		}
	})

	return source
}

func QueryIntegrationDSN(t *testing.T) string {
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

func quoteIdent(identifier string) string {
	return `"` + identifier + `"`
}

func unixEndpoint(t *testing.T, name string) rpc.Endpoint {
	t.Helper()

	return rpc.Endpoint{Network: "unix", Address: filepath.Join(t.TempDir(), name)}
}

func dialFilestore(
	t *testing.T,
	ctx context.Context,
	endpoint rpc.Endpoint,
	cancel context.CancelFunc,
) *rpc.FilestoreClient {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		client, err := rpc.NewFilestoreClient(ctx, endpoint)
		if err == nil {
			_, _, readyErr := client.LookupSourceObject(ctx, contracts.SourceObjectRef{
				SourceKind: "test",
				SourceName: "readiness",
				ExternalID: "readiness",
			})
			if readyErr == nil || status.Code(readyErr) != codes.Unavailable {
				return client
			}
			_ = client.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("filestore grpc did not become ready")

	return nil
}

func dialQuery(
	t *testing.T,
	ctx context.Context,
	endpoint rpc.Endpoint,
	cancel context.CancelFunc,
) *rpc.QueryClient {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		client, err := rpc.NewQueryClient(ctx, endpoint)
		if err == nil {
			_, readyErr := client.SourceCursors(ctx, contracts.SourceCursorRequest{})
			if readyErr == nil || status.Code(readyErr) != codes.Unavailable {
				return client
			}
			_ = client.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("query grpc did not become ready")

	return nil
}

func dialScheduler(
	t *testing.T,
	ctx context.Context,
	endpoint rpc.Endpoint,
	cancel context.CancelFunc,
) *rpc.SchedulerClient {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		client, err := rpc.NewSchedulerClient(ctx, endpoint)
		if err == nil {
			_, readyErr := client.Status(ctx)
			if readyErr == nil || status.Code(readyErr) != codes.Unavailable {
				return client
			}
			_ = client.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("scheduler grpc did not become ready")

	return nil
}
