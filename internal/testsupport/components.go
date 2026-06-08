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
	testPostgresDSNEnv     = "GMEOW_TEST_POSTGRES_DSN"
	testQueryMigrationsEnv = "GMEOW_TEST_QUERY_MIGRATIONS_DIR"
	testRabbitMQURLEnv     = "GMEOW_TEST_RABBITMQ_URL"
)

const (
	// queryIntegrationLockKey serializes postgres integration tests through a
	// shared pg_advisory_lock ("gmeowQT" encoded as hex).
	queryIntegrationLockKey = int64(0x676d656f775154)
	// schedulerRetryBackoff is the broker retry backoff used by scheduler tests.
	schedulerRetryBackoff = 50 * time.Millisecond
	// grpcReadyAttempts bounds how many times the dial helpers probe a freshly
	// started gRPC server for readiness.
	grpcReadyAttempts = 50
	// grpcReadyPollInterval is the pause between gRPC readiness probes.
	grpcReadyPollInterval = 20 * time.Millisecond
)

type FilestoreService struct {
	Store  *filestore.FilesystemStore
	Client *rpc.FilestoreClient
	stop   func()
	Root   string
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
	endpoint := UnixEndpoint(t, "filestore.sock")
	serverCtx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)

	go func() {
		//nolint:contextcheck // the filestore server's notify batcher derives its own background ctx; serverCtx still bounds Serve
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

			err := <-errc
			if err != nil {
				t.Fatalf("stop filestore grpc: %v", err)
			}
			// The server is fully stopped, so no goroutine still holds the
			// metadata store; release the embedded Pebble DB rather than leaking
			// an open handle for every test.
			_ = store.Close()
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
	ctx context.Context,
	source *filestore.FilesystemStore,
) *QueryService {
	t.Helper()
	dsn := QueryIntegrationDSN(t)
	migrationsDir := QueryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	schema := createQueryTestSchema(t, ctx, dsn)

	queryConfig := querypg.Config{
		ConnString:     dsnWithSearchPath(dsn, schema),
		MigrationsDir:  migrationsDir,
		MigrationTable: schema + ".goose_db_version",
	}

	err := querypg.Migrate(ctx, queryConfig)
	if err != nil {
		//nolint:contextcheck // cleanup uses context.Background to survive ctx cancellation
		releaseQueryIntegrationLock(t, lock)
		t.Fatal(err)
	}

	index, err := querypg.New(ctx, queryConfig, source)
	if err != nil {
		//nolint:contextcheck // cleanup uses context.Background to survive ctx cancellation
		dropQueryTestSchema(t, dsn, schema)
		//nolint:contextcheck // cleanup uses context.Background to survive ctx cancellation
		releaseQueryIntegrationLock(t, lock)
		t.Fatal(err)
	}

	endpoint := UnixEndpoint(t, "query.sock")
	serverCtx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)

	go func() {
		errc <- rpc.Serve(serverCtx, endpoint, func(server *grpc.Server) {
			pb.RegisterQueryServiceServer(server, rpc.NewQueryServer(index))
		})
	}()

	client := dialQuery(t, ctx, endpoint, cancel)

	return &QueryService{
		Index:  index,
		Client: client,
		lock:   lock,
		//nolint:contextcheck // teardown closure uses context.Background to survive ctx cancellation
		stop: func() {
			_ = client.Close()

			cancel()

			err := <-errc
			if err != nil {
				t.Fatalf("stop query grpc: %v", err)
			}

			index.Close()
			dropQueryTestSchema(t, dsn, schema)
			releaseQueryIntegrationLock(t, lock)
		},
	}
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
		_, err := conn.Exec(
			ctx,
			"DELETE FROM query_objects WHERE object_digest = $1",
			string(digest),
		)
		if err != nil {
			t.Fatalf("cleanup query object %s: %v", digest, err)
		}
	}
}

type SchedulerService struct {
	Broker  *schedmq.Broker
	Client  *rpc.SchedulerClient
	Service *scheduler.Service
	stop    func()
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

	cfg.RetryBackoff = schedulerRetryBackoff
	for _, spec := range specs {
		cfg.Analyzers = append(cfg.Analyzers, spec.Name)
	}

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

	endpoint := UnixEndpoint(t, "scheduler.sock")
	serverCtx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)

	go func() {
		errc <- rpc.Serve(serverCtx, endpoint, func(server *grpc.Server) {
			pb.RegisterSchedulerServiceServer(server, rpc.NewSchedulerServer(service))
		})
	}()

	client := dialScheduler(t, ctx, endpoint, cancel)

	return &SchedulerService{
		Broker:  broker,
		Client:  client,
		Service: service,
		stop: func() {
			_ = client.Close()

			cancel()

			err := <-errc
			if err != nil {
				t.Fatalf("stop scheduler grpc: %v", err)
			}

			err = broker.Close()
			if err != nil {
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
		err := source.Close()
		if err != nil {
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

func QueryIntegrationMigrationsDir(t *testing.T) string {
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

	_, err = conn.Exec(
		ctx,
		"SELECT pg_advisory_lock($1)",
		queryIntegrationLockKey,
	)
	if err != nil {
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

	_, err := conn.Exec(
		ctx,
		"SELECT pg_advisory_unlock($1)",
		queryIntegrationLockKey,
	)
	if err != nil {
		_ = conn.Close(ctx)

		t.Fatalf("release postgres integration lock: %v", err)
	}

	err = conn.Close(ctx)
	if err != nil {
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

	_, err = conn.Exec(ctx, `CREATE SCHEMA `+quoteIdent(schema))
	if err != nil {
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

	_, err = conn.Exec(
		ctx,
		`DROP SCHEMA IF EXISTS `+quoteIdent(schema)+` CASCADE`,
	)
	if err != nil {
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

func UnixEndpoint(t *testing.T, name string) rpc.Endpoint {
	t.Helper()

	// Keep the socket path short and independent of the (possibly long) test
	// name. t.TempDir() embeds the test name, which can push the address past
	// the ~108-byte unix sun_path limit for long-named tests under a long
	// TMPDIR — the server then silently fails to bind and readiness times out.
	//nolint:usetesting // t.TempDir embeds the long test name and overflows the AF_UNIX limit (see above)
	dir, err := os.MkdirTemp("", "gm")
	if err != nil {
		t.Fatalf("create socket dir: %v", err)
	}

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return rpc.Endpoint{Network: "unix", Address: filepath.Join(dir, name)}
}

func dialFilestore(
	t *testing.T,
	ctx context.Context,
	endpoint rpc.Endpoint,
	cancel context.CancelFunc,
) *rpc.FilestoreClient {
	t.Helper()

	for range grpcReadyAttempts {
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

		time.Sleep(grpcReadyPollInterval)
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

	for range grpcReadyAttempts {
		client, err := rpc.NewQueryClient(ctx, endpoint)
		if err == nil {
			_, readyErr := client.SourceCursors(ctx, contracts.SourceCursorRequest{})
			if readyErr == nil || status.Code(readyErr) != codes.Unavailable {
				return client
			}

			_ = client.Close()
		}

		time.Sleep(grpcReadyPollInterval)
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

	for range grpcReadyAttempts {
		client, err := rpc.NewSchedulerClient(ctx, endpoint)
		if err == nil {
			_, readyErr := client.Status(ctx)
			if readyErr == nil || status.Code(readyErr) != codes.Unavailable {
				return client
			}

			_ = client.Close()
		}

		time.Sleep(grpcReadyPollInterval)
	}

	cancel()
	t.Fatal("scheduler grpc did not become ready")

	return nil
}
