// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package testsupport

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	querypg "blackcat.ca/gmeow/internal/query/postgres"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/scheduler"
	schedmq "blackcat.ca/gmeow/internal/scheduler/rabbitmq"
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
	stop   func()
}

func StartQueryGRPC(
	t *testing.T,
	ctx context.Context,
	source *filestore.FilesystemStore,
) *QueryService {
	t.Helper()
	loaded := LoadConfig(t)
	baseDSN := PostgresDSN(loaded.Resolved.Postgres, loaded.Resolved.Postgres.Database)
	schema := createQueryTestSchema(t, ctx, baseDSN)
	queryConfig := querypg.Config{
		ConnString:     dsnWithSearchPath(baseDSN, schema),
		MigrationsDir:  filepath.Join(RepoRoot(t), "migrations", "query"),
		MigrationTable: schema + ".goose_db_version",
	}
	if err := querypg.Migrate(ctx, queryConfig); err != nil {
		t.Fatal(err)
	}
	index, err := querypg.New(ctx, queryConfig, source)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := unixEndpoint(t, "query.sock")
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
		stop: func() {
			_ = client.Close()
			cancel()
			if err := <-errc; err != nil {
				t.Fatalf("stop query grpc: %v", err)
			}
			index.Close()
			dropQueryTestSchema(t, baseDSN, schema)
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
	loaded := LoadConfig(t)
	cfg := schedmq.TestConfigFromResolved(
		loaded.Resolved.RabbitMQ,
		loaded.Resolved.Scheduler,
	)
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
			Priorities:   loaded.Resolved.Scheduler.Priorities,
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

func LoadConfig(t *testing.T) *config.Loaded {
	t.Helper()
	loaded, err := config.Load(config.Options{
		Path: filepath.Join(RepoRoot(t), "gmeow.toml"),
	})
	if err != nil {
		t.Fatalf("load integration config: %v", err)
	}

	return loaded
}

func QueryIntegrationDSN(t *testing.T) string {
	t.Helper()
	loaded := LoadConfig(t)

	return PostgresDSN(loaded.Resolved.Postgres, loaded.Resolved.Postgres.Database)
}

func PostgresDSN(postgres config.ResolvedPostgres, database string) string {
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

func RepoRoot(t *testing.T) string {
	t.Helper()
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(workingDir, "go.mod")); err == nil {
			return workingDir
		}
		parent := filepath.Dir(workingDir)
		if parent == workingDir {
			t.Fatal("repo root with go.mod was not found")
		}
		workingDir = parent
	}
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
			return client
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
			return client
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
			return client
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("scheduler grpc did not become ready")

	return nil
}
