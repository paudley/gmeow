// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/source"
	"blackcat.ca/gmeow/internal/source/sourcegrpc"
)

var configuredSourceRetryDelay = 30 * time.Second

func newSourceCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "source",
		Short: "Run source administrative workflows",
	}
	command.AddCommand(newSourceBackfillCommand(out, configPath))
	command.AddCommand(newSourceServeCommand(out, configPath, "serve"))

	return command
}

func newSourceServeCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	return &cobra.Command{
		Use:   use + " <source-name>",
		Short: "Run a BACKEND/SOURCE gRPC service",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			ctx := command.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			sourceConfig, resolved, err := configuredSource(loaded, args[0])
			if err != nil {
				return err
			}
			adapter, err := sourceAdapter(ctx, loaded, args[0])
			if err != nil {
				return err
			}
			filestoreClient, err := rpc.NewFilestoreClient(
				ctx,
				rpcEndpoint(loaded.Resolved.RPC.Filestore),
			)
			if err != nil {
				return err
			}
			defer filestoreClient.Close()
			sourceService, err := source.NewService(filestoreClient)
			if err != nil {
				return err
			}
			sourceServer, err := sourcegrpc.NewServer(adapter, sourceService)
			if err != nil {
				return err
			}

			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			backgroundErr := make(chan error, 1)
			go func() {
				backgroundErr <- runConfiguredSourceWork(
					runCtx,
					sourceService,
					adapter,
					sourceConfig,
				)
			}()

			endpoint := rpcEndpoint(resolved.Endpoint)
			serveErr := make(chan error, 1)
			go func() {
				serveErr <- rpc.Serve(runCtx, endpoint, func(server *grpc.Server) {
					pb.RegisterSourceServiceServer(server, sourceServer)
				})
			}()

			if _, err := fmt.Fprintf(
				out,
				"source serve: %s/%s %s %s\n",
				adapter.Kind(),
				adapter.Name(),
				endpoint.Network,
				endpoint.Address,
			); err != nil {
				cancel()

				return err
			}

			select {
			case err := <-backgroundErr:
				cancel()
				if err != nil {
					return err
				}

				return <-serveErr
			case err := <-serveErr:
				cancel()

				return err
			}
		},
	}
}

func newSourceBackfillCommand(out io.Writer, configPath *string) *cobra.Command {
	var query string
	var mode string
	var confirmInstance string
	var pageSize int
	var maxPages int
	var concurrency int
	var dryRun bool
	var resume bool

	command := &cobra.Command{
		Use:   "backfill <source-name>",
		Short: "Backfill a configured SOURCE into FILESTORE",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			ctx := command.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if !dryRun {
				if err := requireInstanceConfirmation(
					loaded,
					"source backfill",
					confirmInstance,
				); err != nil {
					return err
				}
			}

			adapter, err := sourcePullAdapter(ctx, loaded, args[0])
			if err != nil {
				return err
			}
			filestoreClient, err := rpc.NewFilestoreClient(
				ctx,
				rpcEndpoint(loaded.Resolved.RPC.Filestore),
			)
			if err != nil {
				return err
			}
			defer filestoreClient.Close()
			sourceService, err := source.NewService(filestoreClient)
			if err != nil {
				return err
			}

			cursor := map[string]any{"mode": mode, "query": query}
			if resume {
				stored, found, err := sourceService.ReadCursor(ctx, adapter)
				if err != nil {
					return err
				}
				if found {
					cursor = stored.Cursor
				}
			}
			if dryRun {
				objects, nextCursor, err := adapter.Pull(ctx, nil, source.PullRequest{
					Cursor: cursor,
					Limit:  pageSize,
				})
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(
					out,
					"dry_run source=%s/%s objects=%d next_cursor=%v\n",
					adapter.Kind(),
					adapter.Name(),
					len(objects),
					nextCursor.Cursor,
				)

				return err
			}

			report, err := sourceService.RunBackfill(ctx, adapter, source.BackfillRequest{
				Cursor:      cursor,
				PageSize:    pageSize,
				MaxPages:    maxPages,
				Concurrency: concurrency,
			})
			if printErr := printBackfillReport(out, adapter, report); printErr != nil {
				return printErr
			}

			return err
		},
	}
	command.Flags().StringVar(&query, "query", "", "Gmail search query for full backfill")
	command.Flags().StringVar(&mode, "mode", "full", "backfill mode: full or history")
	command.Flags().IntVar(&pageSize, "page-size", 100, "messages per Gmail page")
	command.Flags().
		IntVar(&maxPages, "max-pages", 0, "maximum pages to process; 0 means all")
	command.Flags().IntVar(&concurrency, "concurrency", 4, "FILESTORE ingest workers")
	command.Flags().
		BoolVar(&dryRun, "dry-run", false, "list and hydrate one page without writing FILESTORE")
	command.Flags().
		BoolVar(&resume, "resume", true, "resume from FILESTORE source cursor when present")
	command.Flags().StringVar(
		&confirmInstance,
		"confirm-instance",
		"",
		"required production-like instance id confirmation",
	)

	return command
}

func configuredSource(
	loaded *config.Loaded,
	name string,
) (config.SourceConfig, config.ResolvedSource, error) {
	for index, sourceConfig := range loaded.Config.Sources {
		if sourceConfig.Name == strings.TrimSpace(name) {
			return sourceConfig, loaded.Resolved.Sources[index], nil
		}
	}

	return config.SourceConfig{}, config.ResolvedSource{}, fmt.Errorf(
		"no source named %q",
		name,
	)
}

func sourcePullAdapter(
	ctx context.Context,
	loaded *config.Loaded,
	name string,
) (source.PullAdapter, error) {
	adapter, err := sourceAdapter(ctx, loaded, name)
	if err != nil {
		return nil, err
	}
	pull, ok := adapter.(source.PullAdapter)
	if ok && hasSourceCapability(adapter, source.CapabilityBackfill) {
		return pull, nil
	}

	return nil, fmt.Errorf("no backfill-capable source named %q", name)
}

func sourceAdapter(
	ctx context.Context,
	loaded *config.Loaded,
	name string,
) (source.Adapter, error) {
	adapters, err := buildSourceAdapters(ctx, loaded)
	if err != nil {
		return nil, err
	}
	for _, adapter := range adapters {
		if adapter.Name() == strings.TrimSpace(name) {
			return adapter, nil
		}
	}

	return nil, fmt.Errorf("no source named %q", name)
}

func runConfiguredSourceWork(
	ctx context.Context,
	service *source.Service,
	adapter source.Adapter,
	sourceConfig config.SourceConfig,
) error {
	pull, ok := adapter.(source.PullAdapter)
	if sourceConfig.Backfill.Enabled || sourceConfig.InboxRefresh.Enabled {
		if !ok {
			return fmt.Errorf(
				"source %s/%s does not support configured pull work",
				adapter.Kind(),
				adapter.Name(),
			)
		}
	}

	if sourceConfig.Backfill.Enabled {
		go func() {
			runConfiguredBackfill(ctx, service, pull, sourceConfig)
		}()
	}
	if sourceConfig.InboxRefresh.Enabled {
		go func() {
			runConfiguredInboxRefresh(ctx, service, pull, sourceConfig)
		}()
	}

	<-ctx.Done()

	return nil
}

func runConfiguredBackfill(
	ctx context.Context,
	service *source.Service,
	pull source.PullAdapter,
	sourceConfig config.SourceConfig,
) {
	for ctx.Err() == nil {
		fmt.Printf("source backfill: started source=%s/%s\n", pull.Kind(), pull.Name())
		_, err := service.RunBackfill(ctx, pull, source.BackfillRequest{
			Cursor: map[string]any{
				"mode":  firstNonEmpty(sourceConfig.Backfill.Mode, "full"),
				"query": sourceConfig.Backfill.Query,
			},
			CursorKey:   firstNonEmpty(sourceConfig.Backfill.CursorKey, "backfill"),
			PageSize:    sourceConfig.Backfill.PageSize,
			MaxPages:    sourceConfig.Backfill.MaxPages,
			Concurrency: sourceConfig.Backfill.Concurrency,
			Resume:      sourceConfig.Backfill.Resume,
		})
		if err != nil && ctx.Err() == nil {
			fmt.Printf(
				"source backfill: failed source=%s/%s error=%v\n",
				pull.Kind(),
				pull.Name(),
				err,
			)
			if !waitConfiguredSourceRetry(ctx) {
				return
			}

			continue
		}
		if ctx.Err() == nil {
			fmt.Printf(
				"source backfill: completed source=%s/%s\n",
				pull.Kind(),
				pull.Name(),
			)
		}

		return
	}
}

func runConfiguredInboxRefresh(
	ctx context.Context,
	service *source.Service,
	pull source.PullAdapter,
	sourceConfig config.SourceConfig,
) {
	interval := sourceRefreshInterval(sourceConfig.InboxRefresh.Interval)
	for {
		query := firstNonEmpty(
			sourceConfig.InboxRefresh.Query,
			"in:inbox newer_than:30d",
		)
		fmt.Printf(
			"source inbox refresh: started source=%s/%s query=%q\n",
			pull.Kind(),
			pull.Name(),
			query,
		)
		report, err := service.RunBackfill(ctx, pull, source.BackfillRequest{
			Cursor: map[string]any{
				"mode":  "full",
				"query": query,
			},
			CursorKey:   "inbox_refresh",
			PageSize:    sourceConfig.InboxRefresh.PageSize,
			MaxPages:    sourceConfig.InboxRefresh.MaxPages,
			Concurrency: sourceConfig.InboxRefresh.Concurrency,
			Resume:      false,
		})
		if err != nil {
			fmt.Printf(
				"source inbox refresh: failed source=%s/%s error=%v\n",
				pull.Kind(),
				pull.Name(),
				err,
			)
			if !waitConfiguredSourceRetry(ctx) {
				return
			}

			continue
		}
		fmt.Printf(
			"source inbox refresh: completed source=%s/%s processed=%d created=%d skipped=%d completed=%t\n",
			pull.Kind(),
			pull.Name(),
			report.Processed,
			report.Created,
			report.Skipped,
			report.Completed,
		)

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
		}
	}
}

func waitConfiguredSourceRetry(ctx context.Context) bool {
	timer := time.NewTimer(configuredSourceRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sourceRefreshInterval(raw string) time.Duration {
	duration, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || duration <= 0 {
		return 5 * time.Minute
	}

	return duration
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}

	return ""
}

func hasSourceCapability(adapter source.Adapter, capability string) bool {
	for _, candidate := range adapter.Capabilities() {
		if candidate == capability {
			return true
		}
	}

	return false
}

func printBackfillReport(
	out io.Writer,
	adapter source.Adapter,
	report source.BackfillReport,
) error {
	_, err := fmt.Fprintf(
		out,
		"source=%s/%s pages=%d processed=%d created=%d skipped=%d failed=%d completed=%t\n",
		adapter.Kind(),
		adapter.Name(),
		report.Pages,
		report.Processed,
		report.Created,
		report.Skipped,
		report.Failed,
		report.Completed,
	)

	return err
}
