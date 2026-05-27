// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/source"
)

func newSourceCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "source",
		Short: "Run source administrative workflows",
	}
	command.AddCommand(newSourceBackfillCommand(out, configPath))

	return command
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
	command.Flags().IntVar(&maxPages, "max-pages", 0, "maximum pages to process; 0 means all")
	command.Flags().IntVar(&concurrency, "concurrency", 4, "FILESTORE ingest workers")
	command.Flags().BoolVar(&dryRun, "dry-run", false, "list and hydrate one page without writing FILESTORE")
	command.Flags().BoolVar(&resume, "resume", true, "resume from FILESTORE source cursor when present")
	command.Flags().StringVar(
		&confirmInstance,
		"confirm-instance",
		"",
		"required production-like instance id confirmation",
	)

	return command
}

func sourcePullAdapter(
	ctx context.Context,
	loaded *config.Loaded,
	name string,
) (source.PullAdapter, error) {
	adapters, err := buildSourceAdapters(ctx, loaded)
	if err != nil {
		return nil, err
	}
	for _, adapter := range adapters {
		pull, ok := adapter.(source.PullAdapter)
		if ok &&
			adapter.Name() == strings.TrimSpace(name) &&
			hasSourceCapability(adapter, source.CapabilityBackfill) {
			return pull, nil
		}
	}

	return nil, fmt.Errorf("no backfill-capable source named %q", name)
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
