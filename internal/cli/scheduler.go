// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/scheduler"
	schedmq "blackcat.ca/gmeow/internal/scheduler/rabbitmq"
)

func newSchedulerCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "scheduler",
		Short: "Manage SCHEDULER work derivation and queues",
	}
	command.AddCommand(newSchedulerScanCommand(out, configPath))
	command.AddCommand(newSchedulerStatusCommand(out, configPath))
	command.AddCommand(newSchedulerPendingCommand(out, configPath))
	command.AddCommand(newSchedulerReconcilePendingCommand(out, configPath, "reconcile-pending"))
	command.AddCommand(newSchedulerReconcilePendingCommand(out, configPath, "prune-pending"))
	command.AddCommand(newSchedulerFailedCommand(out, configPath))
	command.AddCommand(newSchedulerProcessFailedCommand(out, configPath))
	command.AddCommand(newSchedulerDeadLetterCommand(out, configPath))
	command.AddCommand(newSchedulerRequeueCommand(out, configPath))
	command.AddCommand(newSchedulerForceCommand(out, configPath))
	command.AddCommand(newSchedulerRunCommand(out, configPath, "run"))
	command.AddCommand(newSchedulerServeCommand(out, configPath, "serve"))

	return command
}

func newSchedulerScanCommand(out io.Writer, configPath *string) *cobra.Command {
	var priority string

	command := &cobra.Command{
		Use:   "scan",
		Short: "Scan FILESTORE and enqueue missing or stale analysis work",
		RunE: func(command *cobra.Command, _ []string) error {
			service, closeFn, err := openScheduler(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			response, err := service.Scan(command.Context(), contracts.SchedulerScanRequest{
				SchemaVersion: contracts.SchemaVersionPhase00,
				PriorityClass: priority,
				RequestedBy:   "operator",
				Reason:        "scan",
			})
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				out,
				"scheduler scan: scanned=%d enqueued=%d skipped=%d failed=%d\n",
				response.Scanned,
				response.Enqueued,
				response.Skipped,
				response.Failed,
			)

			return err
		},
	}
	command.Flags().
		StringVar(&priority, "priority", contracts.PriorityBackground, "priority class")

	return command
}

func newSchedulerStatusCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Inspect scheduler queue depths",
		RunE: func(command *cobra.Command, _ []string) error {
			service, closeFn, err := openScheduler(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			status, err := service.Status(command.Context())
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				out,
				"scheduler status: pending=%d retry=%d failed=%d dead_letter=%d\n",
				status.Pending,
				status.Retry,
				status.Failed,
				status.DeadLetter,
			)

			return err
		},
	}
}

func newSchedulerPendingCommand(out io.Writer, configPath *string) *cobra.Command {
	var limit int

	command := &cobra.Command{
		Use:   "pending",
		Short: "Inspect pending analysis jobs without consuming them",
		RunE: func(command *cobra.Command, _ []string) error {
			service, closeFn, err := openScheduler(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			response, err := service.PendingJobs(command.Context(), contracts.DeadLetterRequest{
				SchemaVersion: contracts.SchemaVersionPhase00,
				Limit:         limit,
			})
			if err != nil {
				return err
			}

			encoded, err := json.MarshalIndent(response, "", "  ")
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(out, string(encoded))

			return err
		},
	}
	command.Flags().IntVar(&limit, "limit", 20, "maximum pending jobs to inspect")

	return command
}

func newSchedulerReconcilePendingCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	var (
		confirmInstance string
		limit           int
	)

	command := &cobra.Command{
		Use:   use,
		Short: "Reconcile pending analysis jobs by dropping satisfied and duplicate work",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"scheduler "+use,
				confirmInstance,
			); err != nil {
				return err
			}
			service, closeFn, err := openSchedulerLoaded(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer closeFn()

			response, err := service.ReconcilePending(command.Context(), contracts.RequeueRequest{
				SchemaVersion: contracts.SchemaVersionPhase00,
				Limit:         limit,
			})
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				out,
				"scheduler %s: checked=%d dropped_satisfied=%d dropped_duplicate=%d republished=%d kept=%d\n",
				use,
				response.Checked,
				response.DroppedSatisfied,
				response.DroppedDuplicate,
				response.Republished,
				response.Kept,
			)

			return err
		},
	}
	command.Flags().IntVar(&limit, "limit", 100, "maximum pending jobs to inspect")
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "confirm production-like instance id before pruning pending jobs")

	return command
}

func newSchedulerFailedCommand(out io.Writer, configPath *string) *cobra.Command {
	var limit int

	command := &cobra.Command{
		Use:   "failed",
		Short: "Inspect failed analysis jobs waiting for retry routing",
		RunE: func(command *cobra.Command, _ []string) error {
			service, closeFn, err := openScheduler(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			response, err := service.FailedJobs(command.Context(), contracts.DeadLetterRequest{
				SchemaVersion: contracts.SchemaVersionPhase00,
				Limit:         limit,
			})
			if err != nil {
				return err
			}

			encoded, err := json.MarshalIndent(response, "", "  ")
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(out, string(encoded))

			return err
		},
	}
	command.Flags().IntVar(&limit, "limit", 20, "maximum failed jobs to inspect")

	return command
}

func newSchedulerDeadLetterCommand(out io.Writer, configPath *string) *cobra.Command {
	var limit int

	command := &cobra.Command{
		Use:   "dead-letter",
		Short: "Inspect dead-lettered analysis jobs",
		RunE: func(command *cobra.Command, _ []string) error {
			service, closeFn, err := openScheduler(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			response, err := service.DeadLetters(command.Context(), contracts.DeadLetterRequest{
				SchemaVersion: contracts.SchemaVersionPhase00,
				Limit:         limit,
			})
			if err != nil {
				return err
			}

			encoded, err := json.MarshalIndent(response, "", "  ")
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(out, string(encoded))

			return err
		},
	}
	command.Flags().IntVar(&limit, "limit", 20, "maximum dead-letter jobs to inspect")

	return command
}

func newSchedulerProcessFailedCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		confirmInstance string
		limit           int
	)

	command := &cobra.Command{
		Use:   "process-failed",
		Short: "Route failed analysis jobs through scheduler retry handling",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"scheduler process-failed",
				confirmInstance,
			); err != nil {
				return err
			}
			service, closeFn, err := openSchedulerLoaded(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer closeFn()

			response, err := service.ProcessFailures(command.Context(), contracts.RequeueRequest{
				SchemaVersion: contracts.SchemaVersionPhase00,
				Limit:         limit,
			})
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				out,
				"scheduler process-failed: processed=%d\n",
				response.Requeued,
			)

			return err
		},
	}
	command.Flags().IntVar(&limit, "limit", 20, "maximum failed jobs to process")
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "confirm production-like instance id before processing failed jobs")

	return command
}

func newSchedulerRequeueCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		confirmInstance string
		limit           int
	)

	command := &cobra.Command{
		Use:   "requeue",
		Short: "Requeue dead-lettered analysis jobs",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"scheduler requeue",
				confirmInstance,
			); err != nil {
				return err
			}
			service, closeFn, err := openSchedulerLoaded(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer closeFn()

			response, err := service.Requeue(command.Context(), contracts.RequeueRequest{
				SchemaVersion: contracts.SchemaVersionPhase00,
				Limit:         limit,
			})
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(out, "scheduler requeue: requeued=%d\n", response.Requeued)

			return err
		},
	}
	command.Flags().IntVar(&limit, "limit", 20, "maximum dead-letter jobs to requeue")
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "confirm production-like instance id before requeueing")

	return command
}

func newSchedulerForceCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		analyzers []string
		traceID   string
	)

	command := &cobra.Command{
		Use:   "force <digest>",
		Short: "Force reanalysis for a FILESTORE object",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			service, closeFn, err := openScheduler(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			response, err := service.Force(
				command.Context(),
				contracts.ObjectDigest(args[0]),
				analyzers,
				"operator",
				traceID,
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				out,
				"scheduler force: scanned=%d enqueued=%d skipped=%d failed=%d\n",
				response.Scanned,
				response.Enqueued,
				response.Skipped,
				response.Failed,
			)

			return err
		},
	}
	command.Flags().StringSliceVar(&analyzers, "analyzer", nil, "analyzer name to force")
	command.Flags().
		StringVar(&traceID, "trace-id", "", "trace identifier to include in jobs")

	return command
}

func newSchedulerRunCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: "Run the scheduler background loop",
		RunE: func(command *cobra.Command, _ []string) error {
			service, closeFn, err := openSchedulerWithProjector(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			if _, err := fmt.Fprintln(out, "scheduler run: started"); err != nil {
				return err
			}

			return service.Run(command.Context())
		},
	}
}

func newSchedulerServeCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: "Run the SCHEDULER gRPC service",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			service, closeFn, err := openSchedulerLoaded(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer closeFn()

			endpoint := rpcEndpoint(loaded.Resolved.RPC.Scheduler)
			if _, err := fmt.Fprintf(
				out,
				"scheduler serve: %s %s\n",
				endpoint.Network,
				endpoint.Address,
			); err != nil {
				return err
			}

			return rpc.Serve(command.Context(), endpoint, func(server *grpc.Server) {
				pb.RegisterSchedulerServiceServer(server, rpc.NewSchedulerServer(service))
			})
		},
	}
}

func openScheduler(
	ctx context.Context,
	configPath *string,
) (*scheduler.Service, func(), error) {
	loaded, err := config.Load(config.Options{Path: *configPath})
	if err != nil {
		return nil, nil, err
	}

	return openSchedulerLoaded(ctx, loaded)
}

func openSchedulerWithProjector(
	ctx context.Context,
	configPath *string,
) (*scheduler.Service, func(), error) {
	loaded, err := config.Load(config.Options{Path: *configPath})
	if err != nil {
		return nil, nil, err
	}

	queryClient, err := rpc.NewQueryClient(ctx, rpcEndpoint(loaded.Resolved.RPC.Query))
	if err != nil {
		return nil, nil, err
	}

	service, closeFn, err := openSchedulerLoaded(
		ctx,
		loaded,
		scheduler.WithProjector(queryClient),
	)
	if err != nil {
		_ = queryClient.Close()

		return nil, nil, err
	}

	return service, func() {
		closeFn()
		_ = queryClient.Close()
	}, nil
}

func openSchedulerLoaded(
	ctx context.Context,
	loaded *config.Loaded,
	options ...scheduler.Option,
) (*scheduler.Service, func(), error) {
	root, err := resolvedFilestoreRoot(loaded)
	if err != nil {
		return nil, nil, err
	}

	store := filestore.NewFilesystemStore(root)

	broker, err := schedmq.New(ctx, schedmq.ConfigFromResolved(
		loaded.Resolved.RabbitMQ,
		loaded.Resolved.Scheduler,
	))
	if err != nil {
		return nil, nil, err
	}

	schedulerConfig, err := schedulerConfigFromResolved(loaded.Resolved.Scheduler)
	if err != nil {
		broker.Close()

		return nil, nil, err
	}

	service, err := scheduler.NewService(
		store,
		broker,
		scheduler.SpecsFromConfig(loaded.Config.Analysis.Analyzers),
		schedulerConfig,
		options...,
	)
	if err != nil {
		broker.Close()

		return nil, nil, err
	}

	return service, func() { _ = broker.Close() }, nil
}

func schedulerConfigFromResolved(
	resolved config.ResolvedScheduler,
) (scheduler.Config, error) {
	scanInterval, err := time.ParseDuration(resolved.ScanInterval)
	if err != nil {
		return scheduler.Config{}, fmt.Errorf("parse scheduler.scan_interval: %w", err)
	}

	return scheduler.Config{
		ScanInterval: scanInterval,
		RetryBackoff: retryBackoffDuration(resolved.RetryBackoff),
		Priorities:   resolved.Priorities,
	}, nil
}

func retryBackoffDuration(raw string) time.Duration {
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return 30 * time.Second
	}

	return duration
}
