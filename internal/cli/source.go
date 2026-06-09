// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	schedmq "blackcat.ca/gmeow/internal/scheduler/rabbitmq"
	"blackcat.ca/gmeow/internal/source"
	"blackcat.ca/gmeow/internal/source/sourcegrpc"
)

var configuredSourceRetryDelay = 30 * time.Second

const schedulerPressureCheckTimeout = 5 * time.Second

// schedulerPressureGate reports the scheduler's analysis backpressure to the
// source with the same high/low-water hysteresis the scheduler applies
// internally: it engages at or above the high-water mark and only releases once
// depth falls to the low-water mark, so backfill does not flap on and off near
// a single threshold. It satisfies source.PressureReporter.
type schedulerPressureGate struct {
	client    *rpc.SchedulerClient
	mu        sync.Mutex
	highWater int
	lowWater  int
	pressured bool
}

func newSchedulerPressureGate(
	client *rpc.SchedulerClient,
	resolved config.ResolvedScheduler,
) *schedulerPressureGate {
	high := resolved.BackpressureHighWater
	if high <= 0 {
		high = 10000
	}

	low := resolved.BackpressureLowWater
	if low <= 0 || low > high {
		low = high / 2
	}

	return &schedulerPressureGate{client: client, highWater: high, lowWater: low}
}

func (gate *schedulerPressureGate) Pressured(ctx context.Context) (bool, error) {
	checkCtx, cancel := context.WithTimeout(ctx, schedulerPressureCheckTimeout)
	defer cancel()

	status, err := gate.client.Status(checkCtx)
	if err != nil {
		return false, err
	}

	depth := status.Pending + status.Retry

	gate.mu.Lock()
	defer gate.mu.Unlock()

	if gate.pressured && depth <= gate.lowWater {
		gate.pressured = false
	} else if !gate.pressured && depth >= gate.highWater {
		gate.pressured = true
	}

	return gate.pressured, nil
}

func newSourceCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "source",
		Short: "Run source administrative workflows",
	}
	command.AddCommand(newSourceBackfillCommand(out, configPath))
	command.AddCommand(newSourceImportCommand(out, configPath))
	command.AddCommand(newSourceImportWorkerCommand(out, configPath))
	command.AddCommand(newSourceListImportsCommand(out, configPath))
	command.AddCommand(newSourceDeleteImportCommand(out, configPath))
	command.AddCommand(newSourceServeCommand(out, configPath, "serve"))

	return command
}

func newSourceDeleteImportCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		confirmInstance string
		stateDir        string
	)

	command := &cobra.Command{
		Use:   "delete-import <run-id>",
		Short: "Remove an import's provenance; objects with no remaining source are reclaimable by gc",
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
			if err := requireInstanceConfirmation(
				loaded,
				"source delete-import",
				confirmInstance,
			); err != nil {
				return err
			}

			runID := args[0]
			dir := archiveImportStateDir(loaded, stateDir)
			record, found, err := source.LoadImportRunRecord(dir, runID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("import run %q not found in %s", runID, dir)
			}

			filestoreClient, err := rpc.NewFilestoreClient(
				ctx,
				rpcEndpoint(loaded.Resolved.RPC.Filestore),
			)
			if err != nil {
				return err
			}
			defer filestoreClient.Close()

			report, err := filestoreClient.DeleteImport(
				ctx,
				record.SourceKind,
				record.SourceName,
			)
			if err != nil {
				return err
			}

			record.Status = source.ImportRunStatusDeleted
			if writeErr := source.WriteImportRunRecord(dir, record); writeErr != nil {
				return writeErr
			}

			encoded, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintln(out, string(encoded)); err != nil {
				return err
			}
			_, err = fmt.Fprintln(
				out,
				"run marked deleted; run 'gmeow-admin filestore gc' to reclaim freed chunks",
			)

			return err
		},
	}
	command.Flags().StringVar(
		&confirmInstance,
		"confirm-instance",
		"",
		"required production-like instance id confirmation",
	)
	command.Flags().StringVar(
		&stateDir,
		"state-dir",
		"",
		"import run state directory; defaults to system.data_dir/import-runs",
	)

	return command
}

func newSourceListImportsCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		jsonOutput bool
		stateDir   string
	)

	command := &cobra.Command{
		Use:   "list-imports",
		Short: "List recorded archive import runs",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			records, err := source.ListImportRunRecords(archiveImportStateDir(loaded, stateDir))
			if err != nil {
				return err
			}

			if jsonOutput {
				encoded, err := json.MarshalIndent(records, "", "  ")
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(out, string(encoded))

				return err
			}

			if len(records) == 0 {
				_, err := fmt.Fprintln(out, "no import runs recorded")

				return err
			}

			writer := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
			fmt.Fprintln(writer, "RUN ID\tSOURCE\tSTATUS\tIMPORTED\tDUP\tFAIL\tSTARTED\tROOTS")
			for _, record := range records {
				fmt.Fprintf(
					writer,
					"%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\n",
					record.RunID,
					record.SourceName,
					record.Status,
					record.Imported,
					record.Duplicates,
					record.Failures,
					record.StartedAt.Local().Format("2006-01-02 15:04"),
					strings.Join(record.Roots, ","),
				)
			}

			return writer.Flush()
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit the import runs as JSON")
	command.Flags().StringVar(
		&stateDir,
		"state-dir",
		"",
		"import run state directory; defaults to system.data_dir/import-runs",
	)

	return command
}

func newSourceImportCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		sourceName      string
		format          string
		confirmInstance string
		stateDir        string
		dryRun          bool
		lowNoise        bool
		enqueueOnly     bool
		concurrency     int
		queueHighWater  int
		quiet           bool
	)

	command := &cobra.Command{
		Use:   "import <root...>",
		Short: "Import read-only archive mail roots into FILESTORE",
		Args:  cobra.MinimumNArgs(1),
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
					"source import",
					confirmInstance,
				); err != nil {
					return err
				}
			}

			filestoreClient, err := rpc.NewFilestoreClient(
				ctx,
				rpcEndpoint(loaded.Resolved.RPC.Filestore),
			)
			if err != nil {
				return err
			}
			defer filestoreClient.Close()

			importer, err := source.NewArchiveImporter(filestoreClient)
			if err != nil {
				return err
			}
			importer.SetConcurrency(concurrency)
			request := source.ArchiveImportRequest{
				SourceName: sourceName,
				Format:     format,
				Roots:      args,
				DryRun:     dryRun,
				LowNoise:   lowNoise,
				StateDir:   archiveImportStateDir(loaded, stateDir),
			}
			if queueHighWater > 0 {
				request.QueueHighWater = queueHighWater
			} else if enqueueOnly {
				request.QueueHighWater = 250000
			}
			// Live progress goes to stderr so --json stdout stays clean.
			if !quiet {
				request.Progress = func(progress source.ArchiveImportProgress) {
					fmt.Fprintf(
						os.Stderr,
						"\rimport: total=%d scanned=%d parsed=%d ingested=%d failures=%d %.0f msg/s elapsed=%s   ",
						progress.Total,
						progress.Scanned,
						progress.Parsed,
						progress.Ingested,
						progress.Failures,
						progress.MessagesPerSecond,
						progress.Elapsed.Round(time.Second),
					)
				}
			}
			var report source.ArchiveImportReport
			if enqueueOnly {
				broker, brokerErr := schedmq.New(ctx, schedmq.ConfigFromResolved(
					loaded.Resolved.RabbitMQ,
					loaded.Resolved.Scheduler,
					loaded.Config.Analysis.Analyzers,
				))
				if brokerErr != nil {
					return brokerErr
				}
				defer broker.Close()
				request.Publisher = broker
				report, err = (source.ArchiveImportQueuedRun{
					Importer: importer,
				}).EnqueueOnly(ctx, request)
			} else {
				report, err = importer.Import(ctx, request)
			}
			if !quiet {
				// Terminate the in-place progress line before the report prints.
				fmt.Fprintln(os.Stderr)
			}
			if printErr := printArchiveImportReport(out, report); printErr != nil {
				return printErr
			}

			return err
		},
	}
	command.Flags().
		StringVar(&sourceName, "source-name", "", "archive source name; defaults to first root basename")
	command.Flags().
		StringVar(&format, "format", source.ArchiveImportFormatAuto, "archive format: auto, maildir, mbox, nnml, mh, eml-dir")
	command.Flags().
		BoolVar(&dryRun, "dry-run", false, "scan and parse without writing FILESTORE")
	command.Flags().
		BoolVar(&lowNoise, "low-noise", false, "skip Message-ID/body-line matches and trivial archive differences without per-message writes")
	command.Flags().
		BoolVar(&enqueueOnly, "enqueue-only", false, "scan archive roots, enqueue source-import jobs, and exit without processing them")
	command.Flags().
		StringVar(&stateDir, "state-dir", "", "local import run state directory; defaults to system.data_dir/import-runs")
	command.Flags().
		IntVar(&concurrency, "concurrency", 8, "object Puts run in parallel across messages and their parts")
	command.Flags().
		IntVar(&queueHighWater, "queue-high-water", 0, "source-import queue high-water mark before enqueue waits; enqueue-only defaults high")
	command.Flags().
		BoolVar(&quiet, "quiet", false, "suppress the live progress line on stderr")
	command.Flags().StringVar(
		&confirmInstance,
		"confirm-instance",
		"",
		"required production-like instance id confirmation",
	)

	return command
}

func newSourceImportWorkerCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		confirmInstance string
		stateDir        string
		maxJobs         int
		prefetch        int
		workers         int
		quiet           bool
	)

	command := &cobra.Command{
		Use:   "import-worker",
		Short: "Process queued archive source-import jobs",
		RunE: func(command *cobra.Command, _ []string) error {
			ctx := command.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"source import-worker",
				confirmInstance,
			); err != nil {
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

			importer, err := source.NewArchiveImporter(filestoreClient)
			if err != nil {
				return err
			}

			jobSource, err := schedmq.NewSourceImportJobSource(
				ctx,
				schedmq.SourceImportJobSourceConfig{
					URL:         loaded.Resolved.RabbitMQ.URL,
					QueuePrefix: loaded.Resolved.Scheduler.QueuePrefix,
					Prefetch:    prefetch,
				},
			)
			if err != nil {
				return err
			}
			defer jobSource.Close()

			broker, err := schedmq.New(ctx, schedmq.ConfigFromResolved(
				loaded.Resolved.RabbitMQ,
				loaded.Resolved.Scheduler,
				loaded.Config.Analysis.Analyzers,
			))
			if err != nil {
				return err
			}
			defer broker.Close()

			report, err := runSourceImportWorker(
				ctx,
				importer,
				sourceImportJobSourceAdapter{source: jobSource},
				broker,
				archiveImportStateDir(loaded, stateDir),
				maxJobs,
				workers,
				quiet,
			)
			if printErr := printArchiveImportReport(out, report); printErr != nil {
				return printErr
			}

			return err
		},
	}
	command.Flags().
		IntVar(&maxJobs, "jobs", 0, "maximum jobs to process before exiting; 0 runs until interrupted")
	command.Flags().
		IntVar(&prefetch, "prefetch", 8, "RabbitMQ source-import prefetch count")
	command.Flags().
		IntVar(&workers, "workers", 4, "concurrent source-import workers in this process")
	command.Flags().
		StringVar(&stateDir, "state-dir", "", "local import run state directory; defaults to system.data_dir/import-runs")
	command.Flags().
		BoolVar(&quiet, "quiet", false, "suppress progress logs on stderr")
	command.Flags().StringVar(
		&confirmInstance,
		"confirm-instance",
		"",
		"required production-like instance id confirmation",
	)

	return command
}

type sourceImportJobSourceAdapter struct {
	source *schedmq.SourceImportJobSource
}

func (adapter sourceImportJobSourceAdapter) Receive(
	ctx context.Context,
) (source.ArchiveImportJobReceipt, error) {
	return adapter.source.Receive(ctx)
}

func runSourceImportWorker(
	ctx context.Context,
	importer *source.ArchiveImporter,
	jobSource source.ArchiveImportJobSource,
	publisher source.ArchiveImportPublisher,
	stateDir string,
	maxJobs int,
	workers int,
	quiet bool,
) (source.ArchiveImportReport, error) {
	if workers < 1 {
		workers = 1
	}
	report := source.ArchiveImportReport{}
	start := time.Now()
	var (
		mu      sync.Mutex
		claimed atomic.Int64
		joined  error
		group   sync.WaitGroup
	)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				if maxJobs > 0 && claimed.Add(1) > int64(maxJobs) {
					return
				}
				err := runSourceImportWorkerJob(
					workerCtx,
					importer,
					jobSource,
					publisher,
					stateDir,
					&report,
					&mu,
					start,
					quiet,
				)
				if err == nil {
					continue
				}
				if errors.Is(err, context.Canceled) {
					return
				}
				mu.Lock()
				joined = errors.Join(joined, err)
				mu.Unlock()
				cancel()

				return
			}
		}()
	}
	group.Wait()

	return report, joined
}

func runSourceImportWorkerJob(
	ctx context.Context,
	importer *source.ArchiveImporter,
	jobSource source.ArchiveImportJobSource,
	publisher source.ArchiveImportPublisher,
	stateDir string,
	report *source.ArchiveImportReport,
	mu *sync.Mutex,
	start time.Time,
	quiet bool,
) error {
	receipt, err := jobSource.Receive(ctx)
	if err != nil {
		return err
	}
	job := receipt.Job()
	mu.Lock()
	report.SourceName = job.SourceName
	report.RunID = job.RunID
	mu.Unlock()
	request := source.ArchiveImportRequest{
		SourceName: job.SourceName,
		Format:     job.Format,
		RunID:      job.RunID,
		DryRun:     job.DryRun,
		LowNoise:   job.LowNoise,
		StateDir:   stateDir,
	}
	local := source.ArchiveImportReport{SourceName: job.SourceName, RunID: job.RunID}
	err = importer.ProcessSourceImportJob(
		ctx,
		job.SourceName,
		job,
		request,
		&local,
	)
	if err != nil {
		if errors.Is(err, source.ErrArchiveMessageRejected) {
			if ackErr := receipt.Ack(ctx); ackErr != nil {
				return ackErr
			}
			mu.Lock()
			report.ParseFailures += local.ParseFailures
			report.Failures = append(report.Failures, err.Error())
			report.Processed++
			if !quiet {
				logSourceImportWorkerProgress(*report, start)
			}
			mu.Unlock()

			return nil
		}
		if retryErr := receipt.Retry(ctx, err); retryErr != nil {
			return retryErr
		}
		mu.Lock()
		report.Failures = append(report.Failures, err.Error())
		if !quiet {
			logSourceImportWorkerProgress(*report, start)
		}
		mu.Unlock()
		_, processErr := publisher.ProcessSourceImportFailures(
			ctx,
			sourceImportFailureProcessLimitForCLI(),
		)

		return processErr
	}
	if ackErr := receipt.Ack(ctx); ackErr != nil {
		return ackErr
	}
	mu.Lock()
	report.Processed++
	report.Imported += local.Imported
	report.ExactDuplicates += local.ExactDuplicates
	report.MessageIDDuplicates += local.MessageIDDuplicates
	report.GeneratedMessageIDs += local.GeneratedMessageIDs
	report.LowNoiseSkipped += local.LowNoiseSkipped
	report.TrivialSkipped += local.TrivialSkipped
	report.MinorVersions += local.MinorVersions
	report.MajorVersions += local.MajorVersions
	report.Promoted += local.Promoted
	report.Collisions += local.Collisions
	if !quiet {
		logSourceImportWorkerProgress(*report, start)
	}
	mu.Unlock()

	return nil
}

func sourceImportFailureProcessLimitForCLI() int {
	return 100
}

func logSourceImportWorkerProgress(report source.ArchiveImportReport, start time.Time) {
	elapsed := time.Since(start).Round(time.Second)
	rate := 0.0
	if seconds := time.Since(start).Seconds(); seconds > 0 {
		rate = float64(report.Processed) / seconds
	}
	fmt.Fprintf(
		os.Stderr,
		"\rimport-worker: processed=%d imported=%d duplicates=%d failures=%d %.0f msg/s elapsed=%s   ",
		report.Processed,
		report.Imported,
		report.ExactDuplicates+report.MessageIDDuplicates,
		len(report.Failures),
		rate,
		elapsed,
	)
}

func archiveImportStateDir(loaded *config.Loaded, override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}

	return filepath.Join(loaded.Config.System.DataDir, "import-runs")
}

func printArchiveImportReport(out io.Writer, report source.ArchiveImportReport) error {
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(encoded))

	return err
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

			// Wire the scheduler's backpressure signal so the configured backfill
			// pauses when the analysis queue is deep and resumes once it drains —
			// the source self-throttles instead of firehosing the work queue.
			schedulerClient, err := rpc.NewSchedulerClient(
				ctx,
				rpcEndpoint(loaded.Resolved.RPC.Scheduler),
			)
			if err != nil {
				return err
			}
			defer schedulerClient.Close()
			sourceService.SetPressureReporter(
				newSchedulerPressureGate(schedulerClient, loaded.Resolved.Scheduler),
			)

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
				Cursor:        cursor,
				PriorityClass: contracts.PriorityBackground,
				PageSize:      pageSize,
				MaxPages:      maxPages,
				Concurrency:   concurrency,
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
			// Backfill is low priority and the one producer that pauses under
			// analysis backpressure.
			PriorityClass: contracts.PriorityBackground,
			CursorKey:     firstNonEmpty(sourceConfig.Backfill.CursorKey, "backfill"),
			PageSize:      sourceConfig.Backfill.PageSize,
			MaxPages:      sourceConfig.Backfill.MaxPages,
			Concurrency:   sourceConfig.Backfill.Concurrency,
			Resume:        sourceConfig.Backfill.Resume,
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
	queries := inboxRefreshQueries(sourceConfig.InboxRefresh)
refreshLoop:
	for {
		for index, query := range queries {
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
				// Inbox refresh is high priority and runs at full speed: it never pauses
				// under analysis backpressure, and its analysis preempts backfill's.
				PriorityClass: contracts.PriorityFreshIngest,
				CursorKey:     inboxRefreshCursorKey(index),
				PageSize:      sourceConfig.InboxRefresh.PageSize,
				MaxPages:      sourceConfig.InboxRefresh.MaxPages,
				Concurrency:   sourceConfig.InboxRefresh.Concurrency,
				Resume:        false,
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Printf(
					"source inbox refresh: failed source=%s/%s query=%q error=%v\n",
					pull.Kind(),
					pull.Name(),
					query,
					err,
				)
				if !waitConfiguredSourceRetry(ctx) {
					return
				}

				continue refreshLoop
			}
			fmt.Printf(
				"source inbox refresh: completed source=%s/%s query=%q processed=%d created=%d skipped=%d completed=%t\n",
				pull.Kind(),
				pull.Name(),
				query,
				report.Processed,
				report.Created,
				report.Skipped,
				report.Completed,
			)
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
		}
	}
}

func inboxRefreshQueries(refreshConfig config.SourceInboxRefreshConfig) []string {
	configuredQueries := refreshConfig.Queries
	if len(configuredQueries) == 0 {
		configuredQueries = []string{refreshConfig.Query}
	}

	queries := make([]string, 0, len(configuredQueries))
	for _, query := range configuredQueries {
		query = strings.TrimSpace(query)
		if query != "" {
			queries = append(queries, query)
		}
	}
	if len(queries) == 0 {
		return []string{"in:inbox newer_than:30d"}
	}

	return queries
}

func inboxRefreshCursorKey(index int) string {
	if index == 0 {
		return "inbox_refresh"
	}

	return fmt.Sprintf("inbox_refresh_%d", index+1)
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
