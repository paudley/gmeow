// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/analysis"
	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/embedding"
	"blackcat.ca/gmeow/internal/rpc"
	schedmq "blackcat.ca/gmeow/internal/scheduler/rabbitmq"
)

func NewWorkerCommand(out io.Writer) *cobra.Command {
	var configPath string

	root := &cobra.Command{
		Use:   "gmeow-worker",
		Short: "Gmeow analysis worker",
	}
	root.PersistentFlags().StringVar(&configPath, "config", "", "path to gmeow.toml")
	root.AddCommand(newVersionCommand(out))
	root.AddCommand(newStatusCommand("gmeow-worker", out, &configPath))
	root.AddCommand(newWorkerRunCommand(out, &configPath))

	return root
}

func newWorkerRunCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Consume analysis jobs and write FILESTORE annotations",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			store, err := rpc.NewFilestoreClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Filestore),
			)
			if err != nil {
				return err
			}
			defer store.Close()

			// One manager owns the persistent analyzer backends for the worker's
			// lifetime; closing it terminates them on shutdown.
			manager := analysis.NewBackendManager(analysis.BackendManagerConfig{
				IdleTimeout: backendIdleTimeout(loaded.Config.Analysis),
			})
			defer manager.Close()

			var embeddingService analysis.EmbeddingService
			if workerNeedsAnalyzer(loaded.Config.Analysis, analysis.EmbeddingName) {
				client, clientErr := rpc.NewEmbeddingClient(
					command.Context(),
					rpcEndpoint(loaded.Resolved.RPC.Embedding),
				)
				if clientErr != nil {
					return fmt.Errorf("connect to EMBEDDING service: %w", clientErr)
				}
				defer client.Close()
				if waitErr := waitForEmbeddingService(command.Context(), client); waitErr != nil {
					return waitErr
				}
				embeddingService = client
			}

			registry, err := workerRegistryFromConfig(
				loaded.Config.Analysis,
				manager,
				embeddingService,
			)
			if err != nil {
				return err
			}

			// One consumer source + pool per analyzer queue: a slow model analyzer
			// can never block a fast in-process one, and each analyzer's concurrency
			// is tuned independently.
			var (
				sources []*schedmq.AnalysisJobSource
				options []analysis.RuntimeOption
			)
			closeSources := func() {
				for _, source := range sources {
					_ = source.Close()
				}
			}
			for _, analyzer := range loaded.Config.Analysis.Analyzers {
				workers := analyzerWorkers(analyzer, loaded.Config.Analysis)
				source, err := schedmq.NewAnalysisJobSource(
					command.Context(),
					schedmq.AnalysisJobSourceConfig{
						URL:         loaded.Resolved.RabbitMQ.URL,
						QueuePrefix: loaded.Resolved.Scheduler.QueuePrefix,
						Analyzer:    analyzer.Name,
						Prefetch:    workers,
					},
				)
				if err != nil {
					closeSources()

					return err
				}

				sources = append(sources, source)
				options = append(options, analysis.WithConsumerPool(source, workers))
			}
			defer closeSources()

			runtime, err := analysis.NewRuntime(nil, store, registry, options...)
			if err != nil {
				return err
			}

			if _, err := fmt.Fprintln(out, "analysis worker: started"); err != nil {
				return err
			}

			return runtime.Run(command.Context())
		},
	}
}

type embeddingStatusClient interface {
	Status(context.Context) (rpc.EmbeddingStatus, error)
}

func waitForEmbeddingService(
	ctx context.Context,
	client embeddingStatusClient,
) error {
	deadline := time.NewTimer(2 * time.Minute)
	defer deadline.Stop()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		if _, err := client.Status(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("wait for EMBEDDING service: %w", lastErr)
		case <-ticker.C:
		}
	}
}

func analysisWorkerConcurrency(config config.AnalysisConfig) int {
	if config.WorkerConcurrency > 0 {
		return config.WorkerConcurrency
	}

	return 4
}

// analyzerWorkers returns the consumer-goroutine count for one analyzer queue,
// preferring its per-analyzer Workers setting and falling back to the global
// analysis worker concurrency.
func analyzerWorkers(analyzer config.AnalyzerConfig, cfg config.AnalysisConfig) int {
	if analyzer.Workers > 0 {
		return analyzer.Workers
	}

	return analysisWorkerConcurrency(cfg)
}

func workerNeedsAnalyzer(analysisConfig config.AnalysisConfig, name string) bool {
	for _, analyzer := range analysisConfig.Analyzers {
		if analyzer.Name == name {
			return true
		}
	}

	return false
}

func workerRegistryFromConfig(
	analysisConfig config.AnalysisConfig,
	manager *analysis.BackendManager,
	embeddingService analysis.EmbeddingService,
) (*analysis.Registry, error) {
	defaultRegistry, err := analysis.DefaultRegistry()
	if err != nil {
		return nil, err
	}

	defaults := map[string]analysis.Analyzer{}

	for _, spec := range defaultRegistry.Specs() {
		analyzer, ok := defaultRegistry.Analyzer(spec)
		if ok {
			defaults[analysis.SpecKey(spec)] = analyzer
		}
	}

	registered := []analysis.Analyzer{}

	for _, configured := range analysisConfig.Analyzers {
		spec := analyzerSpecFromConfig(configured)
		switch configured.WorkerKind {
		case "", "go":
			analyzer, err := goAnalyzerFromConfig(
				configured,
				spec,
				defaults,
				analysisConfig,
				embeddingService,
			)
			if err != nil {
				return nil, err
			}

			registered = append(registered, analyzer)
		case "external", "python":
			analyzer, err := externalAnalyzerFromConfig(
				configured,
				spec,
				manager,
				analysisConfig,
			)
			if err != nil {
				return nil, err
			}

			registered = append(registered, analyzer)
		default:
			return nil, fmt.Errorf("unsupported analyzer worker_kind %q", configured.WorkerKind)
		}
	}

	if len(registered) == 0 {
		return nil, errors.New("at least one analysis analyzer must be configured")
	}

	return analysis.NewRegistry(registered...)
}

func goAnalyzerFromConfig(
	configured config.AnalyzerConfig,
	spec contracts.AnalyzerSpec,
	defaults map[string]analysis.Analyzer,
	analysisConfig config.AnalysisConfig,
	embeddingService analysis.EmbeddingService,
) (analysis.Analyzer, error) {
	if spec.Name == analysis.EmbeddingName {
		if embeddingService == nil {
			return nil, errors.New("embedding service is required")
		}

		return analysis.NewEmbeddingAnalyzer(analysis.EmbeddingConfig{
			Namespace: embedding.NamespaceEmailSegment,
			Service:   embeddingService,
		})
	}
	if spec.Name == analysis.SummaryName {
		return analysis.NewSummaryAnalyzer(analysis.SummaryConfig{
			Endpoint: analysisConfig.Summary.Endpoint,
			Model:    analysisConfig.Summary.Model,
		})
	}

	analyzer, ok := defaults[analysis.SpecKey(spec)]
	if !ok {
		return nil, fmt.Errorf(
			"go analyzer %s version %s is not registered",
			configured.Name,
			configured.Version,
		)
	}

	return analyzer, nil
}

func analyzerSpecFromConfig(configured config.AnalyzerConfig) contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:       configured.Name,
		Version:    configured.Version,
		WorkerKind: configured.WorkerKind,
		MediaTypes: append([]string(nil), configured.MediaTypes...),
	}
}

func externalAnalyzerFromConfig(
	configured config.AnalyzerConfig,
	spec contracts.AnalyzerSpec,
	manager *analysis.BackendManager,
	analysisConfig config.AnalysisConfig,
) (analysis.Analyzer, error) {
	timeout, err := parseAnalyzerTimeout(configured.Timeout)
	if err != nil {
		return nil, err
	}

	startup, err := backendStartupTimeout(configured, analysisConfig)
	if err != nil {
		return nil, err
	}

	return analysis.NewExternalCommandAnalyzer(analysis.ExternalCommandConfig{
		Spec:           spec,
		Command:        configured.Command,
		Args:           configured.Args,
		Timeout:        timeout,
		StartupTimeout: startup,
		MaxInstances:   configured.MaxInstances,
		Manager:        manager,
	})
}

// backendIdleTimeout resolves how long a persistent backend may sit idle before
// the reaper reclaims its memory. An unset or unparseable value falls back to the
// manager's own default (one hour), so a missing config never disables reaping.
func backendIdleTimeout(analysisConfig config.AnalysisConfig) time.Duration {
	if analysisConfig.BackendIdleTimeout == "" {
		return 0
	}

	timeout, err := time.ParseDuration(analysisConfig.BackendIdleTimeout)
	if err != nil || timeout <= 0 {
		return 0
	}

	return timeout
}

// backendStartupTimeout resolves the model-load deadline for an analyzer backend.
// A per-analyzer value wins over the analysis-wide default; an unset value returns
// zero so NewExternalCommandAnalyzer applies its built-in default. A malformed
// value is reported rather than silently ignored — startup time is safety-critical
// (too short turns slow model loads into spurious "unavailable" parks).
func backendStartupTimeout(
	configured config.AnalyzerConfig,
	analysisConfig config.AnalysisConfig,
) (time.Duration, error) {
	raw := configured.StartupTimeout
	if raw == "" {
		raw = analysisConfig.BackendStartupTimeout
	}
	if raw == "" {
		return 0, nil
	}

	timeout, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse analyzer startup_timeout: %w", err)
	}

	return timeout, nil
}

func parseAnalyzerTimeout(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}

	timeout, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse analyzer timeout: %w", err)
	}

	return timeout, nil
}
