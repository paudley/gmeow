// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/analysis"
	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
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
			source, err := analysis.NewRabbitMQSource(
				command.Context(),
				analysis.RabbitMQSourceConfig{
					URL:         loaded.Resolved.RabbitMQ.URL,
					QueuePrefix: loaded.Resolved.Scheduler.QueuePrefix,
				},
			)
			if err != nil {
				return err
			}
			defer source.Close()
			store, err := rpc.NewFilestoreClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Filestore),
			)
			if err != nil {
				return err
			}
			defer store.Close()
			registry, err := workerRegistryFromConfig(loaded.Config.Analysis.Analyzers)
			if err != nil {
				return err
			}
			runtime, err := analysis.NewRuntime(
				source,
				store,
				registry,
			)
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

func workerRegistryFromConfig(
	analyzers []config.AnalyzerConfig,
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
	for _, configured := range analyzers {
		spec := analyzerSpecFromConfig(configured)
		switch configured.WorkerKind {
		case "", "go":
			analyzer, ok := defaults[analysis.SpecKey(spec)]
			if !ok {
				return nil, fmt.Errorf(
					"go analyzer %s version %s is not registered",
					spec.Name,
					spec.Version,
				)
			}
			registered = append(registered, analyzer)
		case "external", "python":
			analyzer, err := externalAnalyzerFromConfig(configured, spec)
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
) (analysis.Analyzer, error) {
	timeout, err := parseAnalyzerTimeout(configured.Timeout)
	if err != nil {
		return nil, err
	}
	return analysis.NewExternalCommandAnalyzer(analysis.ExternalCommandConfig{
		Spec:    spec,
		Command: configured.Command,
		Args:    configured.Args,
		Timeout: timeout,
	})
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
