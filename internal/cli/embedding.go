// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/renameio"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/embedding"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

// newEmbeddingServeCommand runs the EMBEDDING gRPC service: the claim-vector
// cache + batched calls to the model endpoint, and the in-process coder/hnsw
// entity index used for ingest-time resolution. IMPORT and FILESTORE call it
// over gRPC; it never touches QUERY/SCHEDULER, so the FILESTORE-only-import
// invariant holds.
func newEmbeddingServeCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	var (
		stateFile        string
		manageModel      bool
		ollamaHost       string
		ollamaModel      string
		ollamaModels     string
		embedBatchSize   int
		embedBatchTokens int
		embedPaceMs      int
	)

	command := &cobra.Command{
		Use:   use,
		Short: "Run the EMBEDDING gRPC service",
		RunE: func(command *cobra.Command, _ []string) error {
			// Cancel the serve context on SIGINT/SIGTERM so rpc.Serve returns and
			// the state-snapshot defer actually runs on shutdown (a bare kill would
			// otherwise terminate before persisting).
			ctx, stop := signal.NotifyContext(command.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			endpointURL := loaded.Config.Analysis.Embeddings.Endpoint
			modelName := loaded.Config.Analysis.Embeddings.Model

			// --manage-model: gmeow OWNS the embedding backend. Supervise our own
			// `ollama serve` (auto-restarting on crash, which the hand-run server
			// didn't), ensure the model is pulled, and point the embedder at it. No
			// operator-provided endpoint required.
			if manageModel {
				backend, beErr := embedding.NewOllamaBackend(embedding.OllamaConfig{
					Host:      ollamaHost,
					Model:     ollamaModel,
					ModelsDir: ollamaModels,
				})
				if beErr != nil {
					return beErr
				}
				go func() { _ = backend.Run(ctx) }()
				_, _ = fmt.Fprintf(
					out,
					"embedding serve: managing ollama at %s (model %s)…\n",
					backend.Endpoint(),
					backend.Model(),
				)
				if hErr := backend.WaitHealthy(ctx, 2*time.Minute); hErr != nil {
					return hErr
				}
				if mErr := backend.EnsureModel(ctx); mErr != nil {
					return mErr
				}
				endpointURL = backend.Endpoint()
				modelName = backend.Model()
			}

			// Tuning: when gmeow controls the backend (--manage-model) it is a
			// dedicated server, so default to big batches + no pacing for
			// throughput; otherwise stay conservative for an unknown endpoint.
			// Explicit flags override either default.
			tuning := embedding.EmbedTuning{
				BatchTexts:    12,
				BatchTokenEst: 1500,
				MinInterval:   200 * time.Millisecond,
			}
			if manageModel {
				tuning = embedding.EmbedTuning{BatchTexts: 128, BatchTokenEst: 6000, MinInterval: 0}
			}
			if command.Flags().Changed("embed-batch-size") {
				tuning.BatchTexts = embedBatchSize
			}
			if command.Flags().Changed("embed-batch-tokens") {
				tuning.BatchTokenEst = embedBatchTokens
			}
			if command.Flags().Changed("embed-pace-ms") {
				tuning.MinInterval = time.Duration(embedPaceMs) * time.Millisecond
			}

			embedder, err := embedding.NewHTTPEmbedderTuned(endpointURL, modelName, nil, tuning)
			if err != nil {
				return fmt.Errorf("configure embedding endpoint: %w", err)
			}

			service := embedding.NewService(
				embedding.NewResolver(embedding.NewMemoryCache(), embedder),
				embedding.NewEntityIndex(embedding.FullDim, embedding.CoarseDim),
				modelName,
			)

			// Restore the persisted resolution state (cache + entity index +
			// ledger) so entities and the claim-vector cache survive restarts and
			// re-imports stay idempotent. Snapshot it back when serving stops.
			if stateFile != "" {
				if loadErr := loadEmbeddingState(service, stateFile); loadErr != nil {
					return loadErr
				}
				defer func() {
					if snapErr := saveEmbeddingState(out, service, stateFile); snapErr != nil {
						_, _ = fmt.Fprintf(out, "embedding serve: state snapshot failed: %v\n", snapErr)
					}
				}()
			}

			endpoint := rpcEndpoint(loaded.Resolved.RPC.Embedding)
			if _, err := fmt.Fprintf(
				out,
				"embedding serve: %s %s (endpoint %s, model %s, state %q)\n",
				endpoint.Network,
				endpoint.Address,
				endpointURL,
				modelName,
				stateFile,
			); err != nil {
				return err
			}

			return rpc.Serve(ctx, endpoint, func(server *grpc.Server) {
				pb.RegisterEmbeddingServiceServer(server, rpc.NewEmbeddingServer(service))
			})
		},
	}
	command.Flags().StringVar(
		&stateFile,
		"state-file",
		"",
		"path to persist the resolution state (claim cache + entity index + ledger); "+
			"loaded at startup, snapshotted on shutdown. Empty = in-memory only",
	)
	command.Flags().BoolVar(
		&manageModel,
		"manage-model",
		false,
		"gmeow supervises its own ollama embedding backend (auto-restart on crash, "+
			"auto-pull the model) instead of requiring an operator-provided endpoint",
	)
	command.Flags().
		StringVar(&ollamaHost, "ollama-host", "", "OLLAMA_HOST for the managed backend (default 127.0.0.1:11434)")
	command.Flags().
		StringVar(&ollamaModel, "ollama-model", "", "ollama embedding model tag (default nomic-embed-text)")
	command.Flags().
		StringVar(&ollamaModels, "ollama-models-dir", "", "OLLAMA_MODELS dir so weights live under gmeow's control (default: ollama's own)")
	command.Flags().
		IntVar(&embedBatchSize, "embed-batch-size", 0, "max texts per embedding request (overrides the auto default)")
	command.Flags().
		IntVar(&embedBatchTokens, "embed-batch-tokens", 0, "max estimated tokens per embedding request (overrides the auto default)")
	command.Flags().
		IntVar(&embedPaceMs, "embed-pace-ms", 0, "min ms between embedding requests; 0 = none (overrides the auto default)")

	return command
}

// newEmbeddingRepairCommand runs the global REPAIR re-partition (correlation
// clustering over the signed idDiff edge graph) OFFLINE over a persisted resolution
// state: load the state, consolidate the conservative ingest's over-split by merging
// entities along strong positive signed edges (never across a cannot-link), and
// snapshot the re-partitioned state back. The embedding service must be stopped
// (it owns the state file); the warm claim-vector cache in the snapshot serves the
// centroid re-pool, so the model endpoint is not required for an all-cached state.
func newEmbeddingRepairCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	var (
		stateFile      string
		matchThreshold float64
		nameThreshold  float64
		mergeThreshold float64
		topN           int
	)

	command := &cobra.Command{
		Use:   use,
		Short: "Run the global REPAIR re-partition over a persisted resolution state",
		RunE: func(command *cobra.Command, _ []string) error {
			if stateFile == "" {
				return fmt.Errorf("--state-file is required")
			}

			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			embedder, err := embedding.NewHTTPEmbedder(
				loaded.Config.Analysis.Embeddings.Endpoint,
				loaded.Config.Analysis.Embeddings.Model,
				nil,
			)
			if err != nil {
				return fmt.Errorf("configure embedding endpoint: %w", err)
			}

			service := embedding.NewService(
				embedding.NewResolver(embedding.NewMemoryCache(), embedder),
				embedding.NewEntityIndex(embedding.FullDim, embedding.CoarseDim),
				loaded.Config.Analysis.Embeddings.Model,
			)
			if loadErr := loadEmbeddingState(service, stateFile); loadErr != nil {
				return loadErr
			}

			result, err := service.Repair(command.Context(), embedding.RepairParams{
				Threshold:      matchThreshold,
				NameThreshold:  nameThreshold,
				MergeThreshold: mergeThreshold,
				TopN:           topN,
			})
			if err != nil {
				return fmt.Errorf("repair: %w", err)
			}

			if _, err := fmt.Fprintf(
				out,
				"repair: %d entities → %d (%d records reassigned into %d clusters)\n",
				result.EntitiesBefore, result.EntitiesAfter, result.Merges, result.Clusters,
			); err != nil {
				return err
			}

			return saveEmbeddingState(out, service, stateFile)
		},
	}

	command.Flags().
		StringVar(&stateFile, "state-file", "", "resolution state to re-partition in place (required)")
	command.Flags().
		Float64Var(&matchThreshold, "match-threshold", 0.72, "idDiff set/identifier value-match cosine")
	command.Flags().
		Float64Var(&nameThreshold, "name-threshold", 0.88, "idDiff functional (name) value-match cosine")
	command.Flags().
		Float64Var(&mergeThreshold, "merge-threshold", 0, "min positive signed edge weight to merge (0 = the ingest merge gate)")
	command.Flags().
		IntVar(&topN, "top-n", 0, "blocking neighbours scored per entity (0 = the ingest default)")

	return command
}

func loadEmbeddingState(service *embedding.Service, path string) error {
	data, err := os.ReadFile(path) //nolint:gosec // operator-provided state path
	if err != nil {
		if os.IsNotExist(err) {
			return nil // first run: no state yet
		}

		return fmt.Errorf("read embedding state %q: %w", path, err)
	}

	return service.LoadState(data)
}

// embeddingStatePerm is the file mode for the persisted embedding state.
const embeddingStatePerm os.FileMode = 0o600

func saveEmbeddingState(out io.Writer, service *embedding.Service, path string) error {
	data, err := service.SnapshotState()
	if err != nil {
		return err
	}

	err = renameio.WriteFile(path, data, embeddingStatePerm)
	if err != nil {
		return fmt.Errorf("write embedding state %q: %w", path, err)
	}
	_, _ = fmt.Fprintf(
		out,
		"embedding serve: persisted %d bytes of resolution state to %q\n",
		len(data),
		path,
	)

	return nil
}
