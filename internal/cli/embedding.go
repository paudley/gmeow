// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

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
	var stateFile string

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

			embedConfig := loaded.Config.Analysis.Embeddings
			embedder, err := embedding.NewHTTPEmbedder(embedConfig.Endpoint, embedConfig.Model, nil)
			if err != nil {
				return fmt.Errorf("configure embedding endpoint: %w", err)
			}

			service := embedding.NewService(
				embedding.NewResolver(embedding.NewMemoryCache(), embedder),
				embedding.NewEntityIndex(embedding.FullDim, embedding.CoarseDim),
				embedConfig.Model,
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
				embedConfig.Endpoint,
				embedConfig.Model,
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

func saveEmbeddingState(out io.Writer, service *embedding.Service, path string) error {
	data, err := service.SnapshotState()
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write embedding state %q: %w", path, err)
	}
	_, _ = fmt.Fprintf(out, "embedding serve: persisted %d bytes of resolution state to %q\n", len(data), path)

	return nil
}
