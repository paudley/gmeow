// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

func newFilestoreCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "filestore",
		Short: "Inspect and verify FILESTORE data",
	}
	command.AddCommand(newFilestoreVerifyCommand(out, configPath))
	command.AddCommand(newFilestoreServeCommand(out, configPath, "serve"))

	return command
}

func newFilestoreServeCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: "Run the FILESTORE gRPC service",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			root, err := resolvedFilestoreRoot(loaded)
			if err != nil {
				return err
			}

			store := filestore.NewFilesystemStore(root)
			schedulerClient, err := rpc.NewSchedulerClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Scheduler),
			)
			if err != nil {
				return err
			}
			defer schedulerClient.Close()

			endpoint := rpcEndpoint(loaded.Resolved.RPC.Filestore)
			if _, err := fmt.Fprintf(
				out,
				"filestore serve: %s %s\n",
				endpoint.Network,
				endpoint.Address,
			); err != nil {
				return err
			}

			return rpc.Serve(command.Context(), endpoint, func(server *grpc.Server) {
				pb.RegisterFilestoreServiceServer(
					server,
					rpc.NewFilestoreServer(
						store,
						rpc.WithObjectChangeNotifier(schedulerClient),
					),
				)
			})
		},
	}
}

func newFilestoreVerifyCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "verify",
		Short: "Verify FILESTORE object directories",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			root, err := resolvedFilestoreRoot(loaded)
			if err != nil {
				return err
			}

			store := filestore.NewFilesystemStore(root)

			report, err := store.Verify(command.Context())
			if err != nil {
				return err
			}

			if _, err := fmt.Fprintf(
				out,
				"filestore verify: status=%s checked=%d findings=%d\n",
				report.Status,
				report.Checked,
				len(report.Findings),
			); err != nil {
				return err
			}

			for _, finding := range report.Findings {
				if _, err := fmt.Fprintf(
					out,
					"%s %s %s %s\n",
					finding.Code,
					finding.Digest,
					finding.Path,
					finding.Message,
				); err != nil {
					return err
				}
			}

			if report.Status == filestore.VerifyStatusError {
				return errors.New("filestore verification failed")
			}

			return nil
		},
	}
}

func resolvedFilestoreRoot(loaded *config.Loaded) (string, error) {
	root := loaded.Config.Filestore.Root
	if filepath.IsAbs(root) {
		return root, nil
	}

	if loaded.Path == "" {
		return "", errors.New("relative filestore.root requires a loaded config path")
	}

	return filepath.Join(filepath.Dir(loaded.Path), root), nil
}
