// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
)

func newObjectSearchCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		facets []string
		limit  int
	)

	command := &cobra.Command{
		Use:   "search <text>",
		Short: "Search objects through application services",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			services, closeFn, err := openInterfaceServices(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			result, err := services.ObjectSearch(command.Context(), appsvc.SearchOptions{
				Query:  args[0],
				Facets: facets,
				Limit:  limit,
			})

			return writeAppJSON(out, result, err)
		},
	}
	command.Flags().StringSliceVar(&facets, "facet", nil, "facet filter")
	command.Flags().IntVar(&limit, "limit", 20, "maximum results")

	return command
}

func newMailSearchCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		limit       int
		fullMessage bool
	)

	command := &cobra.Command{
		Use:   "mail-search <text>",
		Short: "Search mail messages through application services",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			services, closeFn, err := openInterfaceServices(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			result, err := services.MailSearch(command.Context(), appsvc.SearchOptions{
				Query:       args[0],
				Limit:       limit,
				FullMessage: fullMessage,
			})

			return writeAppJSON(out, result, err)
		},
	}
	command.Flags().IntVar(&limit, "limit", 20, "maximum results")
	command.Flags().
		BoolVar(&fullMessage, "full-msg", false, "include structure for mail results")

	return command
}

func newObjectRetrieveCommand(out io.Writer, configPath *string) *cobra.Command {
	var includeContent bool

	command := &cobra.Command{
		Use:   "retrieve <digest>",
		Short: "Retrieve an object through application services",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			services, closeFn, err := openInterfaceServices(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			result, err := services.Retrieve(
				command.Context(),
				contracts.ObjectDigest(args[0]),
				includeContent,
			)

			return writeAppJSON(out, result, err)
		},
	}
	command.Flags().
		BoolVar(&includeContent, "content", false, "include up to 1MiB object content")

	return command
}

func newOpsStatusCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "ops-status",
		Short: "Inspect operational status through application services",
		RunE: func(command *cobra.Command, _ []string) error {
			services, closeFn, err := openInterfaceServices(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			result, err := services.OpsStatus(command.Context())

			return writeAppJSON(out, result, err)
		},
	}
}

func newForceAnalysisCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		analyzers []string
		traceID   string
	)

	command := &cobra.Command{
		Use:   "force-analysis <digest>",
		Short: "Force analysis through application services",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			services, closeFn, err := openInterfaceServices(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			result, err := services.ForceAnalysis(command.Context(), appsvc.ForceAnalysisRequest{
				Digest:      contracts.ObjectDigest(args[0]),
				Analyzers:   analyzers,
				RequestedBy: "operator",
				TraceID:     traceID,
			})

			return writeAppJSON(out, result, err)
		},
	}
	command.Flags().StringSliceVar(&analyzers, "analyzer", nil, "analyzer name to force")
	command.Flags().
		StringVar(&traceID, "trace-id", "", "trace identifier to include in jobs")

	return command
}

func writeAppJSON[T any](out io.Writer, value T, err error) error {
	if err != nil {
		return err
	}

	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(out, string(encoded))

	return err
}
