// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/version"
)

type ConfigCommand func(*config.Loaded) error

func NewServiceCommand(name, summary string, out io.Writer) *cobra.Command {
	var configPath string

	root := &cobra.Command{
		Use:   name,
		Short: summary,
	}
	root.PersistentFlags().StringVar(&configPath, "config", "", "path to gmeow.toml")
	root.AddCommand(newVersionCommand(out))
	root.AddCommand(newStatusCommand(name, out, &configPath))
	root.AddCommand(newFilestoreServeCommand(out, &configPath, "filestore-serve"))
	root.AddCommand(newSchedulerServeCommand(out, &configPath, "scheduler-serve"))
	root.AddCommand(newQueryServeCommand(out, &configPath, "query-serve"))
	root.AddCommand(newSourceServeCommand(out, &configPath, "source-serve"))
	root.AddCommand(newMCPServeCommand(out, &configPath))
	root.AddCommand(newMCPHTTPServeCommand(out, &configPath))
	root.AddCommand(newRESTServeCommand(out, &configPath))
	root.AddCommand(newIMAPServeCommand(out, &configPath))
	root.AddCommand(newJMAPServeCommand(out, &configPath))
	root.AddCommand(newObjectSearchCommand(out, &configPath))
	root.AddCommand(newMailSearchCommand(out, &configPath))
	root.AddCommand(newObjectRetrieveCommand(out, &configPath))
	root.AddCommand(newOpsStatusCommand(out, &configPath))
	root.AddCommand(newForceAnalysisCommand(out, &configPath))

	return root
}

func NewAdminCommand(out io.Writer, in io.Reader) *cobra.Command {
	var configPath string

	root := &cobra.Command{
		Use:   "gmeow-admin",
		Short: "Gmeow administrative commands",
	}
	root.PersistentFlags().StringVar(&configPath, "config", "", "path to gmeow.toml")
	root.AddCommand(newVersionCommand(out))
	root.AddCommand(newStatusCommand("gmeow-admin", out, &configPath))
	root.AddCommand(newConfigCommand(out, in, &configPath))
	root.AddCommand(newFilestoreCommand(out, &configPath))
	root.AddCommand(newQueryCommand(out, &configPath))
	root.AddCommand(newSchedulerCommand(out, &configPath))
	root.AddCommand(newSourceCommand(out, &configPath))

	return root
}

func Execute(cmd *cobra.Command) {
	err := cmd.Execute()
	if err != nil {
		slog.Error("command failed", "error", err)
		os.Exit(1)
	}
}

func newVersionCommand(out io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		RunE: func(_ *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(out, version.Version)

			return err
		},
	}
}

func newStatusCommand(name string, out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Validate config and print startup status",
		RunE: func(_ *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				out,
				"%s: config valid for instance %s\n",
				name,
				loaded.Config.System.InstanceID,
			)

			return err
		},
	}
}
