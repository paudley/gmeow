// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"blackat.ca/gmeow/internal/config"
	"blackat.ca/gmeow/internal/version"
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
	return root
}

func Execute(cmd *cobra.Command) {
	if err := cmd.Execute(); err != nil {
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
