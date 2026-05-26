// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/config"
)

var secretNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

func newConfigCommand(out io.Writer, in io.Reader, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "config",
		Short: "Validate and inspect Gmeow configuration",
	}
	command.AddCommand(newConfigValidateCommand(out, configPath))
	command.AddCommand(newConfigSecretCommand(out, in, configPath))
	return command
}

func newConfigValidateCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate the effective config",
		RunE: func(_ *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(
				out,
				"config valid: %s instance=%s\n",
				loaded.Path,
				loaded.Config.System.InstanceID,
			)
			return err
		},
	}
}

func newConfigSecretCommand(
	out io.Writer,
	in io.Reader,
	configPath *string,
) *cobra.Command {
	command := &cobra.Command{
		Use:   "secret",
		Short: "Inspect encrypted config leaf secrets",
	}
	command.AddCommand(newSecretListCommand(out, configPath))
	command.AddCommand(newSecretSetCommand(in, configPath))
	command.AddCommand(newSecretUnsetCommand(configPath))
	return command
}

func newSecretListCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List secret names and reference counts",
		RunE: func(_ *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			for _, name := range config.SecretNames(loaded) {
				if _, err := fmt.Fprintf(
					out,
					"%s references=%d\n",
					name,
					loaded.SecretReferences[name],
				); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func newSecretSetCommand(in io.Reader, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "set <name>",
		Short: "Validate a secret update request",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if _, err := config.Load(config.Options{Path: *configPath}); err != nil {
				return err
			}
			if err := validateSecretName(args[0]); err != nil {
				return err
			}
			value, err := readSecretValue(in)
			if err != nil {
				return err
			}
			if strings.TrimSpace(value) == "" {
				return errors.New("secret value must not be empty")
			}
			return errors.New("encrypted secret writes are not implemented in Phase 00")
		},
	}
}

func newSecretUnsetCommand(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "unset <name>",
		Short: "Validate a secret removal request",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			name := args[0]
			if err := validateSecretName(name); err != nil {
				return err
			}
			if loaded.SecretReferences[name] > 0 {
				return fmt.Errorf("secret %q is still referenced by enabled config", name)
			}
			return errors.New("encrypted secret writes are not implemented in Phase 00")
		},
	}
}

func validateSecretName(name string) error {
	if !secretNamePattern.MatchString(name) {
		return fmt.Errorf("invalid secret name %q", name)
	}
	return nil
}

func readSecretValue(in io.Reader) (string, error) {
	scanner := bufio.NewScanner(in)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", err
		}
		return "", errors.New("secret value is required on stdin")
	}
	return scanner.Text(), nil
}
