// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/config"
)

var (
	secretNamePattern   = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	writableSecretNames = map[string]bool{
		"postgres_password":      true,
		"rabbitmq_password":      true,
		"rabbitmq_test_password": true,
	}
)

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
				updated := secretUpdateMetadata(loaded.Config.Secrets[name])
				if _, err := fmt.Fprintf(
					out,
					"%s references=%d updated=%s\n",
					name,
					loaded.SecretReferences[name],
					updated,
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
		Short: "Update an encrypted password leaf",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			if err := validateSecretName(name); err != nil {
				return err
			}

			if !writableSecretNames[name] {
				return fmt.Errorf("secret %q is not a writable password leaf", name)
			}

			value, err := readSecretValue(in)
			if err != nil {
				return err
			}

			if strings.TrimSpace(value) == "" {
				return errors.New("secret value must not be empty")
			}

			encrypted, err := encryptSecretLeaf(value)
			if err != nil {
				return err
			}

			return updateSecretLeaf(selectedConfigPath(*configPath), name, encrypted)
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
				return fmt.Errorf("secret %q is still referenced by config", name)
			}

			return removeSecretLeaf(selectedConfigPath(*configPath), name)
		},
	}
}

func secretUpdateMetadata(encryptedLeaf string) string {
	if strings.TrimSpace(encryptedLeaf) == "" {
		return "unknown"
	}

	return "unknown"
}

func selectedConfigPath(path string) string {
	if strings.TrimSpace(path) != "" {
		return path
	}

	if envPath := strings.TrimSpace(os.Getenv("GMEOW_CONFIG")); envPath != "" {
		return envPath
	}

	return "gmeow.toml"
}

func encryptSecretLeaf(value string) (string, error) {
	keyPath, err := sopsAgeKeyPath()
	if err != nil {
		return "", err
	}

	recipientBytes, err := exec.Command("age-keygen", "-y", keyPath).Output()
	if err != nil {
		return "", fmt.Errorf("derive SOPS age recipient: %w", err)
	}

	recipient := strings.TrimSpace(string(recipientBytes))
	if recipient == "" {
		return "", errors.New("derive SOPS age recipient: empty recipient")
	}

	command := exec.Command(
		"sops",
		"encrypt",
		"--input-type",
		"binary",
		"--output-type",
		"json",
		"--age",
		recipient,
		"/dev/stdin",
	)
	command.Stdin = strings.NewReader(value)

	output, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf(
				"encrypt secret leaf: %s",
				strings.TrimSpace(string(exitErr.Stderr)),
			)
		}

		return "", fmt.Errorf("encrypt secret leaf: %w", err)
	}

	return string(output), nil
}

func sopsAgeKeyPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory for SOPS age key: %w", err)
	}

	path := filepath.Join(home, ".config", "gmeow", "key.txt")
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("%s is required before secret writes", path)
	}

	return path, nil
}

func updateSecretLeaf(path, name, encrypted string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}

	text := string(raw)
	block := name + " = " + tomlLiteral(encrypted)

	pattern := regexp.MustCompile(
		`(?ms)^` + regexp.QuoteMeta(name) + `\s*=\s*'''` + "\n.*?\n'''",
	)
	if pattern.MatchString(text) {
		text = pattern.ReplaceAllString(text, block)
	} else {
		secretsHeader := regexp.MustCompile(`(?m)^\[secrets\]\s*$`)

		location := secretsHeader.FindStringIndex(text)
		if location == nil {
			text = strings.TrimRight(text, "\n") + "\n\n[secrets]\n" + block + "\n"
		} else {
			insertAt := location[1]
			text = text[:insertAt] + "\n" + block + text[insertAt:]
		}
	}

	if err := atomicWriteConfig(path, []byte(text)); err != nil {
		return fmt.Errorf("write config %s: %w", path, err)
	}

	return nil
}

func removeSecretLeaf(path, name string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}

	pattern := regexp.MustCompile(
		`(?ms)^` + regexp.QuoteMeta(name) + `\s*=\s*'''` + "\n.*?\n'''\n?",
	)

	text := pattern.ReplaceAllString(string(raw), "")
	if text == string(raw) {
		return fmt.Errorf("secret %q is not present in config", name)
	}

	if err := atomicWriteConfig(path, []byte(text)); err != nil {
		return fmt.Errorf("write config %s: %w", path, err)
	}

	return nil
}

func atomicWriteConfig(path string, content []byte) error {
	info, statErr := os.Stat(path)

	perm := os.FileMode(0o600)
	if statErr == nil {
		perm = info.Mode().Perm()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}

	dir := filepath.Dir(path)

	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".")
	if err != nil {
		return err
	}

	tmpPath := file.Name()
	cleanup := true

	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := file.Write(content); err != nil {
		_ = file.Close()

		return err
	}

	if err := file.Chmod(perm); err != nil {
		_ = file.Close()

		return err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()

		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}

	cleanup = false

	dirHandle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirHandle.Close()

	return dirHandle.Sync()
}

func tomlLiteral(value string) string {
	return "'''\n" + strings.TrimRight(value, "\n") + "\n'''"
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
		err := scanner.Err()
		if err != nil {
			return "", err
		}

		return "", errors.New("secret value is required on stdin")
	}

	return scanner.Text(), nil
}
