// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/spf13/cobra"

	"blackat.ca/gmeow/internal/config"
	"blackat.ca/gmeow/internal/filestore"
)

func newFilestoreCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "filestore",
		Short: "Inspect and verify FILESTORE data",
	}
	command.AddCommand(newFilestoreVerifyCommand(out, configPath))
	return command
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
