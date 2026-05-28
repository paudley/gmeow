// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type CleanupLocksReport struct {
	RemovedFiles int `json:"removed_files"`
	RemovedDirs  int `json:"removed_dirs"`
	ScannedDirs  int `json:"scanned_dirs"`
}

type CompactReport struct {
	SourceIndexes         int  `json:"source_indexes"`
	CompoundParentIndexes int  `json:"compound_parent_indexes"`
	RecoverySidecars      int  `json:"recovery_sidecars"`
	RemovedFiles          int  `json:"removed_files"`
	RemovedDirs           int  `json:"removed_dirs"`
	DryRun                bool `json:"dry_run"`
}

func (store *FilesystemStore) CleanupSourceLocks(
	ctx context.Context,
) (CleanupLocksReport, error) {
	report := CleanupLocksReport{}
	base := filepath.Join(store.root, "source-locks")
	err := filepath.WalkDir(
		base,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) && path == base {
					return nil
				}
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() {
				report.ScannedDirs++
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if time.Since(info.ModTime()) <= sourceIngestClaimTTL {
				return nil
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			report.RemovedFiles++

			return nil
		},
	)
	if errors.Is(err, os.ErrNotExist) {
		return report, nil
	}
	if err != nil {
		return report, err
	}
	removed, err := removeEmptyDirs(ctx, base)
	report.RemovedDirs = removed

	return report, err
}

func (store *FilesystemStore) CompactV1Layout(
	ctx context.Context,
	dryRun bool,
) (CompactReport, error) {
	report := CompactReport{DryRun: dryRun}
	if err := store.compactSourceIndexes(ctx, dryRun, &report); err != nil {
		return report, err
	}
	if err := store.compactCompoundParentIndexes(ctx, dryRun, &report); err != nil {
		return report, err
	}
	if err := store.compactRecoverySidecars(ctx, dryRun, &report); err != nil {
		return report, err
	}
	if !dryRun {
		for _, dir := range []string{
			filepath.Join(store.root, "source-index"),
			filepath.Join(store.root, "compound-parent-index"),
		} {
			removed, err := removeEmptyDirs(ctx, dir)
			report.RemovedDirs += removed
			if err != nil {
				return report, err
			}
		}
	}

	return report, nil
}

func (store *FilesystemStore) ExportRecoveryJSON(
	ctx context.Context,
	digest contracts.ObjectDigest,
) ([]byte, error) {
	if err := validateObjectDigest(digest); err != nil {
		return nil, err
	}
	if recovery, found, err := store.readPackedRecovery(ctx, digest); err != nil {
		return nil, err
	} else if found {
		return json.MarshalIndent(recovery, "", "  ")
	}

	return os.ReadFile(store.objectPath(digest, recoveryFilename))
}

func (store *FilesystemStore) compactSourceIndexes(
	ctx context.Context,
	dryRun bool,
	report *CompactReport,
) error {
	base := filepath.Join(store.root, "source-index")
	return walkJSONFiles(ctx, base, func(path string) error {
		var entry sourceObjectIndexEntry
		if err := readJSON(path, &entry); err != nil {
			return err
		}
		report.SourceIndexes++
		if dryRun {
			return nil
		}
		if err := store.writePackedSourceObjectIndex(ctx, entry); err != nil {
			return err
		}
		return removeCompactedFile(path, &report.RemovedFiles)
	})
}

func (store *FilesystemStore) compactCompoundParentIndexes(
	ctx context.Context,
	dryRun bool,
	report *CompactReport,
) error {
	base := filepath.Join(store.root, "compound-parent-index", "blake3")
	return walkJSONFiles(ctx, base, func(path string) error {
		var record compoundParentIndexRecord
		if err := readJSON(path, &record); err != nil {
			return err
		}
		report.CompoundParentIndexes++
		if dryRun {
			return nil
		}
		if err := store.writePackedCompoundParentIndex(ctx, record); err != nil {
			return err
		}
		return removeCompactedFile(path, &report.RemovedFiles)
	})
}

func (store *FilesystemStore) compactRecoverySidecars(
	ctx context.Context,
	dryRun bool,
	report *CompactReport,
) error {
	base := filepath.Join(store.root, "objects", "blake3")
	return filepath.WalkDir(
		base,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) && path == base {
					return nil
				}
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() || entry.Name() != recoveryFilename {
				return nil
			}
			var recovery recoverySidecar
			if err := readJSON(path, &recovery); err != nil {
				return err
			}
			digest := contracts.ObjectDigest(recovery.Digest)
			if err := validateObjectDigest(digest); err != nil {
				return err
			}
			report.RecoverySidecars++
			if dryRun {
				return nil
			}
			if err := store.writePackedRecovery(ctx, digest, recovery); err != nil {
				return err
			}
			return removeCompactedFile(path, &report.RemovedFiles)
		},
	)
}

func walkJSONFiles(ctx context.Context, base string, fn func(string) error) error {
	err := filepath.WalkDir(
		base,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) && path == base {
					return nil
				}
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				return nil
			}
			return fn(path)
		},
	)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

func removeCompactedFile(path string, counter *int) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	(*counter)++

	return fsyncDir(filepath.Dir(path))
}

func removeEmptyDirs(ctx context.Context, base string) (int, error) {
	removed := 0
	dirs := []string{}
	err := filepath.WalkDir(
		base,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) && path == base {
					return nil
				}
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() && path != base {
				dirs = append(dirs, path)
			}
			return nil
		},
	)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	sort.SliceStable(dirs, func(left, right int) bool {
		return len(dirs[left]) > len(dirs[right])
	})
	for _, path := range dirs {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if err := os.Remove(path); err == nil {
			removed++
		} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscallENOTEMPTY()) {
			return removed, err
		}
	}

	return removed, nil
}

func syscallENOTEMPTY() error {
	return syscall.ENOTEMPTY
}
