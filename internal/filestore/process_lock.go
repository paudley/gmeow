// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

const packedMetadataLockName = "packed-metadata.lock"

func (store *FilesystemStore) withPackedMetadataLock(
	ctx context.Context,
	fn func() error,
) error {
	unlock, err := acquireDirectoryLock(
		ctx,
		filepath.Join(store.root, ".locks", packedMetadataLockName),
	)
	if err != nil {
		return err
	}
	defer unlock()

	return fn()
}

func acquireDirectoryLock(
	ctx context.Context,
	path string,
) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		err := os.Mkdir(path, 0o700)
		if err == nil {
			_ = fsyncDir(filepath.Dir(path))
			return func() {
				_ = os.Remove(path)
				_ = fsyncDir(filepath.Dir(path))
			}, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
