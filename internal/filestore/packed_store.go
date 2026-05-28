// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	packedSourceIndexDir         = "source-index-v2"
	packedCompoundParentIndexDir = "compound-parent-index-v2"
	packedRecoveryDir            = "recovery-v2"
	packedShardFilename          = "records.jsonl"
)

type packedSourceObjectIndexEntry struct {
	SourceObject contracts.SourceObjectRef `json:"source_object"`
	ObjectDigest contracts.ObjectDigest    `json:"object_digest"`
	UpdatedAt    time.Time                 `json:"updated_at"`
}

type packedCompoundParentIndexEntry struct {
	ChildDigest contracts.ObjectDigest `json:"child_digest"`
	Parents     []compoundParentEdge   `json:"parents"`
	UpdatedAt   time.Time              `json:"updated_at"`
}

type packedRecoveryEntry struct {
	Digest    contracts.ObjectDigest `json:"digest"`
	Recovery  recoverySidecar        `json:"recovery"`
	UpdatedAt time.Time              `json:"updated_at"`
}

func (store *FilesystemStore) writePackedSourceObjectIndex(
	ctx context.Context,
	entry sourceObjectIndexEntry,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sourceObjectRefKey(entry.SourceObject)
	return store.appendPackedRecord(
		ctx,
		"source-index-v2:"+key,
		store.packedSourceIndexShardPath(key),
		packedSourceObjectIndexEntry(entry),
	)
}

func (store *FilesystemStore) readPackedSourceObjectIndex(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (sourceObjectIndexEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return sourceObjectIndexEntry{}, false, err
	}
	key := sourceObjectRefKey(ref)
	path := store.packedSourceIndexShardPath(key)
	var found sourceObjectIndexEntry
	ok := false
	err := scanPackedRecords(path, func(record packedSourceObjectIndexEntry) error {
		if sourceObjectRefsEqual(record.SourceObject, ref) {
			found = sourceObjectIndexEntry(record)
			ok = true
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return sourceObjectIndexEntry{}, false, nil
	}
	if err != nil {
		return sourceObjectIndexEntry{}, false, err
	}

	return found, ok, nil
}

func (store *FilesystemStore) writePackedCompoundParentIndex(
	ctx context.Context,
	record compoundParentIndexRecord,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := string(record.ChildDigest)
	return store.appendPackedRecord(
		ctx,
		"compound-parent-index-v2:"+key,
		store.packedCompoundParentShardPath(key),
		packedCompoundParentIndexEntry{
			ChildDigest: record.ChildDigest,
			Parents:     append([]compoundParentEdge{}, record.Parents...),
			UpdatedAt:   time.Now().UTC(),
		},
	)
}

func (store *FilesystemStore) readPackedCompoundParentIndex(
	ctx context.Context,
	childDigest contracts.ObjectDigest,
) (compoundParentIndexRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return compoundParentIndexRecord{}, false, err
	}
	key := string(childDigest)
	path := store.packedCompoundParentShardPath(key)
	var found compoundParentIndexRecord
	ok := false
	err := scanPackedRecords(path, func(record packedCompoundParentIndexEntry) error {
		if record.ChildDigest == childDigest {
			found = compoundParentIndexRecord{
				SchemaVersion: int(contracts.SchemaVersionPhase00),
				ChildDigest:   record.ChildDigest,
				Parents:       append([]compoundParentEdge{}, record.Parents...),
			}
			ok = true
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return compoundParentIndexRecord{}, false, nil
	}
	if err != nil {
		return compoundParentIndexRecord{}, false, err
	}

	return found, ok, nil
}

func (store *FilesystemStore) writePackedRecovery(
	ctx context.Context,
	digest contracts.ObjectDigest,
	recovery recoverySidecar,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := string(digest)
	return store.appendPackedRecord(
		ctx,
		"recovery-v2:"+key,
		store.packedRecoveryShardPath(key),
		packedRecoveryEntry{
			Digest:    digest,
			Recovery:  recovery,
			UpdatedAt: time.Now().UTC(),
		},
	)
}

func (store *FilesystemStore) readPackedRecovery(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (recoverySidecar, bool, error) {
	if err := ctx.Err(); err != nil {
		return recoverySidecar{}, false, err
	}
	key := string(digest)
	path := store.packedRecoveryShardPath(key)
	var found recoverySidecar
	ok := false
	err := scanPackedRecords(path, func(record packedRecoveryEntry) error {
		if record.Digest == digest {
			found = record.Recovery
			ok = true
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return recoverySidecar{}, false, nil
	}
	if err != nil {
		return recoverySidecar{}, false, err
	}

	return found, ok, nil
}

func (store *FilesystemStore) appendPackedRecord(
	ctx context.Context,
	lockKey string,
	path string,
	record any,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock := store.lockKey(lockKey)
	defer unlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
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

	return fsyncDir(filepath.Dir(path))
}

func scanPackedRecords[T any](path string, fn func(T) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record T
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return fmt.Errorf("decode packed record %s: %w", path, err)
		}
		if err := fn(record); err != nil {
			return err
		}
	}

	return scanner.Err()
}

func (store *FilesystemStore) packedSourceIndexShardPath(key string) string {
	return filepath.Join(
		store.root,
		packedSourceIndexDir,
		key[:2],
		key[2:4],
		packedShardFilename,
	)
}

func (store *FilesystemStore) packedCompoundParentShardPath(key string) string {
	return filepath.Join(
		store.root,
		packedCompoundParentIndexDir,
		"blake3",
		key[:2],
		key[2:4],
		packedShardFilename,
	)
}

func (store *FilesystemStore) packedRecoveryShardPath(key string) string {
	return filepath.Join(
		store.root,
		packedRecoveryDir,
		"blake3",
		key[:2],
		key[2:4],
		packedShardFilename,
	)
}
