// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

// Keyed metadata (the chunk-map, object recipes, annotations, and the
// source/alias/compound-parent/recovery indexes) lives in a single embedded
// Pebble LSM (roadmap Phase 3) rather than digest-sharded JSONL files. Pebble
// provides O(log n) point lookups, block-compressed and block-checksummed
// SSTs, bloom filters, and WAL-backed crash recovery — the right engine for the
// hundreds of millions of chunk entries a full-corpus deployment accumulates,
// where the old packed-shard scan-per-lookup did not scale.
//
// Key namespaces (prefix-delimited, ASCII byte order):
//
//	c/<chunk-hash>                       chunk index entry
//	r/<object-digest>                    object content recipe
//	a/<object-digest>/<kind>/<analyzer>  object annotation
//	si/<source-ref-key>                  source object index entry
//	sa/<source-alias-key>                source alias -> digest
//	cp/<child-digest>                    compound parent edges
//	rec/<object-digest>                  recovery sidecar
//	pc/<unix-nano>/<object-digest>       projection change index
//	pc-latest/<object-digest>            latest projection change index key
//
// Large blob content remains in the file-based chunk packs; only metadata is in
// Pebble. The metadata store is opened lazily and shared for the process; Close
// releases it.
const metadataDir = "metadata"

func (store *FilesystemStore) meta() (*pebble.DB, error) {
	store.metaOnce.Do(func() {
		store.metaInst, store.metaErr = pebble.Open(
			filepath.Join(store.root, metadataDir),
			&pebble.Options{},
		)
	})

	return store.metaInst, store.metaErr
}

// Close releases the embedded metadata store and any cached pack file handles.
// It is safe to call when the store was never opened.
func (store *FilesystemStore) Close() error {
	if store.packs != nil {
		store.packs.closeAll()
	}

	if store.metaInst == nil {
		return nil
	}

	return store.metaInst.Close()
}

func (store *FilesystemStore) metaGet(key string, out any) (bool, error) {
	db, err := store.meta()
	if err != nil {
		return false, err
	}

	value, closer, err := db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = closer.Close() }()

	if err := json.Unmarshal(value, out); err != nil {
		return false, err
	}

	return true, nil
}

func (store *FilesystemStore) metaPut(key string, value any) error {
	db, err := store.meta()
	if err != nil {
		return err
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return db.Set([]byte(key), encoded, pebble.Sync)
}

func (store *FilesystemStore) metaDelete(key string) error {
	db, err := store.meta()
	if err != nil {
		return err
	}

	return db.Delete([]byte(key), pebble.Sync)
}

// metaPutNoSync and metaDeleteNoSync write without fsyncing the WAL. They are
// reserved for ephemeral, crash-tolerant coordination state — currently only
// the source ingest claims (lk/). The write is still immediately visible to
// later reads in this process (NoSync skips only the WAL fsync, not the
// memtable insert), which is all the claim's in-memory key lock relies on; and
// because the store holds Pebble's exclusive directory lock, a claim never needs
// to be durable across processes. Losing a claim on a crash is harmless — the
// abandoned ingest is simply re-run, and re-ingest is idempotent (content
// dedup + provenance merge) — and is in fact preferable to resurrecting a stale
// claim that would block its source object until the TTL expires. Object data
// (chunks, recipes, manifests, recovery, indexes) is never written this way.
func (store *FilesystemStore) metaPutNoSync(key string, value any) error {
	db, err := store.meta()
	if err != nil {
		return err
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return db.Set([]byte(key), encoded, pebble.NoSync)
}

func (store *FilesystemStore) metaDeleteNoSync(key string) error {
	db, err := store.meta()
	if err != nil {
		return err
	}

	return db.Delete([]byte(key), pebble.NoSync)
}

// metaDeleteKeys atomically deletes a set of keys in one batch.
func (store *FilesystemStore) metaDeleteKeys(keys []string) error {
	db, err := store.meta()
	if err != nil {
		return err
	}

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, key := range keys {
		if err := batch.Delete([]byte(key), nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// metaSnapshot returns a point-in-time read view of the metadata store. The
// caller must Close it. Garbage collection reads reachability from a snapshot so
// concurrent writes do not perturb the mark phase.
func (store *FilesystemStore) metaSnapshot() (*pebble.Snapshot, error) {
	db, err := store.meta()
	if err != nil {
		return nil, err
	}

	return db.NewSnapshot(), nil
}

// snapshotIterPrefix iterates key/value pairs under prefix in a snapshot.
func snapshotIterPrefix(
	snapshot *pebble.Snapshot,
	prefix string,
	fn func(key string, value []byte) error,
) error {
	iter, err := snapshot.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := fn(string(iter.Key()), iter.Value()); err != nil {
			return err
		}
	}

	return iter.Error()
}

// snapshotHas reports whether key exists in the snapshot.
func snapshotHas(snapshot *pebble.Snapshot, key string) (bool, error) {
	_, closer, err := snapshot.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_ = closer.Close()

	return true, nil
}

func (store *FilesystemStore) metaHas(key string) (bool, error) {
	db, err := store.meta()
	if err != nil {
		return false, err
	}

	_, closer, err := db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_ = closer.Close()

	return true, nil
}

// metaIterPrefix calls fn for each key/value whose key starts with prefix. The
// value slice is only valid for the duration of the call.
func (store *FilesystemStore) metaIterPrefix(
	prefix string,
	fn func(key string, value []byte) error,
) error {
	return store.metaIterRange(prefix, prefixUpperBound(prefix), fn)
}

func (store *FilesystemStore) metaIterRange(
	lower string,
	upper []byte,
	fn func(key string, value []byte) error,
) error {
	db, err := store.meta()
	if err != nil {
		return err
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lower),
		UpperBound: upper,
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := fn(string(iter.Key()), iter.Value()); err != nil {
			return err
		}
	}

	return iter.Error()
}

// prefixUpperBound returns the exclusive upper bound for a prefix scan, or nil
// when the prefix is all 0xff (scan to the end).
func prefixUpperBound(prefix string) []byte {
	bound := []byte(prefix)
	for i := len(bound) - 1; i >= 0; i-- {
		if bound[i] != 0xff {
			upper := append([]byte(nil), bound[:i+1]...)
			upper[i]++

			return upper
		}
	}

	return nil
}
