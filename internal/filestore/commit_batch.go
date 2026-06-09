// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"encoding/json"

	"github.com/cockroachdb/pebble"
)

// metaSink absorbs metadata key/value writes for the chunk-index, recipe,
// manifest, recovery, and source/compound index namespaces. It exists so the
// leaf writers can serve two regimes from one code path:
//
//   - syncSink commits each write immediately with pebble.Sync — the right
//     default for one-off mutations (AttachProvenance, WriteOverlays, repack,
//     verify rebuilds, annotation refreshes).
//   - commitBatch buffers every write into a single pebble.Batch so a whole
//     object Put commits with one fsync, replacing the former ~6-20 fsyncs per
//     small object (two per chunk + one per metadata key) with two. This is the
//     dominant cost when bulk-importing millions of small mail objects.
type metaSink interface {
	set(key string, value any) error
	delete(key string) error
}

// syncSink commits each write immediately, preserving the pre-batch behaviour
// for every caller that is not an object Put.
type syncSink struct {
	store *FilesystemStore
}

func (store *FilesystemStore) syncSink() syncSink {
	return syncSink{store: store}
}

func (sink syncSink) set(key string, value any) error {
	return sink.store.metaPut(key, value)
}

func (sink syncSink) delete(key string) error {
	return sink.store.metaDelete(key)
}

// commitBatch coalesces all metadata writes and pack fsyncs for a single object
// Put into one durable commit. Every chunk-index, recipe, manifest, recovery,
// and source-index write is buffered into a pebble.Batch, and each pack the
// object's chunks landed in is fsynced exactly once; the batch then commits with
// pebble.Sync.
//
// Ordering preserves the crash invariant: the pack bytes are made durable before
// the batch that references them commits, so a crash can only ever leave orphan
// chunk bytes (reclaimed by the next repack), never an index or manifest that
// points at content that is not on disk. Because manifest, recipe, and chunk
// index all land in one atomic batch, the former "write recipe before manifest"
// ordering hazard disappears: the whole object is committed or none of it is.
//
// A commitBatch is single-goroutine (owned by the Put that created it); the
// pebble.Batch and the seen/touchedPacks maps are not accessed concurrently.
type commitBatch struct {
	store        *FilesystemStore
	batch        *pebble.Batch
	touchedPacks map[uint64]struct{}
	// seen deduplicates chunks within this one object: a chunk appended earlier
	// in the same Put is referenced again rather than re-appended. Cross-Put
	// dedup is handled by the committed chunk index (lookupChunk); two concurrent
	// Puts of an identical brand-new chunk may each append a copy, but that is
	// reclaimable dead space, never a dangling reference.
	seen map[string]struct{}
}

func (store *FilesystemStore) newCommitBatch() (*commitBatch, error) {
	db, err := store.meta()
	if err != nil {
		return nil, err
	}

	return &commitBatch{
		store:        store,
		batch:        db.NewBatch(),
		touchedPacks: map[uint64]struct{}{},
		seen:         map[string]struct{}{},
	}, nil
}

func (cb *commitBatch) set(key string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return cb.batch.Set([]byte(key), encoded, nil)
}

func (cb *commitBatch) delete(key string) error {
	return cb.batch.Delete([]byte(key), nil)
}

// commit fsyncs every pack the object's chunks were appended to (and the pack
// directory once) so the bytes are durable, then commits the accumulated
// metadata batch with pebble.Sync. It does not release the batch; the caller's
// deferred close() does that on every path.
func (cb *commitBatch) commit() error {
	if err := cb.store.syncTouchedPacks(cb.touchedPacks); err != nil {
		return err
	}

	return cb.batch.Commit(pebble.Sync)
}

// close releases the batch. It is idempotent and safe to defer immediately after
// newCommitBatch: on the error path it drops the buffered (uncommitted) writes,
// and after a successful commit it simply frees the batch's resources.
func (cb *commitBatch) close() {
	if cb.batch == nil {
		return
	}

	_ = cb.batch.Close()
	cb.batch = nil
}
