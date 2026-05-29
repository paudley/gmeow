// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

// TestPackedShardReaderSkipsTornTail confirms that a partially-written final
// record at the tail of a shard does not block readers from returning the
// preceding intact records, and that scanPackedShard reports the torn span
// for repair.
func TestPackedShardReaderSkipsTornTail(t *testing.T) {
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "test",
		SourceName:      "shard",
		ExternalID:      "id-1",
		ExternalVersion: "v1",
	}
	digest := contracts.ObjectDigest(strings.Repeat("a", 64))
	// The four packed indexes now write through Pebble; the JSONL shard format
	// remains only for reading legacy on-disk shards (verify/compact/resolve).
	// Build a legacy shard directly to exercise the still-live torn-tail
	// detect-and-repair machinery.
	path := filepath.Join(t.TempDir(), packedShardFilename)
	writeLegacyPackedShard(t, path, legacySourceIndexPayload(t, ref, digest))
	intactSize := fileSize(t, path)

	// Append a torn record: a length prefix that promises 100 bytes followed
	// by junk and no terminating newline. This is what a crashed or
	// backup-interrupted write looks like on disk.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("00000064\t{\"partial\":\"never\""); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	// scanPackedShard must report the torn span so verify --repair can truncate.
	result, err := scanPackedShard(path, func(_ []byte) error { return nil })
	if err != nil {
		t.Fatalf("scanPackedShard: %v", err)
	}
	if result.TornTailBytes == 0 {
		t.Fatal("expected torn tail to be reported")
	}
	if result.LastIntactEnd != intactSize {
		t.Fatalf("LastIntactEnd = %d, want %d", result.LastIntactEnd, intactSize)
	}

	// Repair truncates to the last intact end. Subsequent reads see no torn tail.
	if err := truncatePackedShardTornTail(ctx, path, result.LastIntactEnd); err != nil {
		t.Fatalf("truncate torn tail: %v", err)
	}
	after, err := scanPackedShard(path, func(_ []byte) error { return nil })
	if err != nil {
		t.Fatalf("scanPackedShard after repair: %v", err)
	}
	if after.TornTailBytes != 0 {
		t.Fatalf("torn tail still present after repair: %+v", after)
	}
	if after.Records != 1 {
		t.Fatalf("Records = %d, want 1", after.Records)
	}
}

// TestPackedShardReaderMidFileCorruptionIsNotMasked confirms that a bad record
// in the middle of a shard (i.e., not the last line) is surfaced as an error
// rather than silently skipped. Truncation cannot repair this kind of damage.
func TestPackedShardReaderMidFileCorruptionIsNotMasked(t *testing.T) {
	path := filepath.Join(t.TempDir(), packedShardFilename)
	intact := encodePackedShardLine([]byte(`{"a":1}`))
	junk := []byte("00000005\t}}{{}\n")
	trailing := encodePackedShardLine([]byte(`{"b":2}`))
	content := append(append(append([]byte{}, intact...), junk...), trailing...)
	if err := os.WriteFile(path, content, 0o640); err != nil {
		t.Fatal(err)
	}
	_, err := scanPackedShard(path, func(_ []byte) error { return nil })
	if err == nil {
		t.Fatal("mid-file corruption was silently ignored")
	}
}

func TestPackedShardReaderAcceptsRecordsLargerThanReadBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), packedShardFilename)
	payload := []byte(`{"blob":"` + strings.Repeat("x", 128*1024) + `"}`)
	if len(payload) >= packedRecordMaxBytes {
		t.Fatalf("test payload unexpectedly exceeds write limit: %d", len(payload))
	}
	if err := os.WriteFile(path, encodePackedShardLine(payload), 0o640); err != nil {
		t.Fatal(err)
	}
	records := 0
	result, err := scanPackedShard(path, func(found []byte) error {
		records++
		if string(found) != string(payload) {
			t.Fatalf("payload mismatch: got %d bytes want %d", len(found), len(payload))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if records != 1 || result.Records != 1 || result.TornTailBytes != 0 {
		t.Fatalf("unexpected scan result records=%d result=%+v", records, result)
	}
}

// TestPackedShardFlockReleasesOnClose confirms that closing a shard file
// releases the advisory lock, so a second writer can immediately proceed.
// The previous .locks/ directory-mutex did not have this property — it
// required a separate cleanup call.
func TestPackedShardFlockReleasesOnClose(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind: "test", SourceName: "lock", ExternalID: "id", ExternalVersion: "v",
	}
	digest := contracts.ObjectDigest(strings.Repeat("d", 64))

	// Two sequential writes against the same shard must not deadlock.
	for index := 0; index < 2; index++ {
		if err := store.writePackedSourceObjectIndex(ctx, sourceObjectIndexEntry{
			SourceObject: ref,
			ObjectDigest: digest,
			UpdatedAt:    time.Now().UTC(),
		}); err != nil {
			t.Fatalf("write %d: %v", index, err)
		}
	}

	// And there must be no surviving lock directory: the on-disk state should
	// be just the shard file under its packed dir, no .locks/ residue.
	if _, err := os.Stat(
		filepath.Join(store.root, ".locks"),
	); !errors.Is(
		err,
		os.ErrNotExist,
	) {
		t.Fatalf(
			".locks/ directory was created (stat err=%v); flock should leave no on-disk state",
			err,
		)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

// writeLegacyPackedShard writes the given payloads into a length-prefixed JSONL
// shard at path, creating parent directories. The packed indexes now write
// through Pebble, so this is the way tests construct the legacy on-disk shards
// that the read/verify/compact/resolve machinery still consumes.
func writeLegacyPackedShard(t *testing.T, path string, payloads ...[]byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	var content []byte
	for _, payload := range payloads {
		content = append(content, encodePackedShardLine(payload)...)
	}
	if err := os.WriteFile(path, content, 0o640); err != nil {
		t.Fatal(err)
	}
}

// legacySourceIndexPayload marshals a source-object index entry as it would
// appear inside a legacy packed shard.
func legacySourceIndexPayload(
	t *testing.T,
	ref contracts.SourceObjectRef,
	digest contracts.ObjectDigest,
) []byte {
	t.Helper()
	payload, err := json.Marshal(packedSourceObjectIndexEntry{
		SourceObject: ref,
		ObjectDigest: digest,
		UpdatedAt:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// writeLegacySourceIndexShard builds a single-record legacy source-index shard
// at the location the store's resolver/verify code expects for ref.
func writeLegacySourceIndexShard(
	t *testing.T,
	store *FilesystemStore,
	ref contracts.SourceObjectRef,
	digest contracts.ObjectDigest,
) {
	t.Helper()
	writeLegacyPackedShard(
		t,
		store.packedSourceIndexShardPath(sourceObjectRefKey(ref)),
		legacySourceIndexPayload(t, ref, digest),
	)
}
