// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestQuietSnapshotVerifiesClean(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	var digests []contracts.ObjectDigest
	for i := range 5 {
		digest, err := store.Put(ctx, PutRequest{
			Reader: strings.NewReader(strings.Repeat("x", i+1)),
			Facets: []contracts.Facet{{Kind: "file"}},
			Provenance: []contracts.Provenance{{
				SourceKind: "test", SourceName: "backup",
				ExternalID: "msg-" + strings.Repeat("a", i+1), ExternalVersion: "v1",
			}},
		})
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		digests = append(digests, digest)
	}

	copyDir := filepath.Join(t.TempDir(), "snapshot")
	cpTree(t, store.root, copyDir)

	restored := NewFilesystemStore(copyDir)
	report, err := restored.Verify(ctx, VerifyRequest{})
	if err != nil {
		t.Fatalf("verify snapshot: %v", err)
	}
	if report.Status != VerifyStatusOK {
		t.Fatalf("snapshot verify not ok: %+v", report)
	}

	for _, digest := range digests {
		rc, err := restored.Open(ctx, digest)
		if err != nil {
			t.Fatalf("open %s on snapshot: %v", digest, err)
		}
		rc.Close()
	}
}

func TestTornTailRepair(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind: "test", SourceName: "torn", ExternalID: "id-1", ExternalVersion: "v1",
	}
	digest := contracts.ObjectDigest(strings.Repeat("b", 64))
	if err := store.writePackedSourceObjectIndex(ctx, sourceObjectIndexEntry{
		SourceObject: ref,
		ObjectDigest: digest,
		UpdatedAt:    time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.writePackedSourceObjectIndex(ctx, sourceObjectIndexEntry{
		SourceObject: contracts.SourceObjectRef{
			SourceKind: "test", SourceName: "torn", ExternalID: "id-2", ExternalVersion: "v1",
		},
		ObjectDigest: digest,
		UpdatedAt:    time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	shardPath := store.packedSourceIndexShardPath(sourceObjectRefKey(ref))
	intactSize := fileSize(t, shardPath)

	file, err := os.OpenFile(shardPath, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	file.WriteString("00000064\t{\"partial\":\"torn\"")
	file.Close()

	report, err := store.Verify(ctx, VerifyRequest{Repair: true})
	if err != nil {
		t.Fatalf("verify --repair: %v", err)
	}
	if report.Repaired == 0 {
		t.Fatal("expected torn tail to be repaired")
	}

	afterSize := fileSize(t, shardPath)
	if afterSize != intactSize {
		t.Fatalf("shard size after repair = %d, want %d", afterSize, intactSize)
	}

	cleanReport, err := store.Verify(ctx, VerifyRequest{})
	if err != nil {
		t.Fatalf("verify after repair: %v", err)
	}
	hasTornTail := false
	for _, f := range cleanReport.Findings {
		if strings.Contains(f.Code, "torn_tail") {
			hasTornTail = true
		}
	}
	if hasTornTail {
		t.Fatal("torn tail still present after repair")
	}
}

func TestOrphanBlobRepair(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("orphan-content"),
		Facets: []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "test", SourceName: "orphan",
			ExternalID: "msg-orphan", ExternalVersion: "v1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	ref := contracts.SourceObjectRef{
		SourceKind: "test", SourceName: "orphan",
		ExternalID: "msg-orphan", ExternalVersion: "v1",
	}
	shardPath := store.packedSourceIndexShardPath(sourceObjectRefKey(ref))
	if err := os.Remove(shardPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	aliasShardPath := store.packedSourceAliasIndexShardPath(sourceAliasKey(ref))
	if err := os.Remove(aliasShardPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	_, found, err := store.LookupSourceObject(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected source lookup miss after removing index shard")
	}

	report, err := store.Verify(ctx, VerifyRequest{Repair: true})
	if err != nil {
		t.Fatalf("verify --repair: %v", err)
	}
	if report.Repaired == 0 {
		t.Fatal("expected orphan source index to be repaired")
	}

	_, found, err = store.LookupSourceObject(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("source lookup still misses after repair")
	}

	rc, err := store.Open(ctx, digest)
	if err != nil {
		t.Fatalf("open after repair: %v", err)
	}
	rc.Close()
}

func TestStaleStagingReap(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	staleDir := filepath.Join(store.root, "staging", "objects", "stale-stage-dir")
	if err := os.MkdirAll(staleDir, 0o750); err != nil {
		t.Fatal(err)
	}
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	os.Chtimes(staleDir, twoHoursAgo, twoHoursAgo)

	report, err := store.Verify(ctx, VerifyRequest{Repair: true})
	if err != nil {
		t.Fatalf("verify --repair: %v", err)
	}
	if report.Repaired == 0 {
		t.Fatal("expected stale staging dir to be reaped")
	}
	if _, err := os.Stat(staleDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale staging dir still exists after repair")
	}
}

func TestCompactDeduplicatesShards(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	ref := contracts.SourceObjectRef{
		SourceKind: "test", SourceName: "dup", ExternalID: "id-dup", ExternalVersion: "v1",
	}
	for range 5 {
		if err := store.writePackedSourceObjectIndex(ctx, sourceObjectIndexEntry{
			SourceObject: ref,
			ObjectDigest: contracts.ObjectDigest(strings.Repeat("d", 64)),
			UpdatedAt:    time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	shardPath := store.packedSourceIndexShardPath(sourceObjectRefKey(ref))
	beforeSize := fileSize(t, shardPath)

	report, err := store.Compact(ctx, false)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if report.DuplicatesRemoved != 4 {
		t.Fatalf("DuplicatesRemoved = %d, want 4", report.DuplicatesRemoved)
	}
	if report.ShardsCompacted != 1 {
		t.Fatalf("ShardsCompacted = %d, want 1", report.ShardsCompacted)
	}

	afterSize := fileSize(t, shardPath)
	if afterSize >= beforeSize {
		t.Fatalf("shard did not shrink: before=%d after=%d", beforeSize, afterSize)
	}

	entry, found, err := store.readPackedSourceObjectIndex(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("record lost after compaction")
	}
	if entry.ObjectDigest != contracts.ObjectDigest(strings.Repeat("d", 64)) {
		t.Fatalf("wrong digest after compaction: %s", entry.ObjectDigest)
	}
}

func cpTree(t *testing.T, src, dst string) {
	t.Helper()
	cmd := exec.Command("cp", "-a", src, dst)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cp -a %s %s: %v\n%s", src, dst, err, out)
	}
}
