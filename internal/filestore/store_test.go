// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestPutSameBytesDedupesToOneObjectDirectory(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	first, err := store.Put(ctx, PutRequest{
		Reader:    strings.NewReader("hello"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "first",
			ObservedAt: time.Now().UTC(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(ctx, PutRequest{
		Reader:    strings.NewReader("hello"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "analysis_output"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "second",
			ObservedAt: time.Now().UTC(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("duplicate bytes produced different digest: %s != %s", second, first)
	}
	if count := countObjectDirs(t, store.root); count != 1 {
		t.Fatalf("expected one object directory, got %d", count)
	}
	for _, name := range []string{blobFilename, recoveryFilename, manifestFilename} {
		if _, err := os.Stat(store.objectPath(first, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	manifest, err := store.ReadManifest(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if got := facetKinds(
		manifest.Facets,
	); strings.Join(
		got,
		",",
	) != "analysis_output,file" {
		t.Fatalf("manifest facets were not merged: %#v", got)
	}
	reader, err := store.Open(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(content, []byte("hello")) {
		t.Fatalf("unexpected content: %q", content)
	}
}

func TestLookupSourceObjectFindsExactSourceVersion(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind:      "gmail",
			SourceName:      "primary",
			ExternalID:      "message-1",
			ExternalVersion: "history-1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	found, ok, err := store.LookupSourceObject(ctx, contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || found != digest {
		t.Fatalf("expected source lookup hit for %s, got ok=%t digest=%s", digest, ok, found)
	}
	if _, ok, err := store.LookupSourceObject(ctx, contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-2",
	}); err != nil || ok {
		t.Fatalf("expected exact source version miss, ok=%t err=%v", ok, err)
	}
}

func TestLookupSourceObjectDoesNotWalkUnindexedFilestore(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	}
	_, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind:      ref.SourceKind,
			SourceName:      ref.SourceName,
			ExternalID:      ref.ExternalID,
			ExternalVersion: ref.ExternalVersion,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.sourceObjectIndexPath(ref)); err != nil {
		t.Fatal(err)
	}

	found, ok, err := store.LookupSourceObject(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if ok || found != "" {
		t.Fatalf(
			"unindexed source lookup must not walk filestore, ok=%t digest=%s",
			ok,
			found,
		)
	}
}

func TestAttachProvenanceEnablesZeroPayloadLookup(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AttachProvenance(ctx, digest, []contracts.Provenance{{
		SourceKind:      "camera",
		SourceName:      "front-door",
		ExternalID:      "segment-42",
		ExternalVersion: "encoder-run-7",
	}}); err != nil {
		t.Fatal(err)
	}
	found, ok, err := store.LookupSourceObject(ctx, contracts.SourceObjectRef{
		SourceKind:      "camera",
		SourceName:      "front-door",
		ExternalID:      "segment-42",
		ExternalVersion: "encoder-run-7",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || found != digest {
		t.Fatalf("expected attached provenance lookup hit, ok=%t digest=%s", ok, found)
	}
	if count := countObjectDirs(t, store.root); count != 1 {
		t.Fatalf("metadata-only provenance attach wrote payload object: %d", count)
	}
}

func TestSourceIngestClaimSerializesConcurrentWriters(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	}
	claim, acquired, err := store.TryAcquireSourceIngest(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("expected first claim to acquire")
	}
	if _, acquired, err := store.TryAcquireSourceIngest(ctx, ref); err != nil || acquired {
		t.Fatalf("expected second claim to be blocked, acquired=%t err=%v", acquired, err)
	}
	if err := store.ReleaseSourceIngest(ctx, contracts.SourceIngestClaim{
		SourceObject: ref,
		ClaimID:      "wrong",
	}); err == nil {
		t.Fatal("expected wrong claim release to fail")
	}
	if err := store.ReleaseSourceIngest(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := store.TryAcquireSourceIngest(
		ctx,
		ref,
	); err != nil ||
		!acquired {
		t.Fatalf("expected claim after release to acquire, acquired=%t err=%v", acquired, err)
	}
}

func TestSourceIngestClaimReclaimsExpiredLock(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1:mime_structure",
		ExternalVersion: "history-1",
	}
	claim, acquired, err := store.TryAcquireSourceIngest(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("expected first claim to acquire")
	}
	lockPath := store.sourceObjectLockPath(ref)
	expired := time.Now().Add(-(sourceIngestClaimTTL + time.Minute))
	if err := os.Chtimes(lockPath, expired, expired); err != nil {
		t.Fatal(err)
	}
	claim.AcquiredAt = expired
	encoded, err := canonicalJSON(claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockPath, encoded, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(lockPath, expired, expired); err != nil {
		t.Fatal(err)
	}

	nextClaim, acquired, err := store.TryAcquireSourceIngest(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("expected expired claim to be reclaimed")
	}
	if nextClaim.ClaimID == claim.ClaimID {
		t.Fatal("expected replacement claim")
	}
}

func TestPutRejectsObjectWithoutFacet(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())

	_, err := store.Put(
		context.Background(),
		PutRequest{Reader: strings.NewReader("hello")},
	)
	if err == nil {
		t.Fatal("expected missing facet error")
	}
	if count := countObjectDirs(t, store.root); count != 0 {
		t.Fatalf("object was written before validation failed: %d dirs", count)
	}
}

func TestPutDoesNotRepairMissingRecoverySidecarOnExistingObject(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	request := PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	}
	digest, err := store.Put(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.objectPath(digest, recoveryFilename)); err != nil {
		t.Fatal(err)
	}
	_, err = store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store.objectPath(digest, recoveryFilename)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	} else {
		t.Fatal("existing object write repaired immutable recovery sidecar")
	}
	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != VerifyStatusError {
		t.Fatalf("expected missing recovery sidecar to remain visible: %#v", report)
	}
	assertFinding(t, report, "recovery_missing")
}

func TestPutRefusesToRepairIncompleteObjectDirectory(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	request := PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	}
	digest, err := store.Put(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.objectPath(digest, blobFilename)); err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})

	if err == nil {
		t.Fatal("expected incomplete object directory error")
	}
	if !strings.Contains(err.Error(), "missing immutable blob") {
		t.Fatalf("unexpected error: %v", err)
	}
	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertFinding(t, report, "blob_read_failed")
}

func TestPutCommitsNoStagedObjectDirectories(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(store.objectDir(digest))
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "."+string(digest)+".") {
			t.Fatalf("staged object directory was left behind: %s", entry.Name())
		}
	}
}

func TestPublicMethodsRejectMalformedDigests(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	if _, err := store.Open(ctx, "abc"); err == nil {
		t.Fatal("expected Open to reject malformed digest")
	}
	if _, err := store.ReadManifest(ctx, "abc"); err == nil {
		t.Fatal("expected ReadManifest to reject malformed digest")
	}
	if _, err := store.GetStructure(ctx, "abc"); err == nil {
		t.Fatal("expected GetStructure to reject malformed digest")
	}
	err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: "abc",
		Kind:         "analysis",
	})
	if err == nil {
		t.Fatal("expected WriteAnnotation to reject malformed digest")
	}
}

func TestWriteAnnotationWritesIndependentCompressedJSON(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		Data:         map[string]any{"summary": "ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var annotation contracts.Annotation
	if err := store.readCompressedJSON(
		store.objectPath(digest, "analysis.json.zst"),
		&annotation,
	); err != nil {
		t.Fatal(err)
	}
	if annotation.Data["summary"] != "ok" {
		t.Fatalf("annotation was not round-tripped: %#v", annotation)
	}
	if _, err := store.ReadManifest(ctx, digest); err != nil {
		t.Fatalf("manifest should remain readable after annotation write: %v", err)
	}
}

func TestWriteOverlaysUpdatesManifestAndAnnotation(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteOverlays(ctx, digest, map[string]any{
		"category": "review",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteOverlays(ctx, digest, map[string]any{
		"visible": true,
	}); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Overlays["category"] != "review" || manifest.Overlays["visible"] != true {
		t.Fatalf("overlays were not merged into manifest: %#v", manifest.Overlays)
	}
	var overlay contracts.Annotation
	if err := store.readCompressedJSON(
		store.objectPath(digest, "overlays.json.zst"),
		&overlay,
	); err != nil {
		t.Fatal(err)
	}
	if overlay.Kind != "overlays" || overlay.Data["category"] != "review" {
		t.Fatalf("overlay annotation was not written: %#v", overlay)
	}
}

func TestWriteAnalysisAnnotationPreservesPerAnalyzerOutputs(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		AnalyzerName: "summary",
		AnalyzerVer:  "1",
		Kind:         "analysis",
		Data:         map[string]any{"summary": "ok", "score": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		AnalyzerName: "entities",
		AnalyzerVer:  "2",
		Kind:         "analysis",
		Data:         map[string]any{"entities": []any{"alice"}, "score": float64(2)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		AnalyzerName: "summary",
		AnalyzerVer:  "2",
		Kind:         "analysis",
		Data:         map[string]any{"summary": "new", "keywords": []any{"apollo"}},
	}); err != nil {
		t.Fatal(err)
	}
	objects := []ProjectionObject{}
	if err := store.WalkProjection(ctx, func(object ProjectionObject) error {
		objects = append(objects, object)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("expected one projected object, got %d", len(objects))
	}
	annotations := objects[0].Annotations
	if len(annotations) != 2 {
		t.Fatalf("expected two analysis annotations, got %#v", annotations)
	}
	if annotations[0].AnalyzerName != "entities" ||
		annotations[0].AnalyzerVer != "2" ||
		len(annotations[0].Data["entities"].([]any)) != 1 ||
		annotations[0].Data["score"] != float64(2) {
		t.Fatalf("entities annotation was not preserved: %#v", annotations[0])
	}
	if annotations[1].AnalyzerName != "summary" ||
		annotations[1].AnalyzerVer != "2" ||
		annotations[1].Data["summary"] != "new" ||
		annotations[1].Data["score"] != float64(1) ||
		len(annotations[1].Data["keywords"].([]any)) != 1 {
		t.Fatalf("summary annotation was not merged independently: %#v", annotations[1])
	}
	var annotation contracts.Annotation
	if err := store.readCompressedJSON(
		store.objectPath(digest, "analysis.json.zst"),
		&annotation,
	); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	} else {
		t.Fatalf("named analysis annotations should not share legacy path: %#v", annotation)
	}
}

func TestHasAnalysisAnnotationRequiresMatchingAnalyzerVersion(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("annotated"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	found, err := store.HasAnalysisAnnotation(ctx, digest, "summary.extractive", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("unexpected annotation before write")
	}

	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "summary.extractive",
		AnalyzerVer:  "v1",
		Data:         map[string]any{"status": "complete"},
	}); err != nil {
		t.Fatal(err)
	}

	found, err = store.HasAnalysisAnnotation(ctx, digest, "summary.extractive", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected matching annotation to be found")
	}

	found, err = store.HasAnalysisAnnotation(ctx, digest, "summary.extractive", "v2")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("stale analyzer version must not be treated as complete")
	}

	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "summary.extractive",
		AnalyzerVer:  "v3",
		Data:         map[string]any{"status": "failed"},
	}); err != nil {
		t.Fatal(err)
	}
	found, err = store.HasAnalysisAnnotation(ctx, digest, "summary.extractive", "v3")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("failed annotation must not be treated as complete")
	}
}

func TestAnalysisAnnotationRefreshesCompoundParent(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	body, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("body text"),
		Facets: []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.PutCompound(ctx, CompoundPutRequest{
		ObjectID: "mail_message:<refresh@example.test>",
		Facets:   []contracts.Facet{{Kind: "mail_message"}},
		Parts: []contracts.CompoundPart{{
			Digest: body,
			Role:   "email_body",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.ReadManifest(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: body,
		Kind:         "analysis",
		AnalyzerName: "summary",
		AnalyzerVer:  "1",
		Data:         map[string]any{"summary": "body summary"},
	}); err != nil {
		t.Fatal(err)
	}
	after, err := store.ReadManifest(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	if !after.UpdatedAt.After(before.UpdatedAt) {
		t.Fatalf(
			"parent manifest was not refreshed: before=%s after=%s",
			before.UpdatedAt,
			after.UpdatedAt,
		)
	}
	partAnalysis, ok := after.Analysis["part_analysis"].(map[string]any)
	if !ok || partAnalysis[string(body)] == nil {
		t.Fatalf("parent analysis did not reference refreshed part: %#v", after.Analysis)
	}
}

func TestWalkProjectionReadsAnnotationsAndIgnoresRecovery(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		Data:         map[string]any{"summary": "hello world"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		store.objectPath(digest, recoveryFilename),
		[]byte("not json"),
		0o640,
	); err != nil {
		t.Fatal(err)
	}
	objects := []ProjectionObject{}
	if err := store.WalkProjection(ctx, func(object ProjectionObject) error {
		objects = append(objects, object)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("expected one projected object, got %d", len(objects))
	}
	if len(objects[0].Findings) != 0 {
		t.Fatalf(
			"projection should ignore corrupt recovery sidecar: %#v",
			objects[0].Findings,
		)
	}
	if objects[0].Manifest.ObjectDigest != digest {
		t.Fatalf("projection did not read manifest: %#v", objects[0].Manifest)
	}
	if len(objects[0].Annotations) != 1 || objects[0].Annotations[0].Kind != "analysis" {
		t.Fatalf("projection did not read annotations: %#v", objects[0].Annotations)
	}
}

func TestWalkChangedProjectionFiltersByManifestAndAnnotationTimes(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	older, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("older"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().UTC()
	time.Sleep(time.Millisecond)
	newer, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("newer"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: older,
		Kind:         "analysis",
		AnalyzerName: "summary",
		Data:         map[string]any{"summary": "changed after cutoff"},
	}); err != nil {
		t.Fatal(err)
	}
	changed := map[contracts.ObjectDigest]bool{}
	if err := store.WalkChangedProjection(
		ctx,
		cutoff,
		func(object ProjectionObject) error {
			changed[object.Digest] = true
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if !changed[older] || !changed[newer] {
		t.Fatalf("expected changed manifest and annotation objects, got %#v", changed)
	}
}

func TestWalkSourceCursorsReadsSourceState(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	if err := store.WriteSourceCursor(ctx, contracts.SourceCursor{
		SourceKind: "gmail",
		SourceName: "primary",
		Cursor:     map[string]any{"history_id": "42"},
	}); err != nil {
		t.Fatal(err)
	}
	cursors := []contracts.SourceCursor{}
	if err := store.WalkSourceCursors(ctx, func(cursor contracts.SourceCursor) error {
		cursors = append(cursors, cursor)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(cursors) != 1 ||
		cursors[0].SourceKind != "gmail" ||
		cursors[0].SourceName != "primary" ||
		cursors[0].Cursor["history_id"] != "42" {
		t.Fatalf("unexpected source cursors: %#v", cursors)
	}

	cursor, found, err := store.ReadSourceCursor(ctx, contracts.SourceCursorRef{
		SourceKind: "gmail",
		SourceName: "primary",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found || cursor.Cursor["history_id"] != "42" {
		t.Fatalf("expected direct source cursor read, found=%t cursor=%#v", found, cursor)
	}
}

func TestWriteAnnotationRejectsMissingObject(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	err := store.WriteAnnotation(context.Background(), contracts.Annotation{
		ObjectDigest: contracts.ObjectDigest(strings.Repeat("a", 64)),
		Kind:         "analysis",
		Data:         map[string]any{"summary": "orphan"},
	})
	if err == nil {
		t.Fatal("expected missing target object error")
	}
	if count := countObjectDirs(t, store.root); count != 0 {
		t.Fatalf("annotation created an orphan object directory: %d dirs", count)
	}
}

func TestWriteAnnotationRejectsReservedKinds(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"manifest", "recovery", "blob"} {
		t.Run(kind, func(t *testing.T) {
			err := store.WriteAnnotation(ctx, contracts.Annotation{
				ObjectDigest: digest,
				Kind:         kind,
				Data:         map[string]any{"bad": true},
			})
			if err == nil {
				t.Fatalf("expected reserved annotation kind %q to fail", kind)
			}
			if _, err := store.ReadManifest(ctx, digest); err != nil {
				t.Fatalf("reserved annotation kind corrupted manifest: %v", err)
			}
		})
	}
}

func TestVerifyReportsCompressedRecoveryHashMismatch(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var recovery recoverySidecar
	recoveryPath := store.objectPath(digest, recoveryFilename)
	if err := readJSON(recoveryPath, &recovery); err != nil {
		t.Fatal(err)
	}
	recovery.CompressedBlake3 = strings.Repeat("0", 64)
	recovery.CompressedSHA256 = strings.Repeat("1", 64)
	recovery.CompressedSize++
	if err := atomicWriteJSON(recoveryPath, recovery); err != nil {
		t.Fatal(err)
	}
	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != VerifyStatusError {
		t.Fatalf("expected recovery mismatch report: %#v", report)
	}
	assertFinding(t, report, "recovery_compressed_hash_mismatch")
	assertFinding(t, report, "recovery_compressed_size_mismatch")
}

func TestVerifyReportsInterruptedAnnotationWrites(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		store.objectPath(digest, "analysis.summary.json.zst"),
		[]byte("not zstd"),
		0o640,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		store.objectPath(digest, ".analysis.summary.json.zst.interrupted"),
		[]byte("partial"),
		0o640,
	); err != nil {
		t.Fatal(err)
	}

	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if report.Status != VerifyStatusError {
		t.Fatalf("expected interrupted annotation write report: %#v", report)
	}
	assertFinding(t, report, "annotation_read_failed")
	assertFinding(t, report, "staged_write_leftover")
}

func TestVerifyReportsObjectDirectoryUnderWrongPrefix(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrongDir := filepath.Join(store.root, "objects", "blake3", "ff", "ff", string(digest))
	if err := os.MkdirAll(filepath.Dir(wrongDir), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(store.objectDir(digest), wrongDir); err != nil {
		t.Fatal(err)
	}
	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != VerifyStatusError {
		t.Fatalf("expected wrong prefix report: %#v", report)
	}
	assertFinding(t, report, "object_path_mismatch")
}

func TestCompoundStableIdentityAndStructure(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	body, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("body text"),
		Facets: []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("attachment"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := CompoundPutRequest{
		ObjectID: "mail_message:<message@example.test>",
		Facets:   []contracts.Facet{{Kind: "mail_message"}, {Kind: "container"}},
		Parts: []contracts.CompoundPart{
			{Digest: body, Role: "email_body", Order: 1, Required: true},
			{Digest: attachment, Role: "attachment", Order: 2},
		},
	}
	first, err := store.PutCompound(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.PutCompound(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("same compound object ID produced different digest: %s != %s", second, first)
	}
	structure, err := store.GetStructure(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if len(structure.PartsByRole["email_body"]) != 1 ||
		structure.PartsByRole["email_body"][0].Digest != body {
		t.Fatalf("missing body structure: %#v", structure.PartsByRole)
	}
	if len(structure.PartsByRole["attachment"]) != 1 ||
		structure.PartsByRole["attachment"][0].Digest != attachment {
		t.Fatalf("missing attachment structure: %#v", structure.PartsByRole)
	}
	reader, err := store.Open(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	envelope, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		string(envelope),
		`"object_id":"mail_message:<message@example.test>"`,
	) {
		t.Fatalf("compound envelope was not stored as blob: %s", envelope)
	}
}

func TestCompoundMergeReplacesSameRoleAndOrderPart(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	oldMetadata, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader(`{"version":"old"}`),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	newMetadata, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader(`{"version":"new"}`),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("body text"),
		Facets: []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	parent, err := store.PutCompound(ctx, CompoundPutRequest{
		ObjectID: "mail_message:<refresh@example.test>",
		Facets:   []contracts.Facet{{Kind: "mail_message"}, {Kind: "container"}},
		Parts: []contracts.CompoundPart{
			{Digest: body, Role: "email_body", Order: 1},
			{Digest: oldMetadata, Role: "gmail_data", Order: 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.PutCompound(ctx, CompoundPutRequest{
		ObjectID: "mail_message:<refresh@example.test>",
		Facets:   []contracts.Facet{{Kind: "mail_message"}, {Kind: "container"}},
		Parts: []contracts.CompoundPart{
			{Digest: body, Role: "email_body", Order: 1},
			{Digest: newMetadata, Role: "gmail_data", Order: 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	structure, err := store.GetStructure(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	metadataParts := structure.PartsByRole["gmail_data"]
	if len(metadataParts) != 1 {
		t.Fatalf("gmail_data parts = %#v, want one current part", metadataParts)
	}
	if metadataParts[0].Digest != newMetadata {
		t.Fatalf("gmail_data digest = %s, want %s", metadataParts[0].Digest, newMetadata)
	}

	manifest, err := store.ReadManifest(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, relationship := range manifest.Relationships {
		if relationship.To == oldMetadata || relationship.From == oldMetadata {
			t.Fatalf("stale relationship to replaced part remained: %#v", relationship)
		}
	}
}

func TestCompoundMergePreservesBlobAndRecoverySidecar(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	firstPart, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("first"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	secondPart, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("second"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	compound, err := store.PutCompound(ctx, CompoundPutRequest{
		ObjectID: "container:merge",
		Facets:   []contracts.Facet{{Kind: "container"}},
		Parts: []contracts.CompoundPart{{
			Digest: firstPart,
			Role:   "first",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	originalBlob, err := os.ReadFile(store.objectPath(compound, blobFilename))
	if err != nil {
		t.Fatal(err)
	}
	originalRecovery, err := os.ReadFile(store.objectPath(compound, recoveryFilename))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.PutCompound(ctx, CompoundPutRequest{
		ObjectID: "container:merge",
		Facets:   []contracts.Facet{{Kind: "container"}},
		Parts: []contracts.CompoundPart{{
			Digest: secondPart,
			Role:   "second",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	currentBlob, err := os.ReadFile(store.objectPath(compound, blobFilename))
	if err != nil {
		t.Fatal(err)
	}
	currentRecovery, err := os.ReadFile(store.objectPath(compound, recoveryFilename))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(currentBlob, originalBlob) {
		t.Fatal("compound merge rewrote immutable blob")
	}
	if !bytes.Equal(currentRecovery, originalRecovery) {
		t.Fatal("compound merge rewrote immutable recovery sidecar")
	}
	reader, err := store.Open(ctx, compound)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	envelope, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(envelope), string(firstPart)) ||
		strings.Contains(string(envelope), string(secondPart)) {
		t.Fatalf("compound blob should remain the original envelope: %s", envelope)
	}
	structure, err := store.GetStructure(ctx, compound)
	if err != nil {
		t.Fatal(err)
	}
	if len(structure.PartsByRole["first"]) != 1 ||
		len(structure.PartsByRole["second"]) != 1 {
		t.Fatalf("compound manifest did not include merged parts: %#v", structure.PartsByRole)
	}
	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != VerifyStatusOK {
		t.Fatalf("merged compound should verify cleanly: %#v", report)
	}
}

func TestCompoundWithoutStableObjectIDFailsBeforeWriting(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())

	_, err := store.PutCompound(context.Background(), CompoundPutRequest{
		Facets: []contracts.Facet{{Kind: "container"}},
	})
	if err == nil {
		t.Fatal("expected missing object ID error")
	}
	if count := countObjectDirs(t, store.root); count != 0 {
		t.Fatalf("compound was written before validation failed: %d dirs", count)
	}
}

func TestCompoundRejectsInvalidPartsBeforeWriting(t *testing.T) {
	cases := map[string]contracts.CompoundPart{
		"empty digest": {
			Digest: "",
			Role:   "attachment",
		},
		"short digest": {
			Digest: "abc123",
			Role:   "attachment",
		},
		"non-hex digest": {
			Digest: contracts.ObjectDigest(strings.Repeat("z", 64)),
			Role:   "attachment",
		},
		"empty role": {
			Digest: contracts.ObjectDigest(strings.Repeat("a", 64)),
			Role:   "",
		},
	}
	for name, part := range cases {
		t.Run(name, func(t *testing.T) {
			store := NewFilesystemStore(t.TempDir())

			_, err := store.PutCompound(context.Background(), CompoundPutRequest{
				ObjectID: "container:test",
				Facets:   []contracts.Facet{{Kind: "container"}},
				Parts:    []contracts.CompoundPart{part},
			})
			if err == nil {
				t.Fatal("expected invalid compound part error")
			}
			if count := countObjectDirs(t, store.root); count != 0 {
				t.Fatalf("compound was written before validation failed: %d dirs", count)
			}
		})
	}
}

func TestVerifyReportsCorruptBytesMissingRecoveryAndDanglingPart(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	compound, err := store.PutCompound(ctx, CompoundPutRequest{
		ObjectID: "container:test",
		Facets:   []contracts.Facet{{Kind: "container"}},
		Parts: []contracts.CompoundPart{{
			Digest: contracts.ObjectDigest(strings.Repeat("a", 64)),
			Role:   "attachment",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.objectPath(compound, recoveryFilename)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		store.objectPath(digest, blobFilename),
		[]byte("not zstd"),
		0o640,
	); err != nil {
		t.Fatal(err)
	}
	healthy, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("healthy"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != VerifyStatusError {
		t.Fatalf("expected error report: %#v", report)
	}
	assertFinding(t, report, "blob_read_failed")
	assertFinding(t, report, "recovery_missing")
	assertFinding(t, report, "compound_dangling_part")
	if _, err := store.Open(ctx, digest); err == nil {
		t.Fatal("expected normal read to fail on corrupt blob")
	}
	reader, err := store.Open(ctx, healthy)
	if err != nil {
		t.Fatalf("corrupt object poisoned unrelated read: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func countObjectDirs(t *testing.T, root string) int {
	t.Helper()
	count := 0
	base := filepath.Join(root, "objects", "blake3")
	err := filepath.WalkDir(base, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && looksLikeDigest(entry.Name()) {
			count++
			return filepath.SkipDir
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func assertFinding(t *testing.T, report VerifyReport, code string) {
	t.Helper()
	for _, finding := range report.Findings {
		if finding.Code == code {
			return
		}
	}
	t.Fatalf("missing finding %q in %#v", code, report.Findings)
}
