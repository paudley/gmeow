// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"encoding/json"
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
	if count := countObjects(t, store); count != 1 {
		t.Fatalf("expected one object, got %d", count)
	}
	if ok, err := store.metaHas(manifestKey(first)); err != nil || !ok {
		t.Fatalf("expected manifest in metadata store: ok=%t err=%v", ok, err)
	}
	if ok, err := store.hasRecipe(first); err != nil || !ok {
		t.Fatalf("expected content recipe: ok=%t err=%v", ok, err)
	}
	if recovery, err := store.ExportRecoveryJSON(
		ctx,
		first,
	); err != nil ||
		len(recovery) == 0 {
		t.Fatalf("missing packed recovery: len=%d err=%v", len(recovery), err)
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

func TestLookupSourceObjectFindsSourceAliasAcrossVersions(t *testing.T) {
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
	if _, err := os.Stat(store.sourceObjectIndexPath(contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	})); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new source indexes must not write v1 files: %v", err)
	}
	found, ok, err = store.LookupSourceObject(ctx, contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || found != digest {
		t.Fatalf(
			"expected source alias lookup hit for %s, got ok=%t digest=%s",
			digest,
			ok,
			found,
		)
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
	// The source object/alias indexes live in Pebble now; drop their keys so the
	// lookup has no index entry to find.
	if err := store.metaDelete("si/" + sourceObjectRefKey(ref)); err != nil {
		t.Fatal(err)
	}
	if err := store.metaDelete("sa/" + sourceAliasKey(ref)); err != nil {
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
	if _, err := store.AttachProvenance(ctx, digest, []contracts.Provenance{{
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
	if count := countObjects(t, store); count != 1 {
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
	// Age the claim past its TTL in the metadata store; the next acquire
	// reclaims it.
	claim.AcquiredAt = time.Now().Add(-(sourceIngestClaimTTL + time.Minute)).UTC()
	if err := store.metaPut(sourceLockKey(ref), claim); err != nil {
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

func TestSourceIngestClaimZeroTimestampTreatedAsLive(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1:body",
		ExternalVersion: "history-1",
	}
	claim, acquired, err := store.TryAcquireSourceIngest(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("expected first claim to acquire")
	}
	// A claim with no AcquiredAt has unknown age; it is treated conservatively as
	// live and must not be reclaimed out from under an in-flight writer.
	claim.AcquiredAt = time.Time{}
	if err := store.metaPut(sourceLockKey(ref), claim); err != nil {
		t.Fatal(err)
	}

	if _, acquired, err := store.TryAcquireSourceIngest(ctx, ref); err != nil {
		t.Fatal(err)
	} else if acquired {
		t.Fatal("expected zero-timestamp claim to be treated as live (not reclaimed)")
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
	if count := countObjects(t, store); count != 0 {
		t.Fatalf("object was written before validation failed: %d dirs", count)
	}
}

func TestPutDoesNotCreateLegacyRecoverySidecarOnExistingObject(t *testing.T) {
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
		t.Fatal("existing object write created legacy recovery sidecar")
	}
	if recovery, err := store.ExportRecoveryJSON(
		ctx,
		digest,
	); err != nil ||
		len(recovery) == 0 {
		t.Fatalf("missing packed recovery: len=%d err=%v", len(recovery), err)
	}
}

func TestPutSelfHealsIncompleteObject(t *testing.T) {
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
	// Remove the content recipe, leaving a manifest in the metadata LSM without
	// retrievable content: an incomplete object.
	if err := store.metaDelete(objectRecipeKey(digest)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(ctx, digest); err == nil {
		t.Fatal("expected read to fail while content recipe is missing")
	}

	// Re-putting the same bytes self-heals the object: the missing recipe is
	// re-derived and the content becomes retrievable again, with no error and
	// no duplicate object.
	healed, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatalf("re-put should self-heal incomplete object: %v", err)
	}
	if healed != digest {
		t.Fatalf("re-put produced a different digest: %s != %s", healed, digest)
	}
	if has, err := store.hasRecipe(digest); err != nil || !has {
		t.Fatalf("content recipe was not restored: has=%t err=%v", has, err)
	}
	reader, err := store.Open(ctx, digest)
	if err != nil {
		t.Fatalf("object did not become readable after self-heal: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if count := countObjects(t, store); count != 1 {
		t.Fatalf("self-heal must not duplicate the object: %d", count)
	}
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

	// Objects carry no per-object directory at all: the manifest is in the
	// metadata LSM and content in shared chunk packs. Neither a committed nor a
	// staged object directory should exist on disk.
	if _, err := os.Stat(store.objectDir(digest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a per-object directory was created, stat err=%v", err)
	}
	if _, err := os.Stat(
		filepath.Join(store.root, "objects"),
	); !errors.Is(
		err,
		os.ErrNotExist,
	) {
		t.Fatalf("objects/ tree was created, stat err=%v", err)
	}

	// The object remains fully retrievable.
	reader, err := store.Open(ctx, digest)
	if err != nil {
		t.Fatalf("object not readable: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPutDuplicateCleansIncomingStageDirectory(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	_, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "email"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(store.root, "staging", "incoming"))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("incoming staged object directories were left behind: %#v", entries)
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
	annotation, ok, err := store.readPackedAnnotation(digest, "analysis", "")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected packed analysis annotation to be retrievable")
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
	overlay, ok, err := store.readPackedAnnotation(digest, "overlays", "")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected packed overlays annotation to be retrievable")
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

func TestNewCompoundParentIndexesUsePackedLayout(t *testing.T) {
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
		ObjectID: "mail_message:<packed-parent@example.test>",
		Facets:   []contracts.Facet{{Kind: "mail_message"}},
		Parts: []contracts.CompoundPart{{
			Digest: body,
			Role:   "email_body",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	parents, err := store.compoundParentsForChild(ctx, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 1 || parents[0].ParentDigest != parent {
		t.Fatalf("packed parent index mismatch: %#v", parents)
	}
	if _, err := os.Stat(
		store.compoundParentIndexPath(body),
	); !errors.Is(
		err,
		os.ErrNotExist,
	) {
		t.Fatalf("new compound parent indexes must not write v1 files: %v", err)
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
	if count := countObjects(t, store); count != 0 {
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

func TestVerifyReportsPackedRecoveryHashMismatch(t *testing.T) {
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
	recoveryContent, err := store.ExportRecoveryJSON(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(recoveryContent, &recovery); err != nil {
		t.Fatal(err)
	}
	recovery.UncompressedBlake3 = strings.Repeat("0", 64)
	recovery.UncompressedSHA256 = strings.Repeat("1", 64)
	if err := store.writePackedRecovery(ctx, digest, recovery); err != nil {
		t.Fatal(err)
	}
	report, err := store.Verify(ctx, VerifyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != VerifyStatusError {
		t.Fatalf("expected packed recovery mismatch report: %#v", report)
	}
	assertFinding(t, report, "recovery_uncompressed_hash_mismatch")
}

func TestVerifyReportsUnreadableExternalAnnotation(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A large annotation externalizes its payload into the content-addressed
	// chunk store; dropping that payload's recipe simulates lost content that
	// verify must surface when it resolves the object's annotations.
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "summary",
		Data: map[string]any{
			"summary": strings.Repeat("x", annotationInlineMaxBytes*2),
		},
	}); err != nil {
		t.Fatal(err)
	}

	var entry packedObjectAnnotationEntry
	ok, err := store.metaGet(annotationKey(digest, "analysis", "summary"), &entry)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || entry.ExternalDigest == "" {
		t.Fatalf("expected large annotation to externalize: ok=%t %#v", ok, entry)
	}
	if err := store.metaDelete(objectRecipeKey(entry.ExternalDigest)); err != nil {
		t.Fatal(err)
	}

	report, err := store.Verify(ctx, VerifyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != VerifyStatusError {
		t.Fatalf("expected unreadable annotation report: %#v", report)
	}
	assertFinding(t, report, "annotation_read_failed")
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
	originalBlob, err := store.readBlob(compound)
	if err != nil {
		t.Fatal(err)
	}
	originalRecovery, err := store.ExportRecoveryJSON(ctx, compound)
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
	currentBlob, err := store.readBlob(compound)
	if err != nil {
		t.Fatal(err)
	}
	currentRecovery, err := store.ExportRecoveryJSON(ctx, compound)
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
	report, err := store.Verify(ctx, VerifyRequest{})
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
	if count := countObjects(t, store); count != 0 {
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
			if count := countObjects(t, store); count != 0 {
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
	// Recovery sidecars live in Pebble now; drop the compound's key so verify
	// reports it missing.
	if err := store.metaDelete("rec/" + string(compound)); err != nil {
		t.Fatal(err)
	}
	// Drop the content recipe so reads and verify fail on unreadable content.
	if err := store.metaDelete(objectRecipeKey(digest)); err != nil {
		t.Fatal(err)
	}
	healthy, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("healthy"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := store.Verify(ctx, VerifyRequest{})
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

func TestCompactV1LayoutMigratesLegacyIndexesAndRecovery(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	}
	body, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("body text"),
		Facets: []contracts.Facet{{Kind: "email_part"}},
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
	parent, err := store.PutCompound(ctx, CompoundPutRequest{
		ObjectID: "mail_message:<compact@example.test>",
		Facets:   []contracts.Facet{{Kind: "mail_message"}},
		Parts: []contracts.CompoundPart{{
			Digest: body,
			Role:   "email_body",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	recoveryJSON, err := store.ExportRecoveryJSON(ctx, body)
	if err != nil {
		t.Fatal(err)
	}
	var recovery recoverySidecar
	if err := json.Unmarshal(recoveryJSON, &recovery); err != nil {
		t.Fatal(err)
	}
	parentEdges, err := store.compoundParentsForChild(ctx, body)
	if err != nil {
		t.Fatal(err)
	}

	removeFileIfExists(t, store.packedSourceIndexShardPath(sourceObjectRefKey(ref)))
	removeFileIfExists(t, store.packedCompoundParentShardPath(string(body)))
	removeFileIfExists(t, store.packedRecoveryShardPath(string(body)))
	writeTestJSON(t, store.sourceObjectIndexPath(ref), sourceObjectIndexEntry{
		SourceObject: ref,
		ObjectDigest: body,
		UpdatedAt:    time.Now().UTC(),
	})
	writeTestJSON(t, store.compoundParentIndexPath(body), compoundParentIndexRecord{
		SchemaVersion: int(contracts.SchemaVersionPhase00),
		ChildDigest:   body,
		Parents:       parentEdges,
	})
	writeTestJSON(t, store.objectPath(body, recoveryFilename), recovery)

	report, err := store.CompactV1Layout(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.SourceIndexes != 1 ||
		report.CompoundParentIndexes != 1 ||
		report.RecoverySidecars != 1 ||
		report.RemovedFiles != 3 ||
		report.DryRun {
		t.Fatalf("unexpected compaction report: %#v", report)
	}
	found, ok, err := store.LookupSourceObject(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || found != body {
		t.Fatalf("compacted source index lookup mismatch: ok=%t digest=%s", ok, found)
	}
	compactedParents, err := store.compoundParentsForChild(ctx, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(compactedParents) != 1 || compactedParents[0].ParentDigest != parent {
		t.Fatalf("compacted parent index mismatch: %#v", compactedParents)
	}
	if _, err := store.ExportRecoveryJSON(ctx, body); err != nil {
		t.Fatalf("compacted recovery missing: %v", err)
	}
	for _, path := range []string{
		store.sourceObjectIndexPath(ref),
		store.compoundParentIndexPath(body),
		store.objectPath(body, recoveryFilename),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("legacy file was not removed: %s err=%v", path, err)
		}
	}
}

func TestCompactV1LayoutDryRunDoesNotMutateLegacyFiles(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	}
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("body text"),
		Facets: []contracts.Facet{{Kind: "email_part"}},
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
	removeFileIfExists(t, store.packedSourceIndexShardPath(sourceObjectRefKey(ref)))
	writeTestJSON(t, store.sourceObjectIndexPath(ref), sourceObjectIndexEntry{
		SourceObject: ref,
		ObjectDigest: digest,
		UpdatedAt:    time.Now().UTC(),
	})

	report, err := store.CompactV1Layout(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if report.SourceIndexes != 1 || report.RemovedFiles != 0 || !report.DryRun {
		t.Fatalf("unexpected dry-run report: %#v", report)
	}
	if _, err := os.Stat(store.sourceObjectIndexPath(ref)); err != nil {
		t.Fatalf("dry-run removed legacy source index: %v", err)
	}
	if _, err := os.Stat(
		store.packedSourceIndexShardPath(sourceObjectRefKey(ref)),
	); !errors.Is(
		err,
		os.ErrNotExist,
	) {
		t.Fatalf("dry-run wrote packed source shard: %v", err)
	}
}

func TestCleanupSourceLocksRemovesOnlyExpiredLocks(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	expiredRef := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "expired",
		ExternalVersion: "history-1",
	}
	freshRef := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "fresh",
		ExternalVersion: "history-1",
	}
	expiredClaim, ok, err := store.TryAcquireSourceIngest(ctx, expiredRef)
	if err != nil || !ok {
		t.Fatalf("acquire expired lock: ok=%t err=%v", ok, err)
	}
	if _, ok, err := store.TryAcquireSourceIngest(ctx, freshRef); err != nil || !ok {
		t.Fatalf("acquire fresh lock: ok=%t err=%v", ok, err)
	}
	// Age only the expired claim past its TTL in the metadata store.
	expiredClaim.AcquiredAt = time.Now().Add(-2 * sourceIngestClaimTTL).UTC()
	if err := store.metaPut(sourceLockKey(expiredRef), expiredClaim); err != nil {
		t.Fatal(err)
	}

	report, err := store.CleanupSourceLocks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.RemovedFiles != 1 {
		t.Fatalf("expected one expired lock removed: %#v", report)
	}
	if has, err := store.metaHas(sourceLockKey(expiredRef)); err != nil || has {
		t.Fatalf("expired lock still present: has=%t err=%v", has, err)
	}
	if has, err := store.metaHas(sourceLockKey(freshRef)); err != nil || !has {
		t.Fatalf("fresh lock was removed: has=%t err=%v", has, err)
	}
}

func TestStorageBreakdownIncludesObjectAndPackedMetadataFiles(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	}
	digest, err := store.Put(ctx, PutRequest{
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

	report, err := store.StorageBreakdown(ctx, StorageBreakdownRequest{
		Digest:         digest,
		RecursiveParts: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.RootDigest != digest || report.FileCount != len(report.Files) {
		t.Fatalf("unexpected report identity/counts: %#v", report)
	}
	if report.TotalAllocatedBytes <= 0 || report.TotalLogicalBytes <= 0 {
		t.Fatalf("expected positive totals: %#v", report)
	}
	// The source/alias/recovery indexes are now shared Pebble metadata rather
	// than per-object sidecar files, so a per-object breakdown only surfaces the
	// object's own directory files.
	assertStorageRole(t, report, "manifest")
}

func TestStorageBreakdownRecursesCompoundPartsByRequest(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	body, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("body text"),
		Facets: []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	compound, err := store.PutCompound(ctx, CompoundPutRequest{
		ObjectID: "mail_message:<storage@example.test>",
		Facets:   []contracts.Facet{{Kind: contracts.MailMessageFacetKind}},
		Parts: []contracts.CompoundPart{{
			Digest: body,
			Role:   "email_body",
			Order:  1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	recursive, err := store.StorageBreakdown(ctx, StorageBreakdownRequest{
		Digest:         compound,
		RecursiveParts: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ownOnly, err := store.StorageBreakdown(ctx, StorageBreakdownRequest{
		Digest:         compound,
		RecursiveParts: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if recursive.ReferencedObjectCount != 2 {
		t.Fatalf("expected compound plus body object: %#v", recursive)
	}
	if ownOnly.ReferencedObjectCount != 1 {
		t.Fatalf("expected only compound object: %#v", ownOnly)
	}
	if recursive.FileCount <= ownOnly.FileCount {
		t.Fatalf(
			"recursive report did not add files: recursive=%d own=%d",
			recursive.FileCount,
			ownOnly.FileCount,
		)
	}
	foundPart := false
	for _, file := range recursive.Files {
		if file.ObjectDigest == body && file.RecursivePart &&
			file.CompoundRole == "email_body" {
			foundPart = true
			break
		}
	}
	if !foundPart {
		t.Fatalf("missing recursive part file row: %#v", recursive.Files)
	}
}

func TestResolvePathIdentifiesPackedShard(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	}
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: contracts.MailMessageFacetKind}},
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

	// Source indexes write through Pebble now; ResolvePath still inspects legacy
	// on-disk shards, so build one to resolve.
	writeLegacySourceIndexShard(t, store, ref, digest)
	shardReport, err := store.ResolvePath(ctx, PathResolveRequest{
		Path:         store.packedSourceIndexShardPath(sourceObjectRefKey(ref)),
		RecordsLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if shardReport.Kind != "packed_source_index" ||
		shardReport.RecordCount != 1 ||
		len(shardReport.Records) != 1 ||
		shardReport.Records[0].ObjectDigest != digest {
		t.Fatalf("unexpected source shard report: %#v", shardReport)
	}
}

func TestResolvePathIdentifiesSourceCursor(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	cursor := contracts.SourceCursor{
		SourceKind: "gmail",
		SourceName: "primary",
		Cursor:     map[string]any{"history_id": "1"},
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.WriteSourceCursor(ctx, cursor); err != nil {
		t.Fatal(err)
	}
	cursorReport, err := store.ResolvePath(ctx, PathResolveRequest{
		Path: store.sourceCursorPath(cursor),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cursorReport.Kind != "source_cursor" ||
		cursorReport.SourceCursor == nil ||
		cursorReport.SourceCursor.SourceName != cursor.SourceName {
		t.Fatalf("unexpected cursor report: %#v", cursorReport)
	}
}

func TestResolvePathRejectsOutsideAndSymlinkEscape(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolvePath(ctx, PathResolveRequest{Path: outside}); err == nil {
		t.Fatal("expected outside path rejection")
	}
	link := filepath.Join(store.root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolvePath(ctx, PathResolveRequest{Path: "escape"}); err == nil {
		t.Fatal("expected symlink escape rejection")
	}
}

// countObjects returns the number of stored objects by counting manifest keys
// in the metadata LSM. Objects no longer have per-object directories, so the
// manifest key space is the authoritative object inventory.
func countObjects(t *testing.T, store *FilesystemStore) int {
	t.Helper()
	count := 0
	if err := store.metaIterPrefix("m/", func(_ string, _ []byte) error {
		count++

		return nil
	}); err != nil {
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

func assertStorageRole(t *testing.T, report StorageBreakdownReport, role string) {
	t.Helper()
	for _, file := range report.Files {
		if file.Role == role {
			return
		}
	}
	t.Fatalf("missing storage role %q in %#v", role, report.Files)
}

func writeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := canonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o640); err != nil {
		t.Fatal(err)
	}
}

func removeFileIfExists(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}
