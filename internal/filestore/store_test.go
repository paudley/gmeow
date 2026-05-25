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

	"blackat.ca/gmeow/internal/contracts"
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

func TestPutRepairsMissingRecoverySidecarOnRetry(t *testing.T) {
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
		t.Fatalf("recovery sidecar was not repaired: %v", err)
	}
	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != VerifyStatusOK {
		t.Fatalf("expected repaired store to verify cleanly: %#v", report)
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

func TestCompoundMergeRewritesEnvelopeBlob(t *testing.T) {
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
	currentRecovery, err := os.ReadFile(store.objectPath(compound, recoveryFilename))
	if err != nil {
		t.Fatal(err)
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
		!strings.Contains(string(envelope), string(secondPart)) {
		t.Fatalf("compound blob did not include merged parts: %s", envelope)
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
	if report.Status != VerifyStatusError {
		t.Fatalf("merged compound should report immutable recovery mismatch: %#v", report)
	}
	assertFinding(t, report, "recovery_uncompressed_hash_mismatch")
	assertFinding(t, report, "recovery_compressed_hash_mismatch")
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
