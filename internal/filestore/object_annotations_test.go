// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestPackedAnnotationsKeepObjectDirSmall(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Several small annotations from different analyzers plus scheduler/overlays.
	for _, analyzer := range []string{"summary", "ner", "categories"} {
		if err := store.WriteAnnotation(ctx, contracts.Annotation{
			ObjectDigest: digest,
			Kind:         "analysis",
			AnalyzerName: analyzer,
			AnalyzerVer:  "1",
			Data:         map[string]any{"ok": true},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "scheduler",
		Data:         map[string]any{"queued": true},
	}); err != nil {
		t.Fatal(err)
	}

	// Small annotations inline into the metadata LSM and content is chunked, so
	// no per-object directory is ever created.
	if _, err := os.Stat(store.objectDir(digest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no per-object directory, stat err=%v", err)
	}

	annotations, err := store.readPackedAnnotations(digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(annotations) != 4 {
		t.Fatalf("expected 4 packed annotations, got %d: %#v", len(annotations), annotations)
	}
}

func TestPackedAnnotationLargePayloadExternalizes(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest, err := store.Put(ctx, PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	payload := strings.Repeat("x", annotationInlineMaxBytes*2)
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "summary",
		AnalyzerVer:  "1",
		Data:         map[string]any{"summary": payload},
	}); err != nil {
		t.Fatal(err)
	}

	// The payload externalizes into the content-addressed chunk store: the
	// record carries only the payload digest, and the bytes are retrievable
	// from the chunk store — never a per-object side file.
	var entry packedObjectAnnotationEntry
	found, err := store.metaGet(annotationKey(digest, "analysis", "summary"), &entry)
	if err != nil {
		t.Fatal(err)
	}
	if !found || entry.Inline != nil || entry.ExternalDigest == "" {
		t.Fatalf(
			"expected large annotation to externalize to the chunk store: found=%t %#v",
			found,
			entry,
		)
	}
	if _, ok, err := store.readBlobContent(entry.ExternalDigest); err != nil || !ok {
		t.Fatalf(
			"externalized payload not retrievable from chunk store: ok=%t err=%v",
			ok,
			err,
		)
	}
	if _, err := os.Stat(store.objectDir(digest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(
			"externalized payload must not create a per-object directory, stat err=%v",
			err,
		)
	}

	annotation, ok, err := store.readPackedAnnotation(digest, "analysis", "summary")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || annotation.Data["summary"] != payload {
		t.Fatalf("externalized annotation did not round-trip: ok=%t %#v", ok, annotation)
	}
}

func TestPackedAnnotationLatestWins(t *testing.T) {
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
		AnalyzerName: "summary",
		AnalyzerVer:  "1",
		Data:         map[string]any{"summary": "first"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest: digest,
		Kind:         "analysis",
		AnalyzerName: "summary",
		AnalyzerVer:  "2",
		Data:         map[string]any{"summary": "second"},
	}); err != nil {
		t.Fatal(err)
	}

	annotation, ok, err := store.readPackedAnnotation(digest, "analysis", "summary")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || annotation.Data["summary"] != "second" || annotation.AnalyzerVer != "2" {
		t.Fatalf("expected latest annotation to win: ok=%t %#v", ok, annotation)
	}

	annotations, err := store.readPackedAnnotations(digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(annotations) != 1 {
		t.Fatalf("expected a single latest annotation per analyzer, got %d", len(annotations))
	}
}
