// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

// TestDeleteImportRemovesSourceContributionPreservingShared verifies the
// provenance-refcount semantics of DeleteImport: an object owned only by the
// deleted source is removed, while an object also owned by another source
// survives with just that source's provenance detached.
func TestDeleteImportRemovesSourceContributionPreservingShared(t *testing.T) {
	ctx := context.Background()
	store := NewFilesystemStore(t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	provenance := func(name, externalID string) contracts.Provenance {
		return contracts.Provenance{
			SourceKind: contracts.MailArchiveSourceKind,
			SourceName: name,
			ExternalID: externalID,
		}
	}

	// Shared content owned by two imports (src1 then src2 merge onto one digest).
	shared := bytes.Repeat([]byte("shared content across imports "), 64)
	sharedDigest, err := store.Put(ctx, PutRequest{
		Reader:     bytes.NewReader(shared),
		Facets:     []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{provenance("src1", "shared-1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, PutRequest{
		Reader:     bytes.NewReader(shared),
		Facets:     []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{provenance("src2", "shared-2")},
	}); err != nil {
		t.Fatal(err)
	}

	// Content owned only by src1.
	unique := bytes.Repeat([]byte("unique to src1 "), 64)
	uniqueDigest, err := store.Put(ctx, PutRequest{
		Reader:     bytes.NewReader(unique),
		Facets:     []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{provenance("src1", "unique-1")},
	})
	if err != nil {
		t.Fatal(err)
	}

	report, err := store.DeleteImport(ctx, contracts.MailArchiveSourceKind, "src1")
	if err != nil {
		t.Fatal(err)
	}
	if report.ObjectsDeleted != 1 || report.ProvenanceDetached != 1 {
		t.Fatalf("unexpected delete report: %#v", report)
	}

	// The unique object is gone.
	if _, err := store.ReadManifest(ctx, uniqueDigest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected unique object deleted, got err=%v", err)
	}

	// The shared object survives with only src2 provenance.
	manifest, err := store.ReadManifest(ctx, sharedDigest)
	if err != nil {
		t.Fatalf("shared object should survive: %v", err)
	}
	if len(manifest.Provenance) != 1 || manifest.Provenance[0].SourceName != "src2" {
		t.Fatalf("expected only src2 provenance to remain, got %#v", manifest.Provenance)
	}

	// src1's lookup is gone; src2 still resolves to the shared object.
	if _, found, err := store.LookupSourceObject(ctx, contracts.SourceObjectRef{
		SourceKind: contracts.MailArchiveSourceKind,
		SourceName: "src1",
		ExternalID: "shared-1",
	}); err != nil || found {
		t.Fatalf("expected src1 lookup removed: found=%t err=%v", found, err)
	}
	if digest, found, err := store.LookupSourceObject(ctx, contracts.SourceObjectRef{
		SourceKind: contracts.MailArchiveSourceKind,
		SourceName: "src2",
		ExternalID: "shared-2",
	}); err != nil || !found || digest != sharedDigest {
		t.Fatalf(
			"expected src2 still resolves shared: digest=%s found=%t err=%v",
			digest,
			found,
			err,
		)
	}
}
