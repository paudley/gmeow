// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/zeebo/blake3"

	"blackcat.ca/gmeow/internal/contracts"
)

// Object annotations (analysis, scheduler, overlays, and other non-reserved
// kinds) are stored as keyed records in the Pebble metadata LSM rather than as
// one compressed file per annotation per object. At target scale a single
// message accrues ~7-10 tiny annotations; one 4K filesystem block per file
// dominated on-disk usage. The LSM packs them with the rest of the metadata.
//
// Large annotation payloads (e.g. summary or full-text extraction) would bloat
// the LSM, so any annotation whose encoded JSON exceeds annotationInlineMaxBytes
// externalizes into the content-addressed chunk store (the same dedup + pack
// tier as blob content); the record then carries only the payload's content
// digest. Small annotations are inlined. Externalized payloads are addressed by
// content hash so they are de-dup friendly and safe to leave behind until GC.
const (
	// annotationInlineMaxBytes is the encoded-JSON size at or below which an
	// annotation is inlined into the metadata record. Above it the payload is
	// externalized to a compressed side file to preserve compression and keep
	// the record small. A small message's manifest/scheduler/NER/category
	// annotations are well under this; large summaries exceed it.
	annotationInlineMaxBytes = 4096
)

type packedObjectAnnotationEntry struct {
	UpdatedAt      time.Time              `json:"updated_at"`
	Inline         *contracts.Annotation  `json:"inline,omitempty"`
	Digest         contracts.ObjectDigest `json:"digest"`
	Kind           string                 `json:"kind"`
	AnalyzerName   string                 `json:"analyzer_name,omitempty"`
	ExternalDigest contracts.ObjectDigest `json:"external_digest,omitempty"`
}

func annotationKey(digest contracts.ObjectDigest, kind, analyzerName string) string {
	return "a/" + string(digest) + "/" + kind + "/" + analyzerName
}

func annotationPrefix(digest contracts.ObjectDigest) string {
	return "a/" + string(digest) + "/"
}

// writePackedAnnotation appends a latest-wins annotation record for an object.
// Annotations larger than annotationInlineMaxBytes externalize their payload to
// a content-addressed zstd side file in the object directory.
func (store *FilesystemStore) writePackedAnnotation(
	ctx context.Context,
	annotation contracts.Annotation,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	encoded, err := canonicalJSON(annotation)
	if err != nil {
		return err
	}

	entry := packedObjectAnnotationEntry{
		UpdatedAt:    time.Now().UTC(),
		Digest:       annotation.ObjectDigest,
		Kind:         annotation.Kind,
		AnalyzerName: annotation.AnalyzerName,
	}

	if len(encoded) <= annotationInlineMaxBytes {
		inline := annotation
		entry.Inline = &inline
	} else {
		// Store the payload in the content-addressed chunk store, keyed by its
		// own BLAKE3 digest, and reference it from the record.
		sum := blake3.Sum256(encoded)
		contentDigest := contracts.ObjectDigest(hex.EncodeToString(sum[:]))
		if err := store.storeBlobContent(
			ctx,
			contentDigest,
			encoded,
			"application/json",
			nil,
		); err != nil {
			return err
		}
		entry.ExternalDigest = contentDigest
	}

	sink := store.syncSink()
	if err := sink.set(
		annotationKey(annotation.ObjectDigest, annotation.Kind, annotation.AnalyzerName),
		entry,
	); err != nil {
		return err
	}

	return store.recordProjectionChangeTo(
		sink,
		annotation.ObjectDigest,
		annotation.GeneratedAt,
	)
}

func (store *FilesystemStore) resolvePackedAnnotation(
	entry packedObjectAnnotationEntry,
) (contracts.Annotation, error) {
	if entry.Inline != nil {
		return *entry.Inline, nil
	}
	if entry.ExternalDigest == "" {
		return contracts.Annotation{}, errors.New(
			"packed annotation has neither inline nor external payload",
		)
	}

	content, ok, err := store.readBlobContent(entry.ExternalDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}
	if !ok {
		return contracts.Annotation{}, fmt.Errorf(
			"external annotation payload %s missing from chunk store",
			entry.ExternalDigest,
		)
	}

	var annotation contracts.Annotation
	if err := json.Unmarshal(content, &annotation); err != nil {
		return contracts.Annotation{}, fmt.Errorf(
			"decode external annotation payload: %w",
			err,
		)
	}

	return annotation, nil
}

// readPackedAnnotation returns the latest annotation matching the object digest,
// kind, and analyzer name. ok is false when no such annotation exists.
func (store *FilesystemStore) readPackedAnnotation(
	digest contracts.ObjectDigest,
	kind, analyzerName string,
) (contracts.Annotation, bool, error) {
	var entry packedObjectAnnotationEntry
	ok, err := store.metaGet(annotationKey(digest, kind, analyzerName), &entry)
	if err != nil {
		return contracts.Annotation{}, false, err
	}
	if !ok {
		return contracts.Annotation{}, false, nil
	}

	annotation, err := store.resolvePackedAnnotation(entry)
	if err != nil {
		return contracts.Annotation{}, false, err
	}

	return annotation, true, nil
}

// readPackedAnnotations returns the latest annotation per (kind, analyzer) for
// the object digest, sorted by kind then analyzer name then version for stable
// projection output.
func (store *FilesystemStore) readPackedAnnotations(
	digest contracts.ObjectDigest,
) ([]contracts.Annotation, error) {
	annotations := make([]contracts.Annotation, 0)
	err := store.metaIterPrefix(
		annotationPrefix(digest),
		func(_ string, value []byte) error {
			var entry packedObjectAnnotationEntry
			if err := json.Unmarshal(value, &entry); err != nil {
				return err
			}
			annotation, resolveErr := store.resolvePackedAnnotation(entry)
			if resolveErr != nil {
				return resolveErr
			}
			annotations = append(annotations, annotation)

			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(annotations, func(left, right int) bool {
		leftAnnotation := annotations[left]
		rightAnnotation := annotations[right]
		if leftAnnotation.Kind != rightAnnotation.Kind {
			return leftAnnotation.Kind < rightAnnotation.Kind
		}
		if leftAnnotation.AnalyzerName != rightAnnotation.AnalyzerName {
			return leftAnnotation.AnalyzerName < rightAnnotation.AnalyzerName
		}

		return leftAnnotation.AnalyzerVer < rightAnnotation.AnalyzerVer
	})

	return annotations, nil
}
