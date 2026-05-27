// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func (store *FilesystemStore) readObjectAnnotations(
	digest contracts.ObjectDigest,
) ([]contracts.Annotation, error) {
	object := ProjectionObject{
		Digest: digest,
		Path:   store.objectDir(digest),
	}
	store.readProjectionObject(&object)

	if len(object.Findings) > 0 {
		return nil, fmt.Errorf(
			"read annotations for %s: %s",
			digest,
			object.Findings[0].Message,
		)
	}

	return object.Annotations, nil
}

func (store *FilesystemStore) refreshIndexedParentsForSubobject(
	ctx context.Context,
	childDigest contracts.ObjectDigest,
) error {
	parents, err := store.compoundParentsForChild(ctx, childDigest)
	if err != nil {
		return err
	}
	if len(parents) == 0 {
		return nil
	}

	childAnnotations, err := store.readObjectAnnotations(childDigest)
	if err != nil {
		return err
	}

	summaries := []map[string]any{}
	for _, annotation := range childAnnotations {
		if annotation.Kind != "analysis" {
			continue
		}

		summaries = append(summaries, map[string]any{
			"analyzer_name":    annotation.AnalyzerName,
			"analyzer_version": annotation.AnalyzerVer,
			"generated_at":     annotation.GeneratedAt,
			"status": firstNonEmpty(
				stringFromAny(annotation.Data["status"]),
				"complete",
			),
		})
	}
	if len(summaries) == 0 {
		return nil
	}

	seen := map[contracts.ObjectDigest]bool{}
	for _, parent := range parents {
		if seen[parent.ParentDigest] {
			continue
		}
		seen[parent.ParentDigest] = true
		if err := store.refreshIndexedParentForSubobject(
			ctx,
			parent.ParentDigest,
			childDigest,
			summaries,
		); err != nil {
			return err
		}
	}

	return nil
}

func (store *FilesystemStore) refreshIndexedParentForSubobject(
	ctx context.Context,
	parentDigest contracts.ObjectDigest,
	childDigest contracts.ObjectDigest,
	summaries []map[string]any,
) error {
	manifest, err := store.ReadManifest(ctx, parentDigest)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !manifestContainsPart(manifest, childDigest) {
		return nil
	}
	if manifest.Analysis == nil {
		manifest.Analysis = map[string]any{}
	}

	partAnalysis, ok := manifest.Analysis["part_analysis"].(map[string]any)
	if !ok {
		partAnalysis = map[string]any{}
	}
	partAnalysis[string(childDigest)] = map[string]any{
		"refreshed_at": time.Now().UTC(),
		"annotations":  summaries,
	}
	manifest.Analysis["part_analysis"] = partAnalysis
	manifest.UpdatedAt = time.Now().UTC()

	return store.writeCompressedJSON(
		store.objectPath(parentDigest, manifestFilename),
		manifest,
	)
}

func manifestContainsPart(
	manifest contracts.Manifest,
	digest contracts.ObjectDigest,
) bool {
	for _, part := range manifest.Compound.Parts {
		if part.Digest == digest {
			return true
		}
	}

	return false
}
