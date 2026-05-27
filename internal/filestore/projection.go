// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func (store *FilesystemStore) ProjectionObject(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (ProjectionObject, bool, error) {
	if err := ctx.Err(); err != nil {
		return ProjectionObject{}, false, err
	}
	if err := validateObjectDigest(digest); err != nil {
		return ProjectionObject{}, false, err
	}

	path := store.objectDir(digest)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return ProjectionObject{}, false, nil
	} else if err != nil {
		return ProjectionObject{}, false, err
	}

	object := ProjectionObject{
		Digest: digest,
		Path:   path,
	}
	store.readProjectionObject(&object)

	return object, true, nil
}

func (store *FilesystemStore) WalkProjection(
	ctx context.Context,
	fn ProjectionFunc,
) error {
	if fn == nil {
		return errors.New("projection callback is required")
	}

	base := filepath.Join(store.root, "objects", "blake3")

	err := filepath.WalkDir(
		base,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) && path == base {
					return nil
				}

				return walkErr
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			if !entry.IsDir() || !looksLikeDigest(entry.Name()) {
				return nil
			}

			digest := contracts.ObjectDigest(entry.Name())
			object := ProjectionObject{
				Digest: digest,
				Path:   path,
			}
			if expectedPath := store.objectDir(digest); path != expectedPath {
				object.Findings = append(object.Findings, ProjectionFinding{
					Digest:  digest,
					Path:    path,
					Code:    "object_path_mismatch",
					Message: "expected " + expectedPath,
				})
			} else {
				store.readProjectionObject(&object)
			}

			if err := fn(object); err != nil {
				return err
			}

			return filepath.SkipDir
		},
	)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

func (store *FilesystemStore) WalkChangedProjection(
	ctx context.Context,
	since time.Time,
	fn ProjectionFunc,
) error {
	if fn == nil {
		return errors.New("projection callback is required")
	}

	return store.WalkProjection(ctx, func(object ProjectionObject) error {
		if len(object.Findings) > 0 || projectionObjectChangedAfter(object, since) {
			return fn(object)
		}

		return nil
	})
}

func (store *FilesystemStore) readProjectionObject(object *ProjectionObject) {
	manifestPath := filepath.Join(object.Path, manifestFilename)
	if err := store.readCompressedJSON(manifestPath, &object.Manifest); err != nil {
		object.Findings = append(object.Findings, ProjectionFinding{
			Digest:  object.Digest,
			Path:    manifestPath,
			Code:    "manifest_read_failed",
			Message: err.Error(),
		})

		return
	}

	if object.Manifest.ObjectDigest == "" {
		object.Manifest.ObjectDigest = object.Digest
	}

	entries, err := os.ReadDir(object.Path)
	if err != nil {
		object.Findings = append(object.Findings, ProjectionFinding{
			Digest:  object.Digest,
			Path:    object.Path,
			Code:    "annotation_list_failed",
			Message: err.Error(),
		})

		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !isProjectionAnnotationFilename(entry.Name()) {
			continue
		}

		path := filepath.Join(object.Path, entry.Name())

		var annotation contracts.Annotation
		err := store.readCompressedJSON(path, &annotation)
		if err != nil {
			object.Findings = append(object.Findings, ProjectionFinding{
				Digest:  object.Digest,
				Path:    path,
				Code:    "annotation_read_failed",
				Message: err.Error(),
			})

			continue
		}

		if annotation.ObjectDigest == "" {
			annotation.ObjectDigest = object.Digest
		}

		if annotation.Kind == "" {
			annotation.Kind = strings.TrimSuffix(entry.Name(), ".json.zst")
		}

		object.Annotations = append(object.Annotations, annotation)
	}

	sort.SliceStable(object.Annotations, func(left, right int) bool {
		leftAnnotation := object.Annotations[left]
		rightAnnotation := object.Annotations[right]
		if leftAnnotation.Kind != rightAnnotation.Kind {
			return leftAnnotation.Kind < rightAnnotation.Kind
		}

		if leftAnnotation.AnalyzerName != rightAnnotation.AnalyzerName {
			return leftAnnotation.AnalyzerName < rightAnnotation.AnalyzerName
		}

		return leftAnnotation.AnalyzerVer < rightAnnotation.AnalyzerVer
	})
}
