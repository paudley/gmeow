// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
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

	manifest, err := store.ReadManifest(ctx, digest)
	if errors.Is(err, os.ErrNotExist) {
		return ProjectionObject{}, false, nil
	} else if err != nil {
		return ProjectionObject{}, false, err
	}

	object := ProjectionObject{
		Digest:   digest,
		Path:     store.objectDir(digest),
		Manifest: manifest,
	}
	if object.Manifest.ObjectDigest == "" {
		object.Manifest.ObjectDigest = digest
	}
	store.attachProjectionAnnotations(&object)

	return object, true, nil
}

// WalkProjection enumerates every object by scanning the manifest key space in
// the metadata LSM. Objects no longer have on-disk directories, so an ordered
// Pebble prefix scan over m/ replaces the former objects/blake3 tree walk.
func (store *FilesystemStore) WalkProjection(
	ctx context.Context,
	fn ProjectionFunc,
) error {
	if fn == nil {
		return errors.New("projection callback is required")
	}

	return store.metaIterPrefix("m/", func(key string, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		digest := contracts.ObjectDigest(strings.TrimPrefix(key, "m/"))
		object := ProjectionObject{
			Digest: digest,
			Path:   store.objectDir(digest),
		}

		if err := json.Unmarshal(value, &object.Manifest); err != nil {
			object.Findings = append(object.Findings, ProjectionFinding{
				Digest:  digest,
				Path:    object.Path,
				Code:    "manifest_read_failed",
				Message: err.Error(),
			})

			return fn(object)
		}

		if object.Manifest.ObjectDigest == "" {
			object.Manifest.ObjectDigest = digest
		}
		store.attachProjectionAnnotations(&object)

		return fn(object)
	})
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

// attachProjectionAnnotations loads the object's annotations from the metadata
// LSM onto an object whose manifest is already populated.
func (store *FilesystemStore) attachProjectionAnnotations(object *ProjectionObject) {
	annotations, err := store.readPackedAnnotations(object.Digest)
	if err != nil {
		object.Findings = append(object.Findings, ProjectionFinding{
			Digest:  object.Digest,
			Path:    object.Path,
			Code:    "annotation_read_failed",
			Message: err.Error(),
		})

		return
	}

	for _, annotation := range annotations {
		if annotation.ObjectDigest == "" {
			annotation.ObjectDigest = object.Digest
		}

		object.Annotations = append(object.Annotations, annotation)
	}
}
