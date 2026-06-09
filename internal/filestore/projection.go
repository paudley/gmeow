// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
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

	if since.IsZero() {
		return store.WalkProjection(ctx, fn)
	}

	indexed := 0
	seen := map[contracts.ObjectDigest]struct{}{}
	err := store.metaIterRange(
		projectionChangeScanLowerBound(since),
		prefixUpperBound("pc/"),
		func(key string, _ []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			digest := projectionChangeDigestFromKey(key)
			if digest == "" {
				return nil
			}
			indexed++
			if _, ok := seen[digest]; ok {
				return nil
			}
			seen[digest] = struct{}{}

			object, found, err := store.ProjectionObject(ctx, digest)
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
			if len(object.Findings) > 0 || projectionObjectChangedAfter(object, since) {
				return fn(object)
			}

			return nil
		},
	)
	if err != nil {
		return err
	}
	if indexed > 0 {
		return nil
	}

	return store.WalkProjection(ctx, func(object ProjectionObject) error {
		if len(object.Findings) > 0 || projectionObjectChangedAfter(object, since) {
			if len(object.Findings) == 0 {
				if err := store.recordProjectionChangeTo(
					store.syncSink(),
					object.Digest,
					projectionObjectChangedAt(object),
				); err != nil {
					return err
				}
			}
			return fn(object)
		}

		return nil
	})
}

type projectionChangeEntry struct {
	Key       string                 `json:"key"`
	Digest    contracts.ObjectDigest `json:"digest"`
	ChangedAt time.Time              `json:"changed_at"`
}

func projectionChangeKey(digest contracts.ObjectDigest, changedAt time.Time) string {
	return fmt.Sprintf("pc/%020d/%s", changedAt.UTC().UnixNano(), digest)
}

func projectionChangeLatestKey(digest contracts.ObjectDigest) string {
	return "pc-latest/" + string(digest)
}

func projectionChangeScanLowerBound(since time.Time) string {
	return fmt.Sprintf("pc/%020d/", since.UTC().UnixNano()+1)
}

func projectionChangeDigestFromKey(key string) contracts.ObjectDigest {
	if !strings.HasPrefix(key, "pc/") {
		return ""
	}
	return contracts.ObjectDigest(path.Base(key))
}

func (store *FilesystemStore) recordProjectionChangeTo(
	sink metaSink,
	digest contracts.ObjectDigest,
	changedAt time.Time,
) error {
	if digest == "" || changedAt.IsZero() {
		return nil
	}

	latestKey := projectionChangeLatestKey(digest)
	var previous projectionChangeEntry
	if ok, err := store.metaGet(latestKey, &previous); err != nil {
		return err
	} else if ok && previous.Key != "" {
		if err := sink.delete(previous.Key); err != nil {
			return err
		}
	}

	entry := projectionChangeEntry{
		Key:       projectionChangeKey(digest, changedAt),
		Digest:    digest,
		ChangedAt: changedAt,
	}
	if err := sink.set(entry.Key, entry); err != nil {
		return err
	}

	return sink.set(latestKey, entry)
}

func projectionObjectChangedAt(object ProjectionObject) time.Time {
	changedAt := object.Manifest.UpdatedAt
	if object.Manifest.CreatedAt.After(changedAt) {
		changedAt = object.Manifest.CreatedAt
	}
	for _, annotation := range object.Annotations {
		if annotation.GeneratedAt.After(changedAt) {
			changedAt = annotation.GeneratedAt
		}
	}

	return changedAt
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
