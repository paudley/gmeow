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

// gcGracePeriod protects chunks and recipes written within this window of a GC
// run from being swept, so an in-flight Put whose recipe is not yet visible to
// the GC snapshot cannot have its freshly written chunks reclaimed.
const gcGracePeriod = 15 * time.Minute

// GCReport summarizes a garbage-collection pass.
type GCReport struct {
	ScannedChunks  int `json:"scanned_chunks"`
	SweptChunks    int `json:"swept_chunks"`
	RetainedChunks int `json:"retained_chunks"`
	SweptRecipes   int `json:"swept_recipes"`
}

// DeleteObject removes an object's metadata — manifest, content recipe, recovery
// sidecar, annotations, and the source/alias/compound-parent index entries it
// owns — in one atomic batch. The content chunks it referenced are left for the
// next Gc to reclaim once no remaining recipe references them.
func (store *FilesystemStore) DeleteObject(
	ctx context.Context,
	digest contracts.ObjectDigest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateObjectDigest(digest); err != nil {
		return err
	}

	keys := []string{
		manifestKey(digest),
		objectRecipeKey(digest),
		"rec/" + string(digest),
		compoundParentKey(digest),
	}

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		for _, provenance := range manifest.Provenance {
			ref := sourceObjectRefFromProvenance(provenance)
			if validateSourceObjectRef(ref) == nil {
				keys = append(keys, "si/"+sourceObjectRefKey(ref), "sa/"+sourceAliasKey(ref))
			}
		}
	}

	if err := store.metaIterPrefix(
		annotationPrefix(digest),
		func(key string, _ []byte) error {
			keys = append(keys, key)

			return nil
		},
	); err != nil {
		return err
	}

	return store.metaDeleteKeys(keys)
}

// Gc reclaims chunk-index entries and recipes no longer reachable from any
// object manifest or externalized annotation. It runs online against a metadata
// snapshot; concurrent writes are protected by a two-part guard: recipes
// committed after the snapshot are re-scanned from the live store, and any chunk
// or recipe touched within gcGracePeriod is retained. Byte reclamation from the
// packs themselves is performed by Repack.
func (store *FilesystemStore) Gc(ctx context.Context) (GCReport, error) {
	return store.gcWithGrace(ctx, gcGracePeriod)
}

// gcWithGrace is Gc with an explicit grace window (tests use zero to observe
// immediate sweeping; production uses gcGracePeriod).
func (store *FilesystemStore) gcWithGrace(
	ctx context.Context,
	grace time.Duration,
) (GCReport, error) {
	if err := ctx.Err(); err != nil {
		return GCReport{}, err
	}

	gcStart := time.Now().UTC()
	graceCutoff := gcStart.Add(-grace)

	snapshot, err := store.metaSnapshot()
	if err != nil {
		return GCReport{}, err
	}
	defer func() { _ = snapshot.Close() }()

	// Externalized annotation payloads are content recipes referenced by an
	// annotation's ExternalDigest; collect those digests so their recipes count
	// as reachable.
	annotationPayloads := map[string]struct{}{}
	if err := snapshotIterPrefix(snapshot, "a/", func(_ string, value []byte) error {
		var entry packedObjectAnnotationEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return err
		}
		if entry.ExternalDigest != "" {
			annotationPayloads[string(entry.ExternalDigest)] = struct{}{}
		}

		return nil
	}); err != nil {
		return GCReport{}, err
	}

	// Mark: a recipe is reachable if its object manifest exists or it backs an
	// annotation payload; a young orphan recipe is kept until it ages past the
	// grace window. Reachable (and young) recipes contribute live chunks.
	liveChunks := map[string]struct{}{}
	orphanRecipes := map[string]contracts.ObjectDigest{}
	if err := snapshotIterPrefix(snapshot, "r/", func(key string, value []byte) error {
		var recipe objectRecipeEntry
		if err := json.Unmarshal(value, &recipe); err != nil {
			return err
		}
		digest := contracts.ObjectDigest(strings.TrimPrefix(key, "r/"))

		reachable, err := snapshotHas(snapshot, manifestKey(digest))
		if err != nil {
			return err
		}
		if !reachable {
			_, reachable = annotationPayloads[string(digest)]
		}

		// Reachable recipes keep their chunks. A young but unreachable recipe is
		// also kept (chunks retained) until it ages past the grace window, at
		// which point it becomes an orphan-sweep candidate.
		if reachable || !recipe.UpdatedAt.Before(graceCutoff) {
			for _, hash := range recipe.ChunkHashes {
				liveChunks[hash] = struct{}{}
			}

			return nil
		}

		orphanRecipes[key] = digest

		return nil
	}); err != nil {
		return GCReport{}, err
	}

	// Guard in-flight Puts: recipes committed at or after the snapshot may
	// re-reference pre-existing chunks (dedup hits) without being visible above.
	if err := store.metaIterPrefix("r/", func(_ string, value []byte) error {
		var recipe objectRecipeEntry
		if err := json.Unmarshal(value, &recipe); err != nil {
			return err
		}
		if !recipe.UpdatedAt.Before(gcStart) {
			for _, hash := range recipe.ChunkHashes {
				liveChunks[hash] = struct{}{}
			}
		}

		return nil
	}); err != nil {
		return GCReport{}, err
	}

	report := GCReport{}
	var deadChunks []string
	if err := snapshotIterPrefix(snapshot, "c/", func(key string, value []byte) error {
		report.ScannedChunks++
		var entry chunkIndexEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return err
		}
		if _, ok := liveChunks[entry.ChunkHash]; ok {
			return nil
		}
		if !entry.UpdatedAt.Before(graceCutoff) {
			return nil
		}
		deadChunks = append(deadChunks, key)

		return nil
	}); err != nil {
		return GCReport{}, err
	}

	if err := store.metaDeleteKeys(deadChunks); err != nil {
		return GCReport{}, err
	}
	report.SweptChunks = len(deadChunks)
	report.RetainedChunks = report.ScannedChunks - report.SweptChunks

	// Re-check orphan recipes against the live store so a digest re-created after
	// the snapshot is not deleted, then sweep the rest.
	swept := make([]string, 0, len(orphanRecipes))
	for key, digest := range orphanRecipes {
		exists, err := store.metaHas(manifestKey(digest))
		if err != nil {
			return GCReport{}, err
		}
		if !exists {
			swept = append(swept, key)
		}
	}
	if err := store.metaDeleteKeys(swept); err != nil {
		return GCReport{}, err
	}
	report.SweptRecipes = len(swept)

	return report, nil
}
