// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

// externalDigestJSONKey is the serialized field name of
// packedObjectAnnotationEntry.ExternalDigest; its presence in a raw annotation
// row gates the (relatively costly) JSON unmarshal during the GC mark scan.
var externalDigestJSONKey = []byte(`"external_digest"`)

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
			if validateSourceObjectRef(ref) != nil {
				continue
			}
			// Only remove source/alias index rows that still point at this object.
			// A re-ingest under the same source ref may have repointed the row to a
			// newer digest; deleting that would orphan the live object's lookup.
			if indexed, ok, idxErr := store.readPackedSourceObjectIndex(
				ctx,
				ref,
			); idxErr != nil {
				return idxErr
			} else if ok &&
				indexed.ObjectDigest == digest {
				keys = append(keys, "si/"+sourceObjectRefKey(ref))
			}
			if aliasDigest, ok, aliasErr := store.readPackedSourceAlias(
				ctx,
				ref,
			); aliasErr != nil {
				return aliasErr
			} else if ok &&
				aliasDigest == digest {
				keys = append(keys, "sa/"+sourceAliasKey(ref))
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

// DeleteImportReport summarizes a DeleteImport pass.
type DeleteImportReport struct {
	ObjectsScanned     int `json:"objects_scanned"`
	ObjectsDeleted     int `json:"objects_deleted"`
	ProvenanceDetached int `json:"provenance_detached"`
}

// DeleteImport removes the contribution of one source (an import) from the
// store: for every object whose manifest carries provenance from the given
// source kind+name, that provenance — and the source index rows it owns — are
// removed. An object that has no remaining provenance after the removal had this
// source as its only owner and is deleted entirely (its chunks are left for Gc);
// an object still owned by another source survives with the source detached.
//
// This is the correct semantics for "delete an import": a part shared with other
// live imports (e.g. an identical attachment, or the corpus-wide mime-structure
// stub) keeps that other provenance and is preserved, while objects unique to
// the deleted import become unreferenced and reclaimable.
func (store *FilesystemStore) DeleteImport(
	ctx context.Context,
	sourceKind, sourceName string,
) (DeleteImportReport, error) {
	if err := ctx.Err(); err != nil {
		return DeleteImportReport{}, err
	}
	if strings.TrimSpace(sourceKind) == "" || strings.TrimSpace(sourceName) == "" {
		return DeleteImportReport{}, errors.New("source kind and name are required")
	}

	// Collect candidate digests first; mutating manifests while iterating the
	// manifest prefix is avoided by separating the scan from the rewrite.
	var candidates []contracts.ObjectDigest
	report := DeleteImportReport{}
	if err := store.metaIterPrefix("m/", func(key string, value []byte) error {
		report.ObjectsScanned++
		var manifest contracts.Manifest
		if err := json.Unmarshal(value, &manifest); err != nil {
			return err
		}
		if manifestHasSource(manifest, sourceKind, sourceName) {
			candidates = append(
				candidates,
				contracts.ObjectDigest(strings.TrimPrefix(key, "m/")),
			)
		}

		return nil
	}); err != nil {
		return DeleteImportReport{}, err
	}

	for _, digest := range candidates {
		detached, deleted, err := store.detachSourceFromObject(
			ctx,
			digest,
			sourceKind,
			sourceName,
		)
		if err != nil {
			return report, err
		}
		if deleted {
			report.ObjectsDeleted++
		} else if detached {
			report.ProvenanceDetached++
		}
	}

	return report, nil
}

// detachSourceFromObject removes the source's provenance from one object under
// its manifest lock. It re-reads the manifest (the scan is racy), drops the
// matching provenance entries and the source index rows they own, and either
// deletes the object (no provenance left) or rewrites the trimmed manifest.
func (store *FilesystemStore) detachSourceFromObject(
	ctx context.Context,
	digest contracts.ObjectDigest,
	sourceKind, sourceName string,
) (detached, deleted bool, err error) {
	unlock := store.lockKey("manifest:" + string(digest))
	defer unlock()

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		}

		return false, false, err
	}

	kept := make([]contracts.Provenance, 0, len(manifest.Provenance))
	var removed []contracts.SourceObjectRef
	for _, provenance := range manifest.Provenance {
		if provenance.SourceKind == sourceKind && provenance.SourceName == sourceName {
			removed = append(removed, sourceObjectRefFromProvenance(provenance))

			continue
		}
		kept = append(kept, provenance)
	}
	if len(removed) == 0 {
		return false, false, nil
	}

	if len(kept) == 0 {
		// This source was the object's only owner: delete the whole object.
		if err := store.DeleteObject(ctx, digest); err != nil {
			return false, false, err
		}

		return true, true, nil
	}

	manifest.Provenance = kept
	manifest.UpdatedAt = time.Now().UTC()
	if err := store.writeManifest(digest, manifest); err != nil {
		return false, false, err
	}

	// Drop the source/alias index rows owned by the removed refs, but only when
	// they still point at this object (a re-ingest may have repointed them).
	var indexKeys []string
	for _, ref := range removed {
		if validateSourceObjectRef(ref) != nil {
			continue
		}
		if indexed, ok, idxErr := store.readPackedSourceObjectIndex(ctx, ref); idxErr != nil {
			return false, false, idxErr
		} else if ok && indexed.ObjectDigest == digest {
			indexKeys = append(indexKeys, "si/"+sourceObjectRefKey(ref))
		}
		if aliasDigest, ok, aliasErr := store.readPackedSourceAlias(
			ctx,
			ref,
		); aliasErr != nil {
			return false, false, aliasErr
		} else if ok &&
			aliasDigest == digest {
			indexKeys = append(indexKeys, "sa/"+sourceAliasKey(ref))
		}
	}
	if len(indexKeys) > 0 {
		if err := store.metaDeleteKeys(indexKeys); err != nil {
			return false, false, err
		}
	}

	return true, false, nil
}

// manifestHasSource reports whether any provenance entry matches the source.
func manifestHasSource(
	manifest contracts.Manifest,
	sourceKind, sourceName string,
) bool {
	for _, provenance := range manifest.Provenance {
		if provenance.SourceKind == sourceKind && provenance.SourceName == sourceName {
			return true
		}
	}

	return false
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

	store.maintenanceMu.Lock()
	defer store.maintenanceMu.Unlock()

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
		// external_digest is omitempty, so an annotation that externalizes nothing
		// never carries the field; skip the unmarshal for the common inline case.
		if !bytes.Contains(value, externalDigestJSONKey) {
			return nil
		}
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
