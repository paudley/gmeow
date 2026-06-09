// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/blake3"

	"blackcat.ca/gmeow/internal/cache"
	"blackcat.ca/gmeow/internal/contracts"
)

const (
	identityStrategyFileBlake3       = "file_blake3"
	identityStrategyCompoundStableID = "compound_stable_id"
	compressionZstd                  = "zstd"
	blobFilename                     = "blob.zstd"
	recoveryFilename                 = "recovery.json"
	manifestFilename                 = "manifest.json.zst"
	sourceCursorFilename             = "cursor.json.zst"
	schemaVersion                    = "1"
	versionScaleRankTrivial          = 1
	versionScaleRankMinor            = versionScaleRankTrivial + 1
	versionScaleRankMajor            = versionScaleRankMinor + 1
)

type FilesystemStore struct {
	root     string
	locksMu  sync.Mutex
	keyLocks map[string]*filesystemKeyLock
	packMu   sync.Mutex
	metaOnce sync.Once
	metaInst *pebble.DB
	metaErr  error
	dicts    dictionaryCache
	packs    *packCache
	// activePack* caches the newest pack's id and size so appendToActivePack does
	// not os.ReadDir+Stat the pack directory on every chunk write. The store holds
	// Pebble's exclusive directory lock and is the sole writer to chunk-packs/, so
	// the cache is authoritative once populated; it is guarded by packMu (held by
	// every appender) and lazily filled on first append.
	activePackKnown bool
	activePackID    uint64
	activePackSize  int64
	// maintenanceMu serializes whole-store maintenance passes (gc, repack,
	// dictionary training) so they never run concurrently — e.g. repack must not
	// race gc, and two trainings must not race the dictionary-id allocation.
	maintenanceMu sync.Mutex
	// chunkCache holds decompressed, content-verified chunk bytes keyed by hash.
	// Chunk content is immutable by hash, so the cache needs no invalidation; it
	// saves a pack read + zstd decode on repeat reads (multi-analyzer same object,
	// projection re-reads, interface retrieves).
	chunkCache *cache.SizedLRU[string]
}

type filesystemKeyLock struct {
	mu   sync.Mutex
	refs int
}

// defaultChunkCacheBytes bounds the decompressed-chunk cache (~256 MiB).
const defaultChunkCacheBytes = 256 << 20

func NewFilesystemStore(root string) *FilesystemStore {
	return &FilesystemStore{
		root:       root,
		packs:      newPackCache(defaultPackCacheSize),
		chunkCache: cache.NewSizedLRU[string](defaultChunkCacheBytes),
	}
}

func (store *FilesystemStore) Put(
	ctx context.Context,
	request PutRequest,
) (contracts.ObjectDigest, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if err := validateFacets(request.Facets); err != nil {
		return "", err
	}

	// One commit batch buffers the whole object's metadata (chunk index, recipe,
	// manifest, recovery sidecar, source indexes) so it commits with a single
	// pack fsync + a single Pebble Sync instead of one fsync per chunk and per
	// key — the dominant cost when bulk-importing millions of small mail objects.
	cb, err := store.newCommitBatch()
	if err != nil {
		return "", err
	}
	defer cb.close()

	// Stream the content into the chunk store, computing identity in one pass —
	// the whole object is never buffered, so multi-gigabyte media/files are safe.
	digest, uncompressedSHA256, size, err := store.storeBlobReaderBatched(
		ctx,
		cb,
		request.Reader,
		request.MediaType,
		request.ContentRoles,
	)
	if err != nil {
		return "", err
	}

	// Serialize the manifest read-merge-write so concurrent puts of the same
	// content (or a concurrent AttachProvenance/WriteOverlays/analysis refresh on
	// the same digest) cannot lose each other's mutations.
	unlock := store.lockKey("manifest:" + string(digest))
	defer unlock()

	manifest := store.baseManifest(
		digest,
		string(digest),
		identityStrategyFileBlake3,
		request.MediaType,
		size,
		request.ContentRoles,
		request.Facets,
		request.Provenance,
		request.Relationships,
		contracts.Compound{IsCompound: false},
	)
	if existing, manifestErr := store.ReadManifest(ctx, digest); manifestErr == nil {
		manifest = mergeManifest(existing, manifest)
	} else if !errors.Is(manifestErr, os.ErrNotExist) {
		return "", fmt.Errorf("read existing manifest: %w", manifestErr)
	}

	if err := store.writeManifestTo(cb, digest, manifest); err != nil {
		return "", fmt.Errorf("write manifest: %w", err)
	}
	if err := store.writePackedRecoveryTo(
		ctx,
		cb,
		digest,
		recoverySidecarForStreamed(
			digest,
			size,
			string(digest),
			uncompressedSHA256,
			request.SourceHint,
			manifest,
		),
	); err != nil {
		return "", fmt.Errorf("write packed recovery sidecar: %w", err)
	}
	if err := store.recordSourceObjectIndexesTo(
		ctx,
		cb,
		digest,
		request.Provenance,
	); err != nil {
		return "", err
	}

	if err := cb.commit(); err != nil {
		return "", fmt.Errorf("commit object %s: %w", digest, err)
	}

	return digest, nil
}

// AttachProvenance records that a source observed an existing object. Re-observing
// an already-seen object (no new source/version key) is a 100% no-op: it performs
// no manifest rewrite and no source-index write, and reports changed=false so the
// caller can skip emitting a change notice. This keeps a bulk re-ingest of known
// duplicates from amplifying metadata writes or analysis scheduling.
func (store *FilesystemStore) AttachProvenance(
	ctx context.Context,
	digest contracts.ObjectDigest,
	provenance []contracts.Provenance,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if err := validateObjectDigest(digest); err != nil {
		return false, err
	}

	if len(provenance) == 0 {
		return false, errors.New("provenance is required")
	}

	for _, item := range provenance {
		err := validateSourceObjectRef(sourceObjectRefFromProvenance(item))
		if err != nil {
			return false, err
		}
	}

	unlock := store.lockKey("manifest:" + string(digest))
	defer unlock()

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		return false, fmt.Errorf("read provenance target manifest: %w", err)
	}

	if !provenanceAddsNew(manifest.Provenance, provenance) {
		return false, nil
	}

	manifest.Provenance = mergeProvenance(manifest.Provenance, provenance)
	manifest.UpdatedAt = time.Now().UTC()

	if err := store.writeManifest(digest, manifest); err != nil {
		return false, err
	}

	if err := store.recordSourceObjectIndexes(ctx, digest, provenance); err != nil {
		return false, err
	}

	return true, nil
}

func (store *FilesystemStore) PutCompound(
	ctx context.Context,
	request CompoundPutRequest,
) (contracts.ObjectDigest, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	objectID := strings.TrimSpace(request.ObjectID)
	if objectID == "" {
		return "", errors.New("compound object_id is required")
	}

	if err := validateFacets(request.Facets); err != nil {
		return "", err
	}

	parts := normalizedParts(request.Parts)
	if err := validateCompoundParts(parts); err != nil {
		return "", err
	}

	digest := contracts.ObjectDigest(blake3Hex([]byte("compound:" + objectID)))
	relationships := relationshipsWithCompoundParts(
		digest,
		request.Relationships,
		parts,
	)

	envelope, err := canonicalJSON(compoundEnvelope{
		SchemaVersion:    int(contracts.SchemaVersionPhase00),
		ObjectID:         objectID,
		IdentityStrategy: identityStrategyCompoundStableID,
		Provenance:       request.Provenance,
		Parts:            parts,
	})
	if err != nil {
		return "", err
	}

	manifest := store.baseManifest(
		digest,
		objectID,
		identityStrategyCompoundStableID,
		firstNonEmpty(request.MediaType, "application/vnd.gmeow.compound+json"),
		int64(len(envelope)),
		request.ContentRoles,
		request.Facets,
		request.Provenance,
		relationships,
		contracts.Compound{IsCompound: true, Parts: parts},
	)
	// Batch the compound object's content envelope, manifest, recovery sidecar,
	// and source indexes into one synced commit. The compound-parent index is a
	// guarded read-merge-write per child part, so it stays a separate write after
	// the object commits (its non-atomicity relative to the object is unchanged
	// from the pre-batch code, which also wrote it last).
	cb, err := store.newCommitBatch()
	if err != nil {
		return "", err
	}
	defer cb.close()

	if err := store.writeObject(
		ctx,
		cb,
		digest,
		envelope,
		request.SourceHint,
		manifest,
	); err != nil {
		return "", err
	}
	if err := store.recordSourceObjectIndexesTo(
		ctx,
		cb,
		digest,
		request.Provenance,
	); err != nil {
		return "", err
	}
	if err := cb.commit(); err != nil {
		return "", fmt.Errorf("commit compound object %s: %w", digest, err)
	}

	if err := store.recordCompoundParentIndexes(ctx, digest, parts); err != nil {
		return "", err
	}

	return digest, nil
}

func (store *FilesystemStore) Open(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := validateObjectDigest(digest); err != nil {
		return nil, err
	}

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// file_blake3 objects are verified against their content hash as they stream
	// (the reader fails at EOF on mismatch); compound_stable_id objects carry a
	// synthetic digest, so content verification is skipped.
	verifyDigest := digest
	if manifest.IdentityStrategy == identityStrategyCompoundStableID {
		verifyDigest = ""
	}

	reader, ok, err := store.openBlobReader(digest, verifyDigest)
	if err != nil {
		return nil, err
	}
	if ok {
		return reader, nil
	}

	// Legacy whole-blob fallback for streamed/legacy objects not stored as chunks.
	content, err := readZstdFile(store.objectPath(digest, blobFilename))
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(content)), nil
}

// manifestKey is the Pebble metadata key for an object's manifest. Manifests
// are metadata, not content, so they live in the LSM alongside recipes and
// annotations rather than as a per-object manifest.json.zst file — eliminating
// the 4K-block-per-object small-file amplification (and the per-object
// directory entirely, since content is in chunk packs).
func manifestKey(digest contracts.ObjectDigest) string {
	return "m/" + string(digest)
}

func (store *FilesystemStore) writeManifest(
	digest contracts.ObjectDigest,
	manifest contracts.Manifest,
) error {
	return store.writeManifestTo(store.syncSink(), digest, manifest)
}

func (store *FilesystemStore) writeManifestTo(
	sink metaSink,
	digest contracts.ObjectDigest,
	manifest contracts.Manifest,
) error {
	if err := sink.set(manifestKey(digest), manifest); err != nil {
		return err
	}

	return store.recordProjectionChangeTo(sink, digest, manifest.UpdatedAt)
}

// ReadManifest returns a manifest from metadata storage.
//
// Note: a write-through manifest cache was evaluated here but removed — the
// manifest is mutable (provenance/annotations/version-set facets append), and a
// lock-free-read cache produced stale reads in the importer's cross-service
// read-modify-write collision/promotion flow. Pebble's block cache already backs
// this read, so the only saving would have been JSON decode, which did not
// justify the staleness risk.
func (store *FilesystemStore) ReadManifest(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Manifest, error) {
	err := ctx.Err()
	if err != nil {
		return contracts.Manifest{}, err
	}

	err = validateObjectDigest(digest)
	if err != nil {
		return contracts.Manifest{}, err
	}

	var manifest contracts.Manifest
	ok, err := store.metaGet(manifestKey(digest), &manifest)
	if err != nil {
		return contracts.Manifest{}, err
	}
	if !ok {
		return contracts.Manifest{}, fmt.Errorf(
			"read manifest %s: %w",
			digest,
			os.ErrNotExist,
		)
	}

	return manifest, nil
}

func (store *FilesystemStore) GetStructure(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Structure, error) {
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		return contracts.Structure{}, err
	}

	structure := contracts.Structure{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		ObjectID:      manifest.ObjectID,
		Facets:        facetKinds(manifest.Facets),
		PartsByRole:   map[string][]contracts.StructurePart{},
	}
	for _, part := range manifest.Compound.Parts {
		err := validateCompoundPart(part)
		if err != nil {
			return contracts.Structure{}, fmt.Errorf(
				"invalid compound part in %s: %w",
				digest,
				err,
			)
		}

		structurePart := contracts.StructurePart{
			Digest:   part.Digest,
			Role:     part.Role,
			Order:    part.Order,
			Required: part.Required,
			Metadata: cloneMap(part.Metadata),
		}
		if child, childErr := store.ReadManifest(ctx, part.Digest); childErr == nil {
			structurePart.Facets = facetKinds(child.Facets)
		}

		structure.PartsByRole[part.Role] = append(
			structure.PartsByRole[part.Role],
			structurePart,
		)
	}

	for role := range structure.PartsByRole {
		sort.SliceStable(structure.PartsByRole[role], func(left, right int) bool {
			return structure.PartsByRole[role][left].Order < structure.PartsByRole[role][right].Order
		})
	}

	return structure, nil
}

func (store *FilesystemStore) HasAnalysisAnnotation(
	ctx context.Context,
	digest contracts.ObjectDigest,
	analyzerName string,
	analyzerVersion string,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateObjectDigest(digest); err != nil {
		return false, err
	}

	if strings.TrimSpace(analyzerName) == "" {
		return false, errors.New("analysis analyzer name is required")
	}

	annotation, ok, err := store.readPackedAnnotation(digest, "analysis", analyzerName)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	return annotation.Kind == "analysis" &&
			annotation.AnalyzerName == analyzerName &&
			annotation.AnalyzerVer == analyzerVersion &&
			analysisAnnotationSatisfied(annotation),
		nil
}

func (store *FilesystemStore) WriteAnnotation(
	ctx context.Context,
	annotation contracts.Annotation,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if strings.TrimSpace(string(annotation.ObjectDigest)) == "" {
		return errors.New("annotation object digest is required")
	}

	if err := validateObjectDigest(annotation.ObjectDigest); err != nil {
		return err
	}

	if _, err := store.ReadManifest(ctx, annotation.ObjectDigest); err != nil {
		return fmt.Errorf("read annotation target manifest: %w", err)
	}

	// annotationFilename validates the kind (charset + reserved kinds); the
	// returned name is unused now that annotations are packed by digest.
	if _, err := annotationFilename(annotation); err != nil {
		return err
	}

	annotation.SchemaVersion = contracts.SchemaVersionPhase00
	if annotation.GeneratedAt.IsZero() {
		annotation.GeneratedAt = time.Now().UTC()
	}

	if existing, ok, err := store.readPackedAnnotation(
		annotation.ObjectDigest,
		annotation.Kind,
		annotation.AnalyzerName,
	); err != nil {
		return fmt.Errorf("read existing annotation: %w", err)
	} else if ok {
		annotation = mergeAnnotation(existing, annotation)
	}

	if err := store.writePackedAnnotation(ctx, annotation); err != nil {
		return err
	}

	if annotation.Kind == "analysis" {
		return store.refreshIndexedParentsForSubobject(ctx, annotation.ObjectDigest)
	}

	return nil
}

func (store *FilesystemStore) WriteOverlays(
	ctx context.Context,
	digest contracts.ObjectDigest,
	overlays map[string]any,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := validateObjectDigest(digest); err != nil {
		return err
	}

	if overlays == nil {
		overlays = map[string]any{}
	}

	unlock := store.lockKey("manifest:" + string(digest))
	defer unlock()

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		return fmt.Errorf("read overlay target manifest: %w", err)
	}

	manifest.Overlays = mergeMaps(manifest.Overlays, overlays)

	manifest.UpdatedAt = time.Now().UTC()
	if err := store.writeManifest(digest, manifest); err != nil {
		return err
	}

	return store.writePackedAnnotation(
		ctx,
		contracts.Annotation{
			SchemaVersion: contracts.SchemaVersionPhase00,
			ObjectDigest:  digest,
			Kind:          "overlays",
			GeneratedAt:   manifest.UpdatedAt,
			Data:          manifest.Overlays,
		},
	)
}

func (store *FilesystemStore) WriteSourceCursor(
	ctx context.Context,
	cursor contracts.SourceCursor,
) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	if strings.TrimSpace(cursor.SourceKind) == "" {
		return errors.New("source cursor kind is required")
	}

	if strings.TrimSpace(cursor.SourceName) == "" {
		return errors.New("source cursor name is required")
	}

	cursor.SchemaVersion = contracts.SchemaVersionPhase00
	if cursor.UpdatedAt.IsZero() {
		cursor.UpdatedAt = time.Now().UTC()
	}

	if cursor.Cursor == nil {
		cursor.Cursor = map[string]any{}
	}

	return store.writeCompressedJSON(store.sourceCursorPath(cursor), cursor)
}

func (store *FilesystemStore) ReadSourceCursor(
	ctx context.Context,
	ref contracts.SourceCursorRef,
) (contracts.SourceCursor, bool, error) {
	if err := ctx.Err(); err != nil {
		return contracts.SourceCursor{}, false, err
	}
	if strings.TrimSpace(ref.SourceKind) == "" {
		return contracts.SourceCursor{}, false, errors.New("source cursor kind is required")
	}
	if strings.TrimSpace(ref.SourceName) == "" {
		return contracts.SourceCursor{}, false, errors.New("source cursor name is required")
	}

	cursor := contracts.SourceCursor{
		SourceKind: ref.SourceKind,
		SourceName: ref.SourceName,
	}
	path := store.sourceCursorPath(cursor)
	var stored contracts.SourceCursor
	if err := store.readCompressedJSON(path, &stored); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return contracts.SourceCursor{}, false, nil
		}

		return contracts.SourceCursor{}, false, err
	}

	return stored, true, nil
}

func (store *FilesystemStore) WalkSourceCursors(
	ctx context.Context,
	fn SourceCursorProjectionFunc,
) error {
	if fn == nil {
		return errors.New("source cursor projection callback is required")
	}

	base := filepath.Join(store.root, "source-cursors")

	err := filepath.WalkDir(
		base,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) && path == base {
					return nil
				}

				return walkErr
			}

			err := ctx.Err()
			if err != nil {
				return err
			}

			if entry.IsDir() || entry.Name() != sourceCursorFilename {
				return nil
			}

			var cursor contracts.SourceCursor
			err = store.readCompressedJSON(path, &cursor)
			if err != nil {
				return err
			}

			return fn(cursor)
		},
	)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

func (store *FilesystemStore) writeObject(
	ctx context.Context,
	cb *commitBatch,
	digest contracts.ObjectDigest,
	content []byte,
	sourceHint string,
	incoming contracts.Manifest,
) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	manifest := incoming
	if existing, err := store.ReadManifest(ctx, digest); err == nil {
		manifest = mergeManifest(existing, incoming)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing manifest: %w", err)
	}

	// Content lives in the content-addressed chunk store; its recipe is the
	// authoritative "object content exists" signal.
	blobExists, err := store.hasRecipe(digest)
	if err != nil {
		return fmt.Errorf("check object recipe: %w", err)
	}

	if !blobExists && manifest.IdentityStrategy == identityStrategyCompoundStableID {
		content, err = compoundEnvelopeBytes(manifest)
		if err != nil {
			return err
		}

		manifest.Size = int64(len(content))
	}

	if !blobExists {
		return store.commitNewObject(ctx, cb, digest, content, sourceHint, manifest)
	}

	return store.writeManifestTo(cb, digest, manifest)
}

// commitNewObject stores content in the content-addressed chunk store, records
// the manifest and recovery sidecar in the metadata LSM, and creates no
// per-object directory. The recipe is written before the manifest so a crash
// can only ever leave orphan chunks (GC-reclaimable), never a manifest that
// points at missing content.
func (store *FilesystemStore) commitNewObject(
	ctx context.Context,
	cb *commitBatch,
	digest contracts.ObjectDigest,
	content []byte,
	sourceHint string,
	manifest contracts.Manifest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := store.storeBlobContentBatched(
		ctx,
		cb,
		digest,
		content,
		manifest.MediaType,
		manifest.ContentRoles,
	); err != nil {
		return fmt.Errorf("store blob content: %w", err)
	}

	if err := store.writeManifestTo(cb, digest, manifest); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Content lives in the chunk store, so there is no single compressed blob;
	// the recovery sidecar records uncompressed identity only (compressed=nil).
	if err := store.writePackedRecoveryTo(
		ctx,
		cb,
		digest,
		recoverySidecarFor(digest, content, nil, sourceHint, manifest),
	); err != nil {
		return fmt.Errorf("write packed recovery sidecar: %w", err)
	}

	return nil
}

func (store *FilesystemStore) baseManifest(
	digest contracts.ObjectDigest,
	objectID string,
	identityStrategy string,
	mediaType string,
	size int64,
	contentRoles []string,
	facets []contracts.Facet,
	provenance []contracts.Provenance,
	relationships []contracts.Relationship,
	compound contracts.Compound,
) contracts.Manifest {
	now := time.Now().UTC()

	return contracts.Manifest{
		SchemaVersion:    contracts.SchemaVersionPhase00,
		ObjectDigest:     digest,
		ObjectID:         objectID,
		IdentityStrategy: identityStrategy,
		MediaType:        firstNonEmpty(mediaType, "application/octet-stream"),
		Size:             size,
		Compression:      compressionZstd,
		ContentRoles:     uniqueStrings(contentRoles),
		Facets:           normalizeFacets(facets),
		Provenance:       normalizeProvenance(provenance),
		Relationships:    normalizedRelationships(relationships),
		Compound:         compound,
		Analysis:         map[string]any{},
		Overlays:         map[string]any{},
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func (store *FilesystemStore) readBlob(digest contracts.ObjectDigest) ([]byte, error) {
	if content, ok, err := store.readBlobContent(digest); err != nil {
		return nil, err
	} else if ok {
		return content, nil
	}

	// Fallback for streamed/legacy objects stored as a whole compressed blob.
	return readZstdFile(store.objectPath(digest, blobFilename))
}

func (store *FilesystemStore) writeCompressedJSON(path string, value any) error {
	encoded, err := canonicalJSON(value)
	if err != nil {
		return err
	}

	compressed, err := compressZstd(encoded)
	if err != nil {
		return err
	}

	return atomicWriteFile(path, compressed, 0o640)
}

func (store *FilesystemStore) readCompressedJSON(path string, value any) error {
	encoded, err := readZstdFile(path)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(encoded, value); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}

	return nil
}

func (store *FilesystemStore) objectPath(
	digest contracts.ObjectDigest,
	name string,
) string {
	return filepath.Join(store.objectDir(digest), name)
}

func (store *FilesystemStore) objectDir(digest contracts.ObjectDigest) string {
	value := string(digest)

	return filepath.Join(store.root, "objects", "blake3", value[:2], value[2:4], value)
}

func (store *FilesystemStore) sourceObjectLockPath(
	ref contracts.SourceObjectRef,
) string {
	key := sourceObjectRefKey(ref)

	return filepath.Join(store.root, "source-locks", key[:2], key[2:4], key+".json")
}

func (store *FilesystemStore) sourceObjectIndexPath(
	ref contracts.SourceObjectRef,
) string {
	key := sourceObjectRefKey(ref)

	return filepath.Join(store.root, "source-index", key[:2], key[2:4], key+".json")
}

func (store *FilesystemStore) compoundParentIndexPath(
	childDigest contracts.ObjectDigest,
) string {
	value := string(childDigest)

	return filepath.Join(
		store.root,
		"compound-parent-index",
		"blake3",
		value[:2],
		value[2:4],
		value+".json",
	)
}

func (store *FilesystemStore) sourceCursorPath(
	cursor contracts.SourceCursor,
) string {
	return filepath.Join(
		store.root,
		"source-cursors",
		safePathComponent(cursor.SourceKind),
		safePathComponent(cursor.SourceName),
		sourceCursorFilename,
	)
}

type compoundEnvelope struct {
	SchemaVersion    int                      `json:"schema_version"`
	ObjectID         string                   `json:"object_id"`
	IdentityStrategy string                   `json:"identity_strategy"`
	Provenance       []contracts.Provenance   `json:"provenance,omitempty"`
	Parts            []contracts.CompoundPart `json:"parts"`
}

func compoundEnvelopeBytes(manifest contracts.Manifest) ([]byte, error) {
	return canonicalJSON(compoundEnvelope{
		SchemaVersion:    int(contracts.SchemaVersionPhase00),
		ObjectID:         manifest.ObjectID,
		IdentityStrategy: manifest.IdentityStrategy,
		Provenance:       manifest.Provenance,
		Parts:            manifest.Compound.Parts,
	})
}

type recoverySidecar struct {
	SchemaVersion       string         `json:"schema_version"`
	Digest              string         `json:"digest"`
	ObjectID            string         `json:"object_id"`
	IdentityStrategy    string         `json:"identity_strategy"`
	SourceHint          string         `json:"source_hint,omitempty"`
	MediaType           string         `json:"media_type,omitempty"`
	UncompressedSize    int64          `json:"uncompressed_size"`
	CompressedSize      int64          `json:"compressed_size"`
	UncompressedBlake3  string         `json:"uncompressed_blake3"`
	UncompressedSHA256  string         `json:"uncompressed_sha256"`
	CompressedBlake3    string         `json:"compressed_blake3"`
	CompressedSHA256    string         `json:"compressed_sha256"`
	Compression         string         `json:"compression"`
	CreatedAt           time.Time      `json:"created_at"`
	FilestoreVersion    string         `json:"filestore_version"`
	ManifestSourceHints map[string]any `json:"manifest_source_hints,omitempty"`
}

func recoverySidecarFor(
	digest contracts.ObjectDigest,
	content []byte,
	compressed []byte,
	sourceHint string,
	manifest contracts.Manifest,
) recoverySidecar {
	return recoverySidecar{
		SchemaVersion:      schemaVersion,
		Digest:             string(digest),
		ObjectID:           manifest.ObjectID,
		IdentityStrategy:   manifest.IdentityStrategy,
		SourceHint:         strings.TrimSpace(sourceHint),
		MediaType:          manifest.MediaType,
		UncompressedSize:   int64(len(content)),
		CompressedSize:     int64(len(compressed)),
		UncompressedBlake3: blake3Hex(content),
		UncompressedSHA256: sha256Hex(content),
		CompressedBlake3:   blake3Hex(compressed),
		CompressedSHA256:   sha256Hex(compressed),
		Compression:        compressionZstd,
		CreatedAt:          manifest.CreatedAt,
		FilestoreVersion:   schemaVersion,
	}
}

// recoverySidecarForStreamed builds a recovery sidecar from identity hashes
// computed while streaming, without holding the content in memory. Chunked
// objects have no single compressed blob, so the compressed fields carry the
// empty-input hashes (matching recoverySidecarFor called with compressed=nil)
// and verify skips them.
func recoverySidecarForStreamed(
	digest contracts.ObjectDigest,
	size int64,
	uncompressedBlake3 string,
	uncompressedSHA256 string,
	sourceHint string,
	manifest contracts.Manifest,
) recoverySidecar {
	return recoverySidecar{
		SchemaVersion:      schemaVersion,
		Digest:             string(digest),
		ObjectID:           manifest.ObjectID,
		IdentityStrategy:   manifest.IdentityStrategy,
		SourceHint:         strings.TrimSpace(sourceHint),
		MediaType:          manifest.MediaType,
		UncompressedSize:   size,
		CompressedSize:     0,
		UncompressedBlake3: uncompressedBlake3,
		UncompressedSHA256: uncompressedSHA256,
		CompressedBlake3:   blake3Hex(nil),
		CompressedSHA256:   sha256Hex(nil),
		Compression:        compressionZstd,
		CreatedAt:          manifest.CreatedAt,
		FilestoreVersion:   schemaVersion,
	}
}

func validateFacets(facets []contracts.Facet) error {
	if len(facets) == 0 {
		return errors.New("at least one facet is required")
	}

	for _, facet := range facets {
		if strings.TrimSpace(facet.FacetKind()) == "" {
			return errors.New("facet kind is required")
		}
	}

	return nil
}

func validateCompoundParts(parts []contracts.CompoundPart) error {
	for _, part := range parts {
		err := validateCompoundPart(part)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateCompoundPart(part contracts.CompoundPart) error {
	if !looksLikeDigest(string(part.Digest)) {
		return fmt.Errorf(
			"compound part digest %q must be a 64-character hex digest",
			part.Digest,
		)
	}

	if strings.TrimSpace(part.Role) == "" {
		return fmt.Errorf("compound part %s role is required", part.Digest)
	}

	return nil
}

func normalizeFacets(facets []contracts.Facet) []contracts.Facet {
	seen := map[string]bool{}

	normalized := make([]contracts.Facet, 0, len(facets))
	for _, facet := range facets {
		kind := strings.TrimSpace(facet.FacetKind())
		if kind == "" || seen[kind] {
			continue
		}

		seen[kind] = true
		facet.Kind = kind

		facet.Name = ""
		if facet.Metadata == nil {
			facet.Metadata = facet.Attributes
		} else {
			facet.Metadata = mergeMaps(facet.Attributes, facet.Metadata)
		}

		facet.Attributes = nil
		normalized = append(normalized, facet)
	}

	sort.SliceStable(normalized, func(left, right int) bool {
		return normalized[left].Kind < normalized[right].Kind
	})

	return normalized
}

func normalizeProvenance(provenance []contracts.Provenance) []contracts.Provenance {
	result := append([]contracts.Provenance(nil), provenance...)
	sort.SliceStable(result, func(left, right int) bool {
		if result[left].SourceKind != result[right].SourceKind {
			return result[left].SourceKind < result[right].SourceKind
		}

		if result[left].SourceName != result[right].SourceName {
			return result[left].SourceName < result[right].SourceName
		}

		if result[left].ExternalID != result[right].ExternalID {
			return result[left].ExternalID < result[right].ExternalID
		}

		return result[left].ExternalVersion < result[right].ExternalVersion
	})

	return result
}

func normalizedRelationships(
	relationships []contracts.Relationship,
) []contracts.Relationship {
	result := append([]contracts.Relationship(nil), relationships...)
	sort.SliceStable(result, func(left, right int) bool {
		if result[left].Type != result[right].Type {
			return result[left].Type < result[right].Type
		}

		if result[left].From != result[right].From {
			return result[left].From < result[right].From
		}

		return result[left].To < result[right].To
	})

	return result
}

func relationshipsWithCompoundParts(
	compoundDigest contracts.ObjectDigest,
	relationships []contracts.Relationship,
	parts []contracts.CompoundPart,
) []contracts.Relationship {
	result := append([]contracts.Relationship(nil), relationships...)
	for _, part := range parts {
		result = append(
			result,
			contracts.Relationship{
				Type:   "contains",
				From:   compoundDigest,
				To:     part.Digest,
				Role:   part.Role,
				Order:  part.Order,
				Source: "filestore.compound",
			},
			contracts.Relationship{
				Type:   "part_of",
				From:   part.Digest,
				To:     compoundDigest,
				Role:   part.Role,
				Order:  part.Order,
				Source: "filestore.compound",
			},
		)
	}

	return result
}

func normalizedParts(parts []contracts.CompoundPart) []contracts.CompoundPart {
	result := append([]contracts.CompoundPart(nil), parts...)
	sort.SliceStable(result, func(left, right int) bool {
		if result[left].Order != result[right].Order {
			return result[left].Order < result[right].Order
		}

		if result[left].Role != result[right].Role {
			return result[left].Role < result[right].Role
		}

		return result[left].Digest < result[right].Digest
	})

	return result
}

func mergeManifest(existing, incoming contracts.Manifest) contracts.Manifest {
	merged := existing
	merged.UpdatedAt = time.Now().UTC()
	merged.ContentRoles = uniqueStrings(
		append(merged.ContentRoles, incoming.ContentRoles...),
	)
	merged.Facets = mergeFacets(merged.Facets, incoming.Facets)
	merged.Provenance = mergeProvenance(merged.Provenance, incoming.Provenance)
	merged.Relationships = mergeRelationships(merged.Relationships, incoming.Relationships)
	merged.Compound.Parts = mergeParts(merged.Compound.Parts, incoming.Compound.Parts)

	merged.Compound.IsCompound = merged.Compound.IsCompound || incoming.Compound.IsCompound
	if merged.Analysis == nil {
		merged.Analysis = map[string]any{}
	}

	if merged.Overlays == nil {
		merged.Overlays = map[string]any{}
	}

	return merged
}

func mergeAnnotation(
	existing contracts.Annotation,
	incoming contracts.Annotation,
) contracts.Annotation {
	merged := existing
	merged.SchemaVersion = contracts.SchemaVersionPhase00
	merged.ObjectDigest = incoming.ObjectDigest
	merged.Kind = incoming.Kind
	merged.AnalyzerName = firstNonEmpty(incoming.AnalyzerName, existing.AnalyzerName)

	merged.AnalyzerVer = firstNonEmpty(incoming.AnalyzerVer, existing.AnalyzerVer)
	if !incoming.GeneratedAt.IsZero() {
		merged.GeneratedAt = incoming.GeneratedAt
	}

	merged.Data = mergeMaps(existing.Data, incoming.Data)

	return merged
}

func analysisAnnotationSatisfied(annotation contracts.Annotation) bool {
	status, _ := annotation.Data["status"].(string)

	switch status {
	case "complete", "skipped":
		return true
	default:
		return false
	}
}

func mergeMaps(existing, incoming map[string]any) map[string]any {
	merged := map[string]any{}
	maps.Copy(merged, existing)

	maps.Copy(merged, incoming)

	return merged
}

func mergeFacets(existing, incoming []contracts.Facet) []contracts.Facet {
	result := make([]contracts.Facet, 0, len(existing)+len(incoming))
	indexByKind := map[string]int{}

	for _, item := range existing {
		kind := strings.TrimSpace(item.FacetKind())
		if kind != "" {
			indexByKind[kind] = len(result)
		}
		result = append(result, item)
	}

	for _, item := range incoming {
		kind := strings.TrimSpace(item.FacetKind())
		if index, ok := indexByKind[kind]; ok && kind != "" {
			result[index] = mergeFacet(result[index], item)

			continue
		}

		result = append(result, item)
		if kind != "" {
			indexByKind[kind] = len(result) - 1
		}
	}

	return normalizeFacets(result)
}

func mergeFacet(existing, incoming contracts.Facet) contracts.Facet {
	merged := existing
	if incoming.Kind != "" {
		merged.Kind = incoming.Kind
	}

	if incoming.Name != "" {
		merged.Name = incoming.Name
	}

	if incoming.Version != "" {
		merged.Version = incoming.Version
	}

	existingMetadata := mergeMaps(existing.Attributes, existing.Metadata)
	incomingMetadata := mergeMaps(incoming.Attributes, incoming.Metadata)
	merged.Metadata = mergeFacetMetadata(
		merged.FacetKind(),
		existingMetadata,
		incomingMetadata,
	)
	merged.Attributes = nil

	return merged
}

func mergeFacetMetadata(
	kind string,
	existingMetadata map[string]any,
	incomingMetadata map[string]any,
) map[string]any {
	merged := mergeMaps(existingMetadata, incomingMetadata)
	if kind != contracts.MailMessageFacetKind {
		return merged
	}

	existingVersionCount := intFromAny(existingMetadata["version_count"])
	incomingVersionCount := intFromAny(incomingMetadata["version_count"])
	merged["version_count"] = max(existingVersionCount, incomingVersionCount)
	merged["max_scale"] = maxMailVersionScale(
		stringFromAny(existingMetadata["max_scale"]),
		stringFromAny(incomingMetadata["max_scale"]),
	)

	merged["message_id_collision"] = boolFromAny(
		existingMetadata["message_id_collision"],
	) || boolFromAny(incomingMetadata["message_id_collision"])
	if existingVersionCount > incomingVersionCount {
		if existingCanonical := stringFromAny(
			existingMetadata["canonical_version_id"],
		); existingCanonical != "" {
			merged["canonical_version_id"] = existingCanonical
		}
	}

	return merged
}

func maxMailVersionScale(left, right string) string {
	scaleRank := map[string]int{
		contracts.VersionScaleTrivial: versionScaleRankTrivial,
		contracts.VersionScaleMinor:   versionScaleRankMinor,
		contracts.VersionScaleMajor:   versionScaleRankMajor,
	}
	if scaleRank[right] > scaleRank[left] {
		return right
	}

	if left != "" {
		return left
	}

	return right
}

func provenanceMergeKey(item contracts.Provenance) string {
	return strings.Join([]string{
		item.SourceKind,
		item.SourceName,
		item.ExternalID,
		item.ExternalVersion,
	}, "\x00")
}

// provenanceAddsNew reports whether any incoming provenance carries a
// source/name/external-id/version key not already present in existing. It is the
// "did re-observing this object actually change anything?" test: when it is
// false, re-attaching provenance is a pure no-op.
func provenanceAddsNew(existing, incoming []contracts.Provenance) bool {
	existingKeys := make(map[string]bool, len(existing))
	for _, item := range existing {
		existingKeys[provenanceMergeKey(item)] = true
	}

	for _, item := range incoming {
		if !existingKeys[provenanceMergeKey(item)] {
			return true
		}
	}

	return false
}

func mergeProvenance(existing, incoming []contracts.Provenance) []contracts.Provenance {
	seen := map[string]bool{}

	result := make([]contracts.Provenance, 0, len(existing)+len(incoming))
	for _, item := range append(existing, incoming...) {
		key := provenanceMergeKey(item)
		if seen[key] {
			continue
		}

		seen[key] = true

		result = append(result, item)
	}

	return normalizeProvenance(result)
}

func mergeRelationships(
	existing, incoming []contracts.Relationship,
) []contracts.Relationship {
	incomingKeys := map[string]bool{}
	for _, item := range incoming {
		incomingKeys[relationshipMergeKey(item)] = true
	}

	result := make([]contracts.Relationship, 0, len(existing)+len(incoming))
	for _, item := range existing {
		if incomingKeys[relationshipMergeKey(item)] {
			continue
		}

		result = append(result, item)
	}
	result = append(result, incoming...)

	return normalizedRelationships(result)
}

func mergeParts(existing, incoming []contracts.CompoundPart) []contracts.CompoundPart {
	incomingSlots := map[string]bool{}
	for _, item := range incoming {
		incomingSlots[compoundPartSlotKey(item)] = true
	}

	result := make([]contracts.CompoundPart, 0, len(existing)+len(incoming))
	for _, item := range existing {
		if incomingSlots[compoundPartSlotKey(item)] {
			continue
		}

		result = append(result, item)
	}
	result = append(result, incoming...)

	return normalizedParts(result)
}

func relationshipMergeKey(item contracts.Relationship) string {
	switch item.Type {
	case "contains":
		return fmt.Sprintf(
			"%s\x00%s\x00%s\x00%d",
			item.Type,
			item.From,
			item.Role,
			item.Order,
		)
	case "part_of":
		return fmt.Sprintf(
			"%s\x00%s\x00%s\x00%d",
			item.Type,
			item.To,
			item.Role,
			item.Order,
		)
	default:
		return fmt.Sprintf(
			"%s\x00%s\x00%s\x00%s\x00%d",
			item.Type,
			item.From,
			item.To,
			item.Role,
			item.Order,
		)
	}
}

func compoundPartSlotKey(item contracts.CompoundPart) string {
	return fmt.Sprintf("%s\x00%d", item.Role, item.Order)
}

func annotationFilename(annotation contracts.Annotation) (string, error) {
	kind := annotation.Kind

	trimmed := strings.TrimSpace(kind)
	if trimmed == "" {
		return "", errors.New("annotation kind is required")
	}

	if isReservedAnnotationKind(trimmed) {
		return "", fmt.Errorf("annotation kind %q is reserved", kind)
	}

	for _, char := range trimmed {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_' ||
			char == '-' {
			continue
		}

		return "", fmt.Errorf("invalid annotation kind %q", kind)
	}

	if trimmed == "analysis" && strings.TrimSpace(annotation.AnalyzerName) != "" {
		return analysisAnnotationFilename(annotation.AnalyzerName)
	}

	return trimmed + ".json.zst", nil
}

func analysisAnnotationFilename(analyzerName string) (string, error) {
	trimmed := strings.TrimSpace(analyzerName)
	if trimmed == "" {
		return "", errors.New("analysis analyzer name is required")
	}

	sum := sha256.Sum256([]byte(trimmed))

	return "analysis-" + hex.EncodeToString(sum[:]) + ".json.zst", nil
}

func isReservedAnnotationKind(kind string) bool {
	switch strings.ToLower(kind) {
	case "blob", "recovery", "manifest":
		return true
	default:
		return false
	}
}

func compressZstd(content []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}

	return encoder.EncodeAll(content, nil), nil
}

func readZstdFile(path string) ([]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return decompressZstd(path, content)
}

func decompressZstd(path string, content []byte) ([]byte, error) {
	if content == nil {
		return nil, fmt.Errorf("read %s: empty compressed content", path)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	decoded, err := decoder.DecodeAll(content, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress %s: %w", path, err)
	}

	return decoded, nil
}

func atomicWriteFile(path string, content []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}

	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".")
	if err != nil {
		return err
	}

	tmpPath := file.Name()
	cleanup := true

	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := file.Write(content); err != nil {
		_ = file.Close()

		return err
	}

	if err := file.Chmod(perm); err != nil {
		_ = file.Close()

		return err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()

		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}

	cleanup = false

	return fsyncDir(filepath.Dir(path))
}

func fsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()

	return dir.Sync()
}

func canonicalJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer

	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	err := encoder.Encode(value)
	if err != nil {
		return nil, err
	}

	return bytes.TrimSpace(buffer.Bytes()), nil
}

func readJSON(path string, value any) error {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(encoded, value); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}

	return nil
}

func blake3Hex(content []byte) string {
	sum := blake3.Sum256(content)

	return hex.EncodeToString(sum[:])
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)

	return hex.EncodeToString(sum[:])
}

func looksLikeDigest(value string) bool {
	if len(value) != 64 {
		return false
	}

	_, err := hex.DecodeString(value)

	return err == nil
}

func validateObjectDigest(digest contracts.ObjectDigest) error {
	if !looksLikeDigest(string(digest)) {
		return fmt.Errorf("invalid object digest %q", digest)
	}

	return nil
}

func facetKinds(facets []contracts.Facet) []string {
	kinds := make([]string, 0, len(facets))
	for _, facet := range facets {
		kind := facet.FacetKind()
		if kind != "" {
			kinds = append(kinds, kind)
		}
	}

	return uniqueStrings(kinds)
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}

	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || seen[trimmed] {
			continue
		}

		seen[trimmed] = true
		result = append(result, trimmed)
	}

	sort.Strings(result)

	return result
}

func safePathComponent(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", "\x00", "_")

	return replacer.Replace(strings.TrimSpace(value))
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}

	clone := make(map[string]any, len(value))
	maps.Copy(clone, value)

	return clone
}

func projectionObjectChangedAfter(object ProjectionObject, since time.Time) bool {
	if since.IsZero() {
		return true
	}

	if object.Manifest.UpdatedAt.After(since) || object.Manifest.CreatedAt.After(since) {
		return true
	}

	for _, annotation := range object.Annotations {
		if annotation.GeneratedAt.After(since) {
			return true
		}
	}

	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}

	return ""
}

func stringFromAny(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}

	return ""
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func boolFromAny(value any) bool {
	if typed, ok := value.(bool); ok {
		return typed
	}

	return false
}
