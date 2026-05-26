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
	"hash"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/blake3"

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
)

var errStopWalk = errors.New("stop filestore walk")

type FilesystemStore struct {
	root string
}

func NewFilesystemStore(root string) *FilesystemStore {
	return &FilesystemStore{root: root}
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

	blob, err := store.streamObjectBlob(ctx, request.Reader)
	if err != nil {
		return "", err
	}

	cleanup := true

	defer func() {
		if cleanup {
			_ = os.RemoveAll(blob.stageDir)
		}
	}()

	digest := contracts.ObjectDigest(blob.uncompressedBlake3)

	manifest := store.baseManifest(
		digest,
		string(digest),
		identityStrategyFileBlake3,
		request.MediaType,
		blob.uncompressedSize,
		request.ContentRoles,
		request.Facets,
		request.Provenance,
		request.Relationships,
		contracts.Compound{IsCompound: false},
	)
	if err := store.commitStreamedObject(
		ctx,
		digest,
		blob,
		request.SourceHint,
		manifest,
	); err != nil {
		return "", err
	}

	cleanup = false

	return digest, nil
}

func (store *FilesystemStore) AttachProvenance(
	ctx context.Context,
	digest contracts.ObjectDigest,
	provenance []contracts.Provenance,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := validateObjectDigest(digest); err != nil {
		return err
	}

	if len(provenance) == 0 {
		return errors.New("provenance is required")
	}

	for _, item := range provenance {
		err := validateSourceObjectRef(sourceObjectRefFromProvenance(item))
		if err != nil {
			return err
		}
	}

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		return fmt.Errorf("read provenance target manifest: %w", err)
	}

	manifest.Provenance = mergeProvenance(manifest.Provenance, provenance)
	manifest.UpdatedAt = time.Now().UTC()

	return store.writeCompressedJSON(
		store.objectPath(digest, manifestFilename),
		manifest,
	)
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
	if err := store.writeObject(
		ctx,
		digest,
		envelope,
		request.SourceHint,
		manifest,
	); err != nil {
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

	content, err := store.readBlob(digest)
	if err != nil {
		return nil, err
	}

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if manifest.IdentityStrategy != identityStrategyCompoundStableID &&
		blake3Hex(content) != string(digest) {
		actual := blake3Hex(content)

		return nil, fmt.Errorf("CAS digest mismatch for %s: got %s", digest, actual)
	}

	return io.NopCloser(bytes.NewReader(content)), nil
}

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
	err = store.readCompressedJSON(
		store.objectPath(digest, manifestFilename),
		&manifest,
	)
	if err != nil {
		return contracts.Manifest{}, err
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

	name, err := annotationFilename(annotation)
	if err != nil {
		return err
	}

	annotation.SchemaVersion = contracts.SchemaVersionPhase00
	if annotation.GeneratedAt.IsZero() {
		annotation.GeneratedAt = time.Now().UTC()
	}

	annotationPath := store.objectPath(annotation.ObjectDigest, name)
	if existing, err := store.readAnnotation(annotationPath); err == nil {
		annotation = mergeAnnotation(existing, annotation)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing annotation: %w", err)
	}

	if err := store.writeCompressedJSON(
		annotationPath,
		annotation,
	); err != nil {
		return err
	}

	if annotation.Kind == "analysis" {
		return store.refreshParentsForSubobject(ctx, annotation.ObjectDigest)
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

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		return fmt.Errorf("read overlay target manifest: %w", err)
	}

	manifest.Overlays = mergeMaps(manifest.Overlays, overlays)

	manifest.UpdatedAt = time.Now().UTC()
	if err := store.writeCompressedJSON(
		store.objectPath(digest, manifestFilename),
		manifest,
	); err != nil {
		return err
	}

	return store.writeCompressedJSON(
		store.objectPath(digest, "overlays.json.zst"),
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

func (store *FilesystemStore) refreshParentsForSubobject(
	ctx context.Context,
	childDigest contracts.ObjectDigest,
) error {
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

	return store.WalkProjection(ctx, func(object ProjectionObject) error {
		if len(object.Findings) > 0 || !manifestContainsPart(object.Manifest, childDigest) {
			return nil
		}

		manifest := object.Manifest
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
			store.objectPath(manifest.ObjectDigest, manifestFilename),
			manifest,
		)
	})
}

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

			err := ctx.Err()
			if err != nil {
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

			err = fn(object)
			if err != nil {
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

func (store *FilesystemStore) writeObject(
	ctx context.Context,
	digest contracts.ObjectDigest,
	content []byte,
	sourceHint string,
	incoming contracts.Manifest,
) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	objectDir := store.objectDir(digest)
	blobPath := filepath.Join(objectDir, blobFilename)
	manifestPath := filepath.Join(objectDir, manifestFilename)

	manifest := incoming
	if existing, err := store.ReadManifest(ctx, digest); err == nil {
		manifest = mergeManifest(existing, incoming)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing manifest: %w", err)
	}

	blobExists := true
	if _, err := os.Stat(blobPath); errors.Is(err, os.ErrNotExist) {
		blobExists = false
	} else if err != nil {
		return fmt.Errorf("stat blob: %w", err)
	}

	if !blobExists && manifest.IdentityStrategy == identityStrategyCompoundStableID {
		var err error

		content, err = compoundEnvelopeBytes(manifest)
		if err != nil {
			return err
		}

		manifest.Size = int64(len(content))
	}

	if !blobExists {
		if exists, err := pathExists(objectDir); err != nil {
			return fmt.Errorf("stat object directory: %w", err)
		} else if exists {
			return fmt.Errorf("object %s is incomplete: missing immutable blob", digest)
		}

		return store.commitNewObjectDirectory(ctx, digest, content, sourceHint, manifest)
	}

	return store.writeCompressedJSON(manifestPath, manifest)
}

func (store *FilesystemStore) commitStreamedObject(
	ctx context.Context,
	digest contracts.ObjectDigest,
	blob streamedBlob,
	sourceHint string,
	incoming contracts.Manifest,
) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	objectDir := store.objectDir(digest)
	blobPath := filepath.Join(objectDir, blobFilename)
	manifestPath := filepath.Join(objectDir, manifestFilename)

	manifest := incoming
	if existing, err := store.ReadManifest(ctx, digest); err == nil {
		manifest = mergeManifest(existing, incoming)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing manifest: %w", err)
	}

	blobExists := true
	if _, err := os.Stat(blobPath); errors.Is(err, os.ErrNotExist) {
		blobExists = false
	} else if err != nil {
		return fmt.Errorf("stat blob: %w", err)
	}

	if blobExists {
		return store.writeCompressedJSON(manifestPath, manifest)
	}

	if exists, err := pathExists(objectDir); err != nil {
		return fmt.Errorf("stat object directory: %w", err)
	} else if exists {
		return fmt.Errorf("object %s is incomplete: missing immutable blob", digest)
	}

	return store.commitStreamedObjectDirectory(ctx, digest, blob, sourceHint, manifest)
}

func (store *FilesystemStore) commitNewObjectDirectory(
	ctx context.Context,
	digest contracts.ObjectDigest,
	content []byte,
	sourceHint string,
	manifest contracts.Manifest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	compressed, err := compressZstd(content)
	if err != nil {
		return err
	}

	objectDir := store.objectDir(digest)

	parentDir := filepath.Dir(objectDir)
	if err := os.MkdirAll(parentDir, 0o750); err != nil {
		return fmt.Errorf("create object parent directory: %w", err)
	}

	stageDir, err := os.MkdirTemp(parentDir, "."+string(digest)+".")
	if err != nil {
		return fmt.Errorf("create staged object directory: %w", err)
	}

	cleanup := true

	defer func() {
		if cleanup {
			_ = os.RemoveAll(stageDir)
		}
	}()

	if err := atomicWriteFile(
		filepath.Join(stageDir, blobFilename),
		compressed,
		0o640,
	); err != nil {
		return fmt.Errorf("stage blob: %w", err)
	}

	if err := atomicWriteJSON(
		filepath.Join(stageDir, recoveryFilename),
		recoverySidecarFor(digest, content, compressed, sourceHint, manifest),
	); err != nil {
		return fmt.Errorf("stage recovery sidecar: %w", err)
	}

	if err := store.writeCompressedJSON(
		filepath.Join(stageDir, manifestFilename),
		manifest,
	); err != nil {
		return fmt.Errorf("stage manifest: %w", err)
	}

	if err := fsyncDir(stageDir); err != nil {
		return fmt.Errorf("fsync staged object directory: %w", err)
	}

	if err := os.Rename(stageDir, objectDir); err != nil {
		return fmt.Errorf("commit object directory: %w", err)
	}

	cleanup = false

	return fsyncDir(parentDir)
}

func (store *FilesystemStore) commitStreamedObjectDirectory(
	ctx context.Context,
	digest contracts.ObjectDigest,
	blob streamedBlob,
	sourceHint string,
	manifest contracts.Manifest,
) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	objectDir := store.objectDir(digest)

	parentDir := filepath.Dir(objectDir)
	err = os.MkdirAll(parentDir, 0o750)
	if err != nil {
		return fmt.Errorf("create object parent directory: %w", err)
	}

	err = atomicWriteJSON(
		filepath.Join(blob.stageDir, recoveryFilename),
		recoverySidecarForStream(digest, blob, sourceHint, manifest),
	)
	if err != nil {
		return fmt.Errorf("stage recovery sidecar: %w", err)
	}

	err = store.writeCompressedJSON(
		filepath.Join(blob.stageDir, manifestFilename),
		manifest,
	)
	if err != nil {
		return fmt.Errorf("stage manifest: %w", err)
	}

	err = fsyncDir(blob.stageDir)
	if err != nil {
		return fmt.Errorf("fsync staged object directory: %w", err)
	}

	err = os.Rename(blob.stageDir, objectDir)
	if err != nil {
		if existing, readErr := store.ReadManifest(ctx, digest); readErr == nil {
			merged := mergeManifest(existing, manifest)

			return store.writeCompressedJSON(filepath.Join(objectDir, manifestFilename), merged)
		}

		return fmt.Errorf("commit object directory: %w", err)
	}

	return fsyncDir(parentDir)
}

type streamedBlob struct {
	stageDir           string
	uncompressedSize   int64
	compressedSize     int64
	uncompressedBlake3 string
	uncompressedSHA256 string
	compressedBlake3   string
	compressedSHA256   string
}

func (store *FilesystemStore) streamObjectBlob(
	ctx context.Context,
	reader io.Reader,
) (streamedBlob, error) {
	if err := ctx.Err(); err != nil {
		return streamedBlob{}, err
	}

	incomingDir := filepath.Join(store.root, ".incoming")
	if err := os.MkdirAll(incomingDir, 0o750); err != nil {
		return streamedBlob{}, fmt.Errorf("create incoming object directory: %w", err)
	}

	stageDir, err := os.MkdirTemp(incomingDir, "object.")
	if err != nil {
		return streamedBlob{}, fmt.Errorf("create incoming object stage: %w", err)
	}

	cleanup := true

	defer func() {
		if cleanup {
			_ = os.RemoveAll(stageDir)
		}
	}()

	blobPath := filepath.Join(stageDir, blobFilename)

	file, err := os.OpenFile(blobPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return streamedBlob{}, fmt.Errorf("open incoming blob: %w", err)
	}

	compressedHash := newHashingWriter(file)

	encoder, err := zstd.NewWriter(compressedHash)
	if err != nil {
		_ = file.Close()

		return streamedBlob{}, fmt.Errorf("create zstd stream: %w", err)
	}

	uncompressedHash := newHashingWriter(encoder)
	if _, err := io.Copy(uncompressedHash, reader); err != nil {
		encoder.Close()
		_ = file.Close()

		return streamedBlob{}, fmt.Errorf("stream object bytes: %w", err)
	}

	if err := encoder.Close(); err != nil {
		_ = file.Close()

		return streamedBlob{}, fmt.Errorf("finish zstd stream: %w", err)
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()

		return streamedBlob{}, fmt.Errorf("sync incoming blob: %w", err)
	}

	if err := file.Close(); err != nil {
		return streamedBlob{}, fmt.Errorf("close incoming blob: %w", err)
	}

	if err := fsyncDir(stageDir); err != nil {
		return streamedBlob{}, fmt.Errorf("fsync incoming object directory: %w", err)
	}

	if err := fsyncDir(incomingDir); err != nil {
		return streamedBlob{}, fmt.Errorf("fsync incoming parent directory: %w", err)
	}

	cleanup = false

	return streamedBlob{
		stageDir:           stageDir,
		uncompressedSize:   uncompressedHash.size,
		compressedSize:     compressedHash.size,
		uncompressedBlake3: uncompressedHash.blake3Hex(),
		uncompressedSHA256: uncompressedHash.sha256Hex(),
		compressedBlake3:   compressedHash.blake3Hex(),
		compressedSHA256:   compressedHash.sha256Hex(),
	}, nil
}

type hashingWriter struct {
	writer io.Writer
	blake3 hash.Hash
	sha256 hash.Hash
	size   int64
}

func newHashingWriter(writer io.Writer) *hashingWriter {
	return &hashingWriter{
		writer: writer,
		blake3: blake3.New(),
		sha256: sha256.New(),
	}
}

func (writer *hashingWriter) Write(content []byte) (int, error) {
	n, err := writer.writer.Write(content)
	if n > 0 {
		chunk := content[:n]
		writer.size += int64(n)
		_, _ = writer.blake3.Write(chunk)
		_, _ = writer.sha256.Write(chunk)
	}

	return n, err
}

func (writer *hashingWriter) blake3Hex() string {
	return hex.EncodeToString(writer.blake3.Sum(nil))
}

func (writer *hashingWriter) sha256Hex() string {
	return hex.EncodeToString(writer.sha256.Sum(nil))
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

func (store *FilesystemStore) readAnnotation(
	path string,
) (contracts.Annotation, error) {
	var annotation contracts.Annotation
	err := store.readCompressedJSON(path, &annotation)
	if err != nil {
		return contracts.Annotation{}, err
	}

	return annotation, nil
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

func recoverySidecarForStream(
	digest contracts.ObjectDigest,
	blob streamedBlob,
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
		UncompressedSize:   blob.uncompressedSize,
		CompressedSize:     blob.compressedSize,
		UncompressedBlake3: blob.uncompressedBlake3,
		UncompressedSHA256: blob.uncompressedSHA256,
		CompressedBlake3:   blob.compressedBlake3,
		CompressedSHA256:   blob.compressedSHA256,
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
	merged.Facets = normalizeFacets(append(merged.Facets, incoming.Facets...))
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

func mergeMaps(existing, incoming map[string]any) map[string]any {
	merged := map[string]any{}
	maps.Copy(merged, existing)

	maps.Copy(merged, incoming)

	return merged
}

func mergeProvenance(existing, incoming []contracts.Provenance) []contracts.Provenance {
	seen := map[string]bool{}

	result := make([]contracts.Provenance, 0, len(existing)+len(incoming))
	for _, item := range append(existing, incoming...) {
		key := strings.Join([]string{
			item.SourceKind,
			item.SourceName,
			item.ExternalID,
			item.ExternalVersion,
		}, "\x00")
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
	seen := map[string]bool{}

	result := make([]contracts.Relationship, 0, len(existing)+len(incoming))
	for _, item := range append(existing, incoming...) {
		key := fmt.Sprintf(
			"%s\x00%s\x00%s\x00%s\x00%d",
			item.Type,
			item.From,
			item.To,
			item.Role,
			item.Order,
		)
		if seen[key] {
			continue
		}

		seen[key] = true

		result = append(result, item)
	}

	return normalizedRelationships(result)
}

func mergeParts(existing, incoming []contracts.CompoundPart) []contracts.CompoundPart {
	seen := map[string]bool{}

	result := make([]contracts.CompoundPart, 0, len(existing)+len(incoming))
	for _, item := range append(existing, incoming...) {
		key := fmt.Sprintf("%s\x00%s\x00%d", item.Digest, item.Role, item.Order)
		if seen[key] {
			continue
		}

		seen[key] = true

		result = append(result, item)
	}

	return normalizedParts(result)
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
		sum := sha256.Sum256([]byte(annotation.AnalyzerName))

		return "analysis-" + hex.EncodeToString(sum[:]) + ".json.zst", nil
	}

	return trimmed + ".json.zst", nil
}

func isReservedAnnotationKind(kind string) bool {
	switch strings.ToLower(kind) {
	case "blob", "recovery", "manifest":
		return true
	default:
		return false
	}
}

func isProjectionAnnotationFilename(name string) bool {
	switch name {
	case blobFilename, recoveryFilename, manifestFilename:
		return false
	default:
		return strings.HasSuffix(name, ".json.zst")
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

func atomicWriteJSON(path string, value any) error {
	encoded, err := canonicalJSON(value)
	if err != nil {
		return err
	}

	return atomicWriteFile(path, encoded, 0o640)
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

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err
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
