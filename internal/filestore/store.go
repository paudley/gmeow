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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/blake3"

	"blackat.ca/gmeow/internal/contracts"
)

const (
	identityStrategyFileBlake3       = "file_blake3"
	identityStrategyCompoundStableID = "compound_stable_id"
	compressionZstd                  = "zstd"
	blobFilename                     = "blob.zst"
	recoveryFilename                 = "recovery.json"
	manifestFilename                 = "manifest.json.zst"
	schemaVersion                    = "1"
)

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
	content, err := io.ReadAll(request.Reader)
	if err != nil {
		return "", fmt.Errorf("read object bytes: %w", err)
	}
	digest := contracts.ObjectDigest(blake3Hex(content))
	manifest := store.baseManifest(
		digest,
		string(digest),
		identityStrategyFileBlake3,
		request.MediaType,
		int64(len(content)),
		request.ContentRoles,
		request.Facets,
		request.Provenance,
		request.Relationships,
		contracts.Compound{IsCompound: false},
	)
	if err := store.writeObject(
		ctx,
		digest,
		content,
		request.SourceHint,
		manifest,
	); err != nil {
		return "", err
	}
	return digest, nil
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
		request.Relationships,
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
	if err := ctx.Err(); err != nil {
		return contracts.Manifest{}, err
	}
	if err := validateObjectDigest(digest); err != nil {
		return contracts.Manifest{}, err
	}
	var manifest contracts.Manifest
	if err := store.readCompressedJSON(
		store.objectPath(digest, manifestFilename),
		&manifest,
	); err != nil {
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
		if err := validateCompoundPart(part); err != nil {
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
	name, err := annotationFilename(annotation.Kind)
	if err != nil {
		return err
	}
	annotation.SchemaVersion = contracts.SchemaVersionPhase00
	if annotation.GeneratedAt.IsZero() {
		annotation.GeneratedAt = time.Now().UTC()
	}
	return store.writeCompressedJSON(
		store.objectPath(annotation.ObjectDigest, name),
		annotation,
	)
}

func (store *FilesystemStore) Verify(ctx context.Context) (VerifyReport, error) {
	report := VerifyReport{Status: VerifyStatusOK}
	base := filepath.Join(store.root, "objects", "blake3")
	err := filepath.WalkDir(
		base,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) && path == base {
					return nil
				}
				report.addFinding("", path, "walk_error", walkErr.Error())
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !entry.IsDir() || !looksLikeDigest(entry.Name()) {
				return nil
			}
			digest := contracts.ObjectDigest(entry.Name())
			report.Checked++
			if expectedPath := store.objectDir(digest); path != expectedPath {
				report.addFinding(
					digest,
					path,
					"object_path_mismatch",
					fmt.Sprintf("expected %s", expectedPath),
				)
				return filepath.SkipDir
			}
			store.verifyObject(&report, digest, path)
			return filepath.SkipDir
		},
	)
	if errors.Is(err, os.ErrNotExist) {
		return report, nil
	}
	if err != nil {
		return report, err
	}
	if len(report.Findings) > 0 {
		report.Status = VerifyStatusError
	}
	return report, nil
}

func (store *FilesystemStore) writeObject(
	ctx context.Context,
	digest contracts.ObjectDigest,
	content []byte,
	sourceHint string,
	incoming contracts.Manifest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	objectDir := store.objectDir(digest)
	if err := os.MkdirAll(objectDir, 0o750); err != nil {
		return fmt.Errorf("create object directory: %w", err)
	}
	blobPath := filepath.Join(objectDir, blobFilename)
	recoveryPath := filepath.Join(objectDir, recoveryFilename)
	manifestPath := filepath.Join(objectDir, manifestFilename)
	manifest := incoming
	if existing, err := store.ReadManifest(ctx, digest); err == nil {
		manifest = mergeManifest(existing, incoming)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing manifest: %w", err)
	}
	finalContent := content
	if manifest.IdentityStrategy == identityStrategyCompoundStableID {
		var err error
		finalContent, err = compoundEnvelopeBytes(manifest)
		if err != nil {
			return err
		}
		manifest.Size = int64(len(finalContent))
	}
	compressed, err := compressZstd(finalContent)
	if err != nil {
		return err
	}
	if _, err := os.Stat(blobPath); errors.Is(err, os.ErrNotExist) {
		if err := atomicWriteFile(blobPath, compressed, 0o640); err != nil {
			return fmt.Errorf("write blob: %w", err)
		}
		if err := atomicWriteJSON(
			recoveryPath,
			recoverySidecarFor(digest, finalContent, compressed, sourceHint, manifest),
		); err != nil {
			return fmt.Errorf("write recovery sidecar: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("stat blob: %w", err)
	} else if manifest.IdentityStrategy == identityStrategyCompoundStableID {
		if err := atomicWriteFile(blobPath, compressed, 0o640); err != nil {
			return fmt.Errorf("write compound blob: %w", err)
		}
	} else if _, err := os.Stat(recoveryPath); errors.Is(err, os.ErrNotExist) {
		if err := atomicWriteJSON(
			recoveryPath,
			recoverySidecarFor(digest, finalContent, compressed, sourceHint, manifest),
		); err != nil {
			return fmt.Errorf("write recovery sidecar: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("stat recovery sidecar: %w", err)
	}
	return store.writeCompressedJSON(manifestPath, manifest)
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

func (store *FilesystemStore) verifyObject(
	report *VerifyReport,
	digest contracts.ObjectDigest,
	path string,
) {
	blobPath := filepath.Join(path, blobFilename)
	recoveryPath := filepath.Join(path, recoveryFilename)
	manifestPath := filepath.Join(path, manifestFilename)
	compressedBlob, compressedErr := os.ReadFile(blobPath)
	if compressedErr != nil {
		report.addFinding(digest, blobPath, "blob_read_failed", compressedErr.Error())
	}
	blob, err := decompressZstd(blobPath, compressedBlob)
	if err != nil {
		report.addFinding(digest, blobPath, "blob_read_failed", err.Error())
	}
	if _, err := os.Stat(recoveryPath); err != nil {
		report.addFinding(digest, recoveryPath, "recovery_missing", err.Error())
	} else {
		store.verifyRecovery(report, digest, recoveryPath, blob, compressedBlob)
	}
	var manifest contracts.Manifest
	if err := store.readCompressedJSON(manifestPath, &manifest); err != nil {
		report.addFinding(digest, manifestPath, "manifest_read_failed", err.Error())
		return
	}
	if manifest.ObjectDigest != digest {
		report.addFinding(
			digest,
			manifestPath,
			"manifest_digest_mismatch",
			fmt.Sprintf("manifest digest %s", manifest.ObjectDigest),
		)
	}
	if blob != nil {
		switch manifest.IdentityStrategy {
		case identityStrategyCompoundStableID:
			expected := contracts.ObjectDigest(
				blake3Hex([]byte("compound:" + manifest.ObjectID)),
			)
			if expected != digest {
				report.addFinding(
					digest,
					blobPath,
					"compound_digest_mismatch",
					fmt.Sprintf("expected %s", expected),
				)
			}
		default:
			if actual := blake3Hex(blob); actual != string(digest) {
				report.addFinding(
					digest,
					blobPath,
					"blob_digest_mismatch",
					fmt.Sprintf("expected %s got %s", digest, actual),
				)
			}
		}
	}
	if len(manifest.Facets) == 0 {
		report.addFinding(
			digest,
			manifestPath,
			"manifest_missing_facets",
			"manifest must have at least one facet",
		)
	}
	for _, part := range manifest.Compound.Parts {
		if err := validateCompoundPart(part); err != nil {
			report.addFinding(digest, manifestPath, "compound_invalid_part", err.Error())
			continue
		}
		if _, err := os.Stat(
			filepath.Join(store.objectDir(part.Digest), blobFilename),
		); err != nil {
			report.addFinding(
				digest,
				manifestPath,
				"compound_dangling_part",
				fmt.Sprintf("part %s: %v", part.Digest, err),
			)
		}
	}
}

func (store *FilesystemStore) verifyRecovery(
	report *VerifyReport,
	digest contracts.ObjectDigest,
	path string,
	content []byte,
	compressed []byte,
) {
	var recovery recoverySidecar
	if err := readJSON(path, &recovery); err != nil {
		report.addFinding(digest, path, "recovery_read_failed", err.Error())
		return
	}
	if recovery.Digest != string(digest) {
		report.addFinding(
			digest,
			path,
			"recovery_digest_mismatch",
			fmt.Sprintf("recovery digest %s", recovery.Digest),
		)
	}
	if content != nil && recovery.UncompressedBlake3 != blake3Hex(content) {
		report.addFinding(
			digest,
			path,
			"recovery_uncompressed_hash_mismatch",
			"uncompressed BLAKE3 does not match blob",
		)
	}
	if content != nil && recovery.UncompressedSHA256 != sha256Hex(content) {
		report.addFinding(
			digest,
			path,
			"recovery_uncompressed_hash_mismatch",
			"uncompressed SHA-256 does not match blob",
		)
	}
	if compressed != nil && recovery.CompressedSize != int64(len(compressed)) {
		report.addFinding(
			digest,
			path,
			"recovery_compressed_size_mismatch",
			fmt.Sprintf("expected %d got %d", recovery.CompressedSize, len(compressed)),
		)
	}
	if compressed != nil && recovery.CompressedBlake3 != blake3Hex(compressed) {
		report.addFinding(
			digest,
			path,
			"recovery_compressed_hash_mismatch",
			"compressed BLAKE3 does not match blob",
		)
	}
	if compressed != nil && recovery.CompressedSHA256 != sha256Hex(compressed) {
		report.addFinding(
			digest,
			path,
			"recovery_compressed_hash_mismatch",
			"compressed SHA-256 does not match blob",
		)
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
	SchemaVersion       string                 `json:"schema_version"`
	Digest              string                 `json:"digest"`
	ObjectID            string                 `json:"object_id"`
	IdentityStrategy    string                 `json:"identity_strategy"`
	SourceHint          string                 `json:"source_hint,omitempty"`
	MediaType           string                 `json:"media_type,omitempty"`
	UncompressedSize    int64                  `json:"uncompressed_size"`
	CompressedSize      int64                  `json:"compressed_size"`
	UncompressedBlake3  string                 `json:"uncompressed_blake3"`
	UncompressedSHA256  string                 `json:"uncompressed_sha256"`
	CompressedBlake3    string                 `json:"compressed_blake3"`
	CompressedSHA256    string                 `json:"compressed_sha256"`
	Compression         string                 `json:"compression"`
	CreatedAt           time.Time              `json:"created_at"`
	FilestoreVersion    string                 `json:"filestore_version"`
	ManifestSourceHints map[string]interface{} `json:"manifest_source_hints,omitempty"`
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
		if err := validateCompoundPart(part); err != nil {
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
		return result[left].ExternalID < result[right].ExternalID
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

func mergeProvenance(existing, incoming []contracts.Provenance) []contracts.Provenance {
	seen := map[string]bool{}
	result := make([]contracts.Provenance, 0, len(existing)+len(incoming))
	for _, item := range append(existing, incoming...) {
		key := item.SourceKind + "\x00" + item.SourceName + "\x00" + item.ExternalID
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

func annotationFilename(kind string) (string, error) {
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

func canonicalJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
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

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	clone := make(map[string]any, len(value))
	for key, item := range value {
		clone[key] = item
	}
	return clone
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (report *VerifyReport) addFinding(
	digest contracts.ObjectDigest,
	path, code, message string,
) {
	report.Findings = append(report.Findings, VerifyFinding{
		Digest:  digest,
		Path:    path,
		Code:    code,
		Message: message,
	})
}
