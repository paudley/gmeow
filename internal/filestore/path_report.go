// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

const defaultPathRecordLimit = 20

type PathResolveRequest struct {
	Path         string `json:"path"`
	RecordsLimit int    `json:"records_limit,omitempty"`
}

type PathResolveReport struct {
	Records        []PathResolveRecord          `json:"records,omitempty"`
	SourceObject   *contracts.SourceObjectRef   `json:"source_object,omitempty"`
	SourceCursor   *contracts.SourceCursor      `json:"source_cursor,omitempty"`
	IngestClaim    *contracts.SourceIngestClaim `json:"ingest_claim,omitempty"`
	Manifest       *PathResolveManifest         `json:"manifest,omitempty"`
	Kind           string                       `json:"kind"`
	Role           string                       `json:"role,omitempty"`
	InputPath      string                       `json:"input_path"`
	Path           string                       `json:"path"`
	PhysicalPath   string                       `json:"physical_path"`
	ObjectDigest   contracts.ObjectDigest       `json:"object_digest,omitempty"`
	LogicalBytes   int64                        `json:"logical_bytes"`
	AllocatedBytes int64                        `json:"allocated_bytes"`
	Estimated      bool                         `json:"estimated"`
	RecordCount    int                          `json:"record_count,omitempty"`
	RecordsLimit   int                          `json:"records_limit,omitempty"`
	Truncated      bool                         `json:"truncated,omitempty"`
}

type PathResolveManifest struct {
	Facets       []string                 `json:"facets,omitempty"`
	Provenance   []contracts.Provenance   `json:"provenance,omitempty"`
	Parts        []contracts.CompoundPart `json:"parts,omitempty"`
	ObjectDigest contracts.ObjectDigest   `json:"object_digest"`
	ObjectID     string                   `json:"object_id"`
	MediaType    string                   `json:"media_type,omitempty"`
	Size         int64                    `json:"size"`
	Compound     bool                     `json:"compound"`
}

type PathResolveRecord struct {
	SourceObject *contracts.SourceObjectRef `json:"source_object,omitempty"`
	Recovery     *PathResolveRecovery       `json:"recovery,omitempty"`
	Parents      []PathResolveParent        `json:"parents,omitempty"`
	ObjectDigest contracts.ObjectDigest     `json:"object_digest,omitempty"`
	ChildDigest  contracts.ObjectDigest     `json:"child_digest,omitempty"`
	UpdatedAt    string                     `json:"updated_at,omitempty"`
}

type PathResolveParent struct {
	ParentDigest contracts.ObjectDigest `json:"parent_digest"`
	Role         string                 `json:"role"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

type PathResolveRecovery struct {
	Digest             string    `json:"digest"`
	ObjectID           string    `json:"object_id"`
	IdentityStrategy   string    `json:"identity_strategy"`
	MediaType          string    `json:"media_type,omitempty"`
	UncompressedSize   int64     `json:"uncompressed_size"`
	CompressedSize     int64     `json:"compressed_size"`
	UncompressedBlake3 string    `json:"uncompressed_blake3"`
	CompressedBlake3   string    `json:"compressed_blake3"`
	CreatedAt          time.Time `json:"created_at"`
}

func (store *FilesystemStore) ResolvePath(
	ctx context.Context,
	request PathResolveRequest,
) (PathResolveReport, error) {
	if err := ctx.Err(); err != nil {
		return PathResolveReport{}, err
	}
	limit := request.RecordsLimit
	if limit <= 0 {
		limit = defaultPathRecordLimit
	}
	if limit > 1000 {
		return PathResolveReport{}, errors.New("records_limit must be <= 1000")
	}
	physicalPath, relativePath, err := store.resolveContainedPath(request.Path)
	if err != nil {
		return PathResolveReport{}, err
	}
	info, err := os.Stat(physicalPath)
	if err != nil {
		return PathResolveReport{}, err
	}
	allocated, estimated := allocatedBytes(info)
	report := PathResolveReport{
		InputPath:      request.Path,
		Path:           relativePath,
		PhysicalPath:   physicalPath,
		LogicalBytes:   info.Size(),
		AllocatedBytes: allocated,
		Estimated:      estimated,
		RecordsLimit:   limit,
	}
	parts := strings.Split(relativePath, "/")
	if len(parts) == 0 {
		return report, nil
	}
	if info.IsDir() {
		report.Kind = "directory"
		if parts[0] == "staging" {
			report.Kind = "staging"
			if len(parts) >= 2 {
				report.Role = parts[1]
			}
		}
		return report, nil
	}

	switch parts[0] {
	case "objects":
		return store.resolveObjectPath(ctx, report, parts)
	case packedSourceIndexDir:
		return resolvePackedSourceIndexPath(report, physicalPath, parts, limit)
	case packedSourceAliasIndexDir:
		return resolvePackedSourceAliasPath(report, physicalPath, parts, limit)
	case packedCompoundParentIndexDir:
		return resolvePackedCompoundParentPath(report, physicalPath, parts, limit)
	case packedRecoveryDir:
		return resolvePackedRecoveryPath(report, physicalPath, parts, limit)
	case "source-index":
		return store.resolveLegacySourceIndexPath(report, physicalPath, parts)
	case "compound-parent-index":
		return store.resolveLegacyCompoundParentPath(report, physicalPath, parts)
	case "source-cursors":
		return store.resolveSourceCursorPath(report, physicalPath)
	case "source-locks":
		return resolveSourceLockPath(report, physicalPath)
	case "staging":
		report.Kind = "staging"
		if len(parts) >= 2 {
			report.Role = parts[1]
		}
		return report, nil
	default:
		report.Kind = "unknown"
		return report, nil
	}
}

func (store *FilesystemStore) resolveContainedPath(
	path string,
) (string, string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", "", errors.New("path is required")
	}
	root, err := filepath.Abs(store.root)
	if err != nil {
		return "", "", err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return "", "", err
	}
	candidate := trimmed
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(root, candidate)
	}
	candidate, err = filepath.Abs(candidate)
	if err != nil {
		return "", "", err
	}
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", "", err
	}
	relative, err := filepath.Rel(root, resolved)
	if err != nil {
		return "", "", err
	}
	if relative == "." {
		return resolved, ".", nil
	}
	if strings.HasPrefix(relative, ".."+string(filepath.Separator)) ||
		relative == ".." ||
		filepath.IsAbs(relative) {
		return "", "", fmt.Errorf("path %q is outside filestore root", path)
	}

	return resolved, filepath.ToSlash(relative), nil
}

func (store *FilesystemStore) resolveObjectPath(
	ctx context.Context,
	report PathResolveReport,
	parts []string,
) (PathResolveReport, error) {
	report.Kind = "object"
	if len(parts) < 6 || parts[1] != "blake3" {
		report.Role = "unknown_object_path"
		return report, nil
	}
	digest := contracts.ObjectDigest(parts[4])
	if err := validateObjectDigest(digest); err != nil ||
		parts[2] != string(digest)[:2] ||
		parts[3] != string(digest)[2:4] {
		report.Role = "invalid_object_path"
		return report, nil
	}
	report.ObjectDigest = digest
	report.Role = objectStorageRole(parts[len(parts)-1])
	manifest, err := store.ReadManifest(ctx, digest)
	if err == nil {
		report.Manifest = pathResolveManifest(manifest)
	}

	return report, nil
}

func resolvePackedSourceIndexPath(
	report PathResolveReport,
	path string,
	parts []string,
	limit int,
) (PathResolveReport, error) {
	report.Kind = "packed_source_index"
	report.Role = "records"
	if !validPackedShardParts(parts, false) {
		report.Role = "invalid_shard_path"
		return report, nil
	}
	err := scanPackedRecords(path, func(record packedSourceObjectIndexEntry) error {
		report.RecordCount++
		if len(report.Records) >= limit {
			report.Truncated = true
			return nil
		}
		sourceObject := record.SourceObject
		report.Records = append(report.Records, PathResolveRecord{
			SourceObject: &sourceObject,
			ObjectDigest: record.ObjectDigest,
			UpdatedAt:    record.UpdatedAt.Format(time.RFC3339Nano),
		})
		return nil
	})
	return report, err
}

func resolvePackedSourceAliasPath(
	report PathResolveReport,
	path string,
	parts []string,
	limit int,
) (PathResolveReport, error) {
	report.Kind = "packed_source_alias_index"
	report.Role = "records"
	if !validPackedShardParts(parts, false) {
		report.Role = "invalid_shard_path"
		return report, nil
	}
	err := scanPackedRecords(path, func(record packedSourceAliasEntry) error {
		report.RecordCount++
		if len(report.Records) >= limit {
			report.Truncated = true
			return nil
		}
		report.Records = append(report.Records, PathResolveRecord{
			SourceObject: &contracts.SourceObjectRef{
				SourceKind: record.SourceKind,
				SourceName: record.SourceName,
				ExternalID: record.ExternalID,
			},
			ObjectDigest: record.Digest,
			UpdatedAt:    record.UpdatedAt.Format(time.RFC3339Nano),
		})
		return nil
	})
	return report, err
}

func resolvePackedCompoundParentPath(
	report PathResolveReport,
	path string,
	parts []string,
	limit int,
) (PathResolveReport, error) {
	report.Kind = "packed_compound_parent_index"
	report.Role = "records"
	if !validPackedShardParts(parts, true) {
		report.Role = "invalid_shard_path"
		return report, nil
	}
	err := scanPackedRecords(path, func(record packedCompoundParentIndexEntry) error {
		report.RecordCount++
		if len(report.Records) >= limit {
			report.Truncated = true
			return nil
		}
		report.Records = append(report.Records, PathResolveRecord{
			ChildDigest: record.ChildDigest,
			Parents:     pathResolveParents(record.Parents),
			UpdatedAt:   record.UpdatedAt.Format(time.RFC3339Nano),
		})
		return nil
	})
	return report, err
}

func resolvePackedRecoveryPath(
	report PathResolveReport,
	path string,
	parts []string,
	limit int,
) (PathResolveReport, error) {
	report.Kind = "packed_recovery"
	report.Role = "records"
	if !validPackedShardParts(parts, true) {
		report.Role = "invalid_shard_path"
		return report, nil
	}
	err := scanPackedRecords(path, func(record packedRecoveryEntry) error {
		report.RecordCount++
		if len(report.Records) >= limit {
			report.Truncated = true
			return nil
		}
		report.Records = append(report.Records, PathResolveRecord{
			ObjectDigest: record.Digest,
			Recovery:     pathResolveRecovery(record.Recovery),
			UpdatedAt:    record.UpdatedAt.Format(time.RFC3339Nano),
		})
		return nil
	})
	return report, err
}

func pathResolveRecovery(recovery recoverySidecar) *PathResolveRecovery {
	return &PathResolveRecovery{
		Digest:             recovery.Digest,
		ObjectID:           recovery.ObjectID,
		IdentityStrategy:   recovery.IdentityStrategy,
		MediaType:          recovery.MediaType,
		UncompressedSize:   recovery.UncompressedSize,
		CompressedSize:     recovery.CompressedSize,
		UncompressedBlake3: recovery.UncompressedBlake3,
		CompressedBlake3:   recovery.CompressedBlake3,
		CreatedAt:          recovery.CreatedAt,
	}
}

func (store *FilesystemStore) resolveLegacySourceIndexPath(
	report PathResolveReport,
	path string,
	parts []string,
) (PathResolveReport, error) {
	report.Kind = "legacy_source_index"
	report.Role = "record"
	if len(parts) != 4 || filepath.Ext(parts[3]) != ".json" {
		report.Role = "invalid_source_index_path"
		return report, nil
	}
	var entry sourceObjectIndexEntry
	if err := readJSON(path, &entry); err != nil {
		return report, err
	}
	report.ObjectDigest = entry.ObjectDigest
	sourceObject := entry.SourceObject
	report.SourceObject = &sourceObject
	report.Records = []PathResolveRecord{{
		SourceObject: &sourceObject,
		ObjectDigest: entry.ObjectDigest,
		UpdatedAt:    entry.UpdatedAt.Format(time.RFC3339Nano),
	}}
	report.RecordCount = 1

	return report, nil
}

func (store *FilesystemStore) resolveLegacyCompoundParentPath(
	report PathResolveReport,
	path string,
	parts []string,
) (PathResolveReport, error) {
	report.Kind = "legacy_compound_parent_index"
	report.Role = "record"
	if len(parts) != 5 || parts[1] != "blake3" || filepath.Ext(parts[4]) != ".json" {
		report.Role = "invalid_compound_parent_path"
		return report, nil
	}
	var record compoundParentIndexRecord
	if err := readJSON(path, &record); err != nil {
		return report, err
	}
	report.ObjectDigest = record.ChildDigest
	report.Records = []PathResolveRecord{{
		ChildDigest: record.ChildDigest,
		Parents:     pathResolveParents(record.Parents),
	}}
	report.RecordCount = 1

	return report, nil
}

func pathResolveParents(parents []compoundParentEdge) []PathResolveParent {
	result := make([]PathResolveParent, 0, len(parents))
	for _, parent := range parents {
		result = append(result, PathResolveParent(parent))
	}

	return result
}

func (store *FilesystemStore) resolveSourceCursorPath(
	report PathResolveReport,
	path string,
) (PathResolveReport, error) {
	report.Kind = "source_cursor"
	report.Role = "cursor"
	var cursor contracts.SourceCursor
	if err := store.readCompressedJSON(path, &cursor); err != nil {
		return report, err
	}
	report.SourceCursor = &cursor

	return report, nil
}

func resolveSourceLockPath(
	report PathResolveReport,
	path string,
) (PathResolveReport, error) {
	report.Kind = "source_lock"
	report.Role = "claim"
	var claim contracts.SourceIngestClaim
	if err := readJSON(path, &claim); err != nil {
		return report, err
	}
	report.IngestClaim = &claim
	sourceObject := claim.SourceObject
	report.SourceObject = &sourceObject

	return report, nil
}

func validPackedShardParts(parts []string, hasAlgorithm bool) bool {
	if hasAlgorithm {
		return len(parts) == 5 &&
			parts[1] == "blake3" &&
			validHexShard(parts[2]) &&
			validHexShard(parts[3]) &&
			parts[4] == packedShardFilename
	}

	return len(parts) == 4 &&
		validHexShard(parts[1]) &&
		validHexShard(parts[2]) &&
		parts[3] == packedShardFilename
}

func validHexShard(value string) bool {
	if len(value) != 2 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}

	return true
}

func pathResolveManifest(manifest contracts.Manifest) *PathResolveManifest {
	facets := make([]string, 0, len(manifest.Facets))
	for _, facet := range manifest.Facets {
		facets = append(facets, facet.FacetKind())
	}
	sort.Strings(facets)

	return &PathResolveManifest{
		ObjectDigest: manifest.ObjectDigest,
		ObjectID:     manifest.ObjectID,
		MediaType:    manifest.MediaType,
		Size:         manifest.Size,
		Facets:       facets,
		Provenance:   append([]contracts.Provenance{}, manifest.Provenance...),
		Compound:     manifest.Compound.IsCompound,
		Parts:        append([]contracts.CompoundPart{}, manifest.Compound.Parts...),
	}
}
