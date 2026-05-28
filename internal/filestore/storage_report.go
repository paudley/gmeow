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
	"syscall"

	"blackcat.ca/gmeow/internal/contracts"
)

type StorageBreakdownRequest struct {
	Digest         contracts.ObjectDigest `json:"digest"`
	RecursiveParts bool                   `json:"recursive_parts"`
}

type StorageBreakdownReport struct {
	Files                 []StorageBreakdownFile `json:"files"`
	RootDigest            contracts.ObjectDigest `json:"root_digest"`
	TotalAllocatedBytes   int64                  `json:"total_allocated_bytes"`
	TotalLogicalBytes     int64                  `json:"total_logical_bytes"`
	EstimatedAllocated    bool                   `json:"estimated_allocated"`
	FileCount             int                    `json:"file_count"`
	ReferencedObjectCount int                    `json:"referenced_object_count"`
	RecursiveParts        bool                   `json:"recursive_parts"`
}

type StorageBreakdownFile struct {
	ObjectDigest   contracts.ObjectDigest `json:"object_digest,omitempty"`
	Role           string                 `json:"role"`
	Path           string                 `json:"path"`
	LogicalBytes   int64                  `json:"logical_bytes"`
	AllocatedBytes int64                  `json:"allocated_bytes"`
	Estimated      bool                   `json:"estimated"`
	ReferencedBy   contracts.ObjectDigest `json:"referenced_by,omitempty"`
	CompoundRole   string                 `json:"compound_role,omitempty"`
	CompoundOrder  int                    `json:"compound_order,omitempty"`
	RecursivePart  bool                   `json:"recursive_part,omitempty"`
	PhysicalPath   string                 `json:"-"`
}

func (store *FilesystemStore) StorageBreakdown(
	ctx context.Context,
	request StorageBreakdownRequest,
) (StorageBreakdownReport, error) {
	if err := ctx.Err(); err != nil {
		return StorageBreakdownReport{}, err
	}
	if err := validateObjectDigest(request.Digest); err != nil {
		return StorageBreakdownReport{}, err
	}

	report := StorageBreakdownReport{
		RootDigest:        request.Digest,
		RecursiveParts:    request.RecursiveParts,
		Files:             []StorageBreakdownFile{},
		TotalLogicalBytes: 0,
	}
	visitedObjects := map[contracts.ObjectDigest]bool{}
	seenFiles := map[string]bool{}
	if err := store.addObjectStorage(
		ctx,
		&report,
		seenFiles,
		visitedObjects,
		request.Digest,
		contracts.ObjectDigest(""),
		"",
		0,
		false,
		request.RecursiveParts,
	); err != nil {
		return StorageBreakdownReport{}, err
	}
	report.FileCount = len(report.Files)
	report.ReferencedObjectCount = len(visitedObjects)
	sort.SliceStable(report.Files, func(left, right int) bool {
		if report.Files[left].ObjectDigest != report.Files[right].ObjectDigest {
			return report.Files[left].ObjectDigest < report.Files[right].ObjectDigest
		}
		if report.Files[left].Role != report.Files[right].Role {
			return report.Files[left].Role < report.Files[right].Role
		}
		return report.Files[left].Path < report.Files[right].Path
	})

	return report, nil
}

func (store *FilesystemStore) addObjectStorage(
	ctx context.Context,
	report *StorageBreakdownReport,
	seenFiles map[string]bool,
	visitedObjects map[contracts.ObjectDigest]bool,
	digest contracts.ObjectDigest,
	referencedBy contracts.ObjectDigest,
	compoundRole string,
	compoundOrder int,
	recursivePart bool,
	recurse bool,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if visitedObjects[digest] {
		return nil
	}
	visitedObjects[digest] = true

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		return err
	}

	if err := store.addObjectDirectoryFiles(
		ctx,
		report,
		seenFiles,
		digest,
		referencedBy,
		compoundRole,
		compoundOrder,
		recursivePart,
	); err != nil {
		return err
	}
	if err := store.addObjectMetadataFiles(
		ctx,
		report,
		seenFiles,
		digest,
		manifest,
		referencedBy,
		compoundRole,
		compoundOrder,
		recursivePart,
	); err != nil {
		return err
	}

	if !recurse || !manifest.Compound.IsCompound {
		return nil
	}
	for _, part := range manifest.Compound.Parts {
		if err := store.addObjectStorage(
			ctx,
			report,
			seenFiles,
			visitedObjects,
			part.Digest,
			digest,
			part.Role,
			part.Order,
			true,
			true,
		); err != nil {
			return err
		}
	}

	return nil
}

func (store *FilesystemStore) addObjectDirectoryFiles(
	ctx context.Context,
	report *StorageBreakdownReport,
	seenFiles map[string]bool,
	digest contracts.ObjectDigest,
	referencedBy contracts.ObjectDigest,
	compoundRole string,
	compoundOrder int,
	recursivePart bool,
) error {
	objectDir := store.objectDir(digest)
	entries, err := os.ReadDir(objectDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(objectDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err := store.addStorageFile(
			report,
			seenFiles,
			storageFileContext{
				ObjectDigest:  digest,
				Role:          objectStorageRole(entry.Name()),
				Path:          path,
				ReferencedBy:  referencedBy,
				CompoundRole:  compoundRole,
				CompoundOrder: compoundOrder,
				RecursivePart: recursivePart,
			},
		); err != nil {
			return err
		}
	}

	return nil
}

func (store *FilesystemStore) addObjectMetadataFiles(
	ctx context.Context,
	report *StorageBreakdownReport,
	seenFiles map[string]bool,
	digest contracts.ObjectDigest,
	manifest contracts.Manifest,
	referencedBy contracts.ObjectDigest,
	compoundRole string,
	compoundOrder int,
	recursivePart bool,
) error {
	if _, found, err := store.readPackedRecovery(ctx, digest); err != nil {
		return err
	} else if found {
		if err := store.addStorageFile(report, seenFiles, storageFileContext{
			ObjectDigest:  digest,
			Role:          "packed_recovery",
			Path:          store.packedRecoveryShardPath(string(digest)),
			ReferencedBy:  referencedBy,
			CompoundRole:  compoundRole,
			CompoundOrder: compoundOrder,
			RecursivePart: recursivePart,
		}); err != nil {
			return err
		}
	}

	if _, found, err := store.readPackedCompoundParentIndex(ctx, digest); err != nil {
		return err
	} else if found {
		if err := store.addStorageFile(report, seenFiles, storageFileContext{
			ObjectDigest:  digest,
			Role:          "packed_compound_parent_index",
			Path:          store.packedCompoundParentShardPath(string(digest)),
			ReferencedBy:  referencedBy,
			CompoundRole:  compoundRole,
			CompoundOrder: compoundOrder,
			RecursivePart: recursivePart,
		}); err != nil {
			return err
		}
	}

	if legacyPath := store.compoundParentIndexPath(digest); fileExists(legacyPath) {
		if err := store.addStorageFile(report, seenFiles, storageFileContext{
			ObjectDigest:  digest,
			Role:          "legacy_compound_parent_index",
			Path:          legacyPath,
			ReferencedBy:  referencedBy,
			CompoundRole:  compoundRole,
			CompoundOrder: compoundOrder,
			RecursivePart: recursivePart,
		}); err != nil {
			return err
		}
	}

	for _, provenance := range manifest.Provenance {
		ref := contracts.SourceObjectRef{
			SourceKind:      provenance.SourceKind,
			SourceName:      provenance.SourceName,
			ExternalID:      provenance.ExternalID,
			ExternalVersion: provenance.ExternalVersion,
		}
		if validateSourceObjectRef(ref) != nil {
			continue
		}
		if _, found, err := store.readPackedSourceObjectIndex(ctx, ref); err != nil {
			return err
		} else if found {
			if err := store.addStorageFile(report, seenFiles, storageFileContext{
				ObjectDigest:  digest,
				Role:          "packed_source_index",
				Path:          store.packedSourceIndexShardPath(sourceObjectRefKey(ref)),
				ReferencedBy:  referencedBy,
				CompoundRole:  compoundRole,
				CompoundOrder: compoundOrder,
				RecursivePart: recursivePart,
			}); err != nil {
				return err
			}
		}
		if legacyPath := store.sourceObjectIndexPath(ref); fileExists(legacyPath) {
			if err := store.addStorageFile(report, seenFiles, storageFileContext{
				ObjectDigest:  digest,
				Role:          "legacy_source_index",
				Path:          legacyPath,
				ReferencedBy:  referencedBy,
				CompoundRole:  compoundRole,
				CompoundOrder: compoundOrder,
				RecursivePart: recursivePart,
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

type storageFileContext struct {
	ObjectDigest  contracts.ObjectDigest
	Role          string
	Path          string
	ReferencedBy  contracts.ObjectDigest
	CompoundRole  string
	CompoundOrder int
	RecursivePart bool
}

func (store *FilesystemStore) addStorageFile(
	report *StorageBreakdownReport,
	seenFiles map[string]bool,
	context storageFileContext,
) error {
	if seenFiles[context.Path] {
		return nil
	}
	seenFiles[context.Path] = true

	info, err := os.Stat(context.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}
	if !info.Mode().IsRegular() {
		return nil
	}

	allocated, estimated := allocatedBytes(info)
	relative, err := filepath.Rel(store.root, context.Path)
	if err != nil {
		relative = context.Path
	}
	relative = filepath.ToSlash(relative)
	report.TotalLogicalBytes += info.Size()
	report.TotalAllocatedBytes += allocated
	report.EstimatedAllocated = report.EstimatedAllocated || estimated
	report.Files = append(report.Files, StorageBreakdownFile{
		ObjectDigest:   context.ObjectDigest,
		Role:           context.Role,
		Path:           relative,
		LogicalBytes:   info.Size(),
		AllocatedBytes: allocated,
		Estimated:      estimated,
		ReferencedBy:   context.ReferencedBy,
		CompoundRole:   context.CompoundRole,
		CompoundOrder:  context.CompoundOrder,
		RecursivePart:  context.RecursivePart,
		PhysicalPath:   context.Path,
	})

	return nil
}

func allocatedBytes(info os.FileInfo) (int64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return info.Size(), true
	}
	if stat.Blocks <= 0 {
		return info.Size(), true
	}

	return stat.Blocks * 512, false
}

func objectStorageRole(name string) string {
	switch {
	case name == blobFilename:
		return "blob"
	case name == manifestFilename:
		return "manifest"
	case name == recoveryFilename:
		return "legacy_recovery"
	case name == "overlays.json.zst":
		return "overlays"
	case name == "scheduler.json.zst":
		return "scheduler"
	case strings.HasPrefix(name, "analysis") && strings.HasSuffix(name, ".json.zst"):
		return "analysis"
	case strings.HasSuffix(name, ".json.zst"):
		return "annotation"
	default:
		return "object_file"
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
