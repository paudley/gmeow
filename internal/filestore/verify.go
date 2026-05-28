// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/observability"
)

func (store *FilesystemStore) Verify(
	ctx context.Context,
	request VerifyRequest,
) (VerifyReport, error) {
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

			err := ctx.Err()
			if err != nil {
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
					"expected "+expectedPath,
				)

				return filepath.SkipDir
			}

			store.verifyObject(ctx, &report, request, digest, path)

			return filepath.SkipDir
		},
	)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return report, err
	}

	store.verifyPackedShards(ctx, &report, request)
	store.verifyStagingTree(ctx, &report, request)
	if request.Repair {
		if lockReport, lockErr := store.CleanupSourceLocks(ctx); lockErr != nil {
			report.addFinding("", "", "source_lock_cleanup_failed", lockErr.Error())
		} else if lockReport.RemovedFiles > 0 {
			report.Repaired += lockReport.RemovedFiles
		}
	}

	if len(report.Findings) > 0 {
		report.Status = VerifyStatusError
	}
	observability.DefaultMetrics().
		SetGauge("gmeow_corrupt_objects", float64(len(report.Findings)))

	return report, nil
}

func (store *FilesystemStore) verifyObject(
	ctx context.Context,
	report *VerifyReport,
	request VerifyRequest,
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
		if recovery, found, readErr := store.readPackedRecovery(
			ctx,
			digest,
		); readErr != nil {
			report.addFinding(digest, recoveryPath, "recovery_read_failed", readErr.Error())
		} else if found {
			store.verifyRecoveryRecord(
				report,
				digest,
				recoveryPath,
				recovery,
				blob,
				compressedBlob,
			)
		} else {
			report.addFinding(digest, recoveryPath, "recovery_missing", err.Error())
		}
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
		err := validateCompoundPart(part)
		if err != nil {
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

	store.verifyAnnotations(report, digest, path)
	store.verifySourceIndexReachability(ctx, report, request, digest, manifest)
}

func (store *FilesystemStore) verifySourceIndexReachability(
	ctx context.Context,
	report *VerifyReport,
	request VerifyRequest,
	digest contracts.ObjectDigest,
	manifest contracts.Manifest,
) {
	if len(manifest.Provenance) == 0 {
		return
	}
	for _, prov := range manifest.Provenance {
		ref := sourceObjectRefFromProvenance(prov)
		if ref.SourceKind == "" || ref.SourceName == "" || ref.ExternalID == "" {
			continue
		}
		_, found, err := store.LookupSourceObject(ctx, ref)
		if err != nil {
			report.addFinding(
				digest,
				"",
				"source_index_read_error",
				fmt.Sprintf(
					"source %s/%s:%s: %v",
					ref.SourceKind,
					ref.SourceName,
					ref.ExternalID,
					err,
				),
			)
			continue
		}
		if found {
			continue
		}
		report.addFinding(
			digest,
			"",
			"source_index_missing",
			fmt.Sprintf(
				"source %s/%s:%s has no index entry",
				ref.SourceKind,
				ref.SourceName,
				ref.ExternalID,
			),
		)
		if request.Repair {
			if rebuildErr := store.recordSourceObjectIndexes(
				ctx,
				digest,
				manifest.Provenance,
			); rebuildErr != nil {
				report.addFinding(digest, "", "source_index_repair_failed", rebuildErr.Error())
			} else {
				report.Repaired++
			}
			return
		}
	}
}

func (store *FilesystemStore) verifyAnnotations(
	report *VerifyReport,
	digest contracts.ObjectDigest,
	path string,
) {
	entries, err := os.ReadDir(path)
	if err != nil {
		report.addFinding(digest, path, "annotation_list_failed", err.Error())

		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		entryPath := filepath.Join(path, name)
		if strings.HasPrefix(name, ".") {
			report.addFinding(
				digest,
				entryPath,
				"staged_write_leftover",
				"interrupted atomic write left a staged file",
			)

			continue
		}

		if !isProjectionAnnotationFilename(name) {
			continue
		}

		var annotation contracts.Annotation
		if err := store.readCompressedJSON(entryPath, &annotation); err != nil {
			report.addFinding(digest, entryPath, "annotation_read_failed", err.Error())
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
	err := readJSON(path, &recovery)
	if err != nil {
		report.addFinding(digest, path, "recovery_read_failed", err.Error())

		return
	}

	store.verifyRecoveryRecord(report, digest, path, recovery, content, compressed)
}

func (store *FilesystemStore) verifyRecoveryRecord(
	report *VerifyReport,
	digest contracts.ObjectDigest,
	path string,
	recovery recoverySidecar,
	content []byte,
	compressed []byte,
) {
	if recovery.Digest != string(digest) {
		report.addFinding(
			digest,
			path,
			"recovery_digest_mismatch",
			"recovery digest "+recovery.Digest,
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

const stagingMaxAge = 1 * time.Hour

func (store *FilesystemStore) verifyPackedShards(
	ctx context.Context,
	report *VerifyReport,
	request VerifyRequest,
) {
	store.verifyPackedShardDir(ctx, report, request, packedSourceIndexDir, "source_index")
	store.verifyPackedShardDir(
		ctx,
		report,
		request,
		packedSourceAliasIndexDir,
		"source_alias_index",
	)
	store.verifyPackedShardDir(
		ctx,
		report,
		request,
		filepath.Join(packedCompoundParentIndexDir, "blake3"),
		"compound_parent_index",
	)
	store.verifyPackedShardDir(
		ctx,
		report,
		request,
		filepath.Join(packedRecoveryDir, "blake3"),
		"recovery",
	)
}

func (store *FilesystemStore) verifyPackedShardDir(
	ctx context.Context,
	report *VerifyReport,
	request VerifyRequest,
	relDir string,
	tier string,
) {
	base := filepath.Join(store.root, relDir)
	err := filepath.WalkDir(
		base,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) && path == base {
					return filepath.SkipAll
				}
				report.addFinding("", path, tier+"_walk_error", walkErr.Error())
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if entry.IsDir() || entry.Name() != packedShardFilename {
				return nil
			}
			store.verifyOnePackedShard(ctx, report, request, path, tier)
			return nil
		},
	)
	if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, context.Canceled) {
		report.addFinding("", base, tier+"_walk_error", err.Error())
	}
}

func (store *FilesystemStore) verifyOnePackedShard(
	ctx context.Context,
	report *VerifyReport,
	request VerifyRequest,
	path string,
	tier string,
) {
	result, err := scanPackedShard(path, func(payload []byte) error {
		report.Checked++
		return nil
	})
	if err != nil {
		report.addFinding("", path, tier+"_shard_corrupt", err.Error())
		return
	}
	if result.TornTailBytes > 0 {
		report.addFinding(
			"", path, tier+"_torn_tail",
			fmt.Sprintf("%d bytes after last intact record at offset %d",
				result.TornTailBytes, result.LastIntactEnd),
		)
		if request.Repair {
			if truncErr := truncatePackedShardTornTail(
				ctx,
				path,
				result.LastIntactEnd,
			); truncErr != nil {
				report.addFinding("", path, tier+"_repair_failed", truncErr.Error())
			} else {
				report.Repaired++
			}
		}
	}
}

func (store *FilesystemStore) verifyStagingTree(
	_ context.Context,
	report *VerifyReport,
	request VerifyRequest,
) {
	stagingBase := filepath.Join(store.root, "staging")
	entries, err := os.ReadDir(stagingBase)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		report.addFinding("", stagingBase, "staging_read_error", err.Error())
		return
	}
	cutoff := time.Now().Add(-stagingMaxAge)
	for _, sub := range entries {
		subPath := filepath.Join(stagingBase, sub.Name())
		store.verifyStagingSubdir(report, request, subPath, cutoff)
	}
}

func (store *FilesystemStore) verifyStagingSubdir(
	report *VerifyReport,
	request VerifyRequest,
	dir string,
	cutoff time.Time,
) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		report.addFinding("", dir, "staging_read_error", err.Error())
		return
	}
	for _, entry := range entries {
		entryPath := filepath.Join(dir, entry.Name())
		info, infoErr := entry.Info()
		if infoErr != nil {
			report.addFinding("", entryPath, "staging_stat_error", infoErr.Error())
			continue
		}
		if info.ModTime().Before(cutoff) {
			report.addFinding(
				"",
				entryPath,
				"staging_stale",
				fmt.Sprintf(
					"modified %s, older than %s",
					info.ModTime().Format(time.RFC3339),
					stagingMaxAge,
				),
			)
			if request.Repair {
				if removeErr := os.RemoveAll(entryPath); removeErr != nil {
					report.addFinding("", entryPath, "staging_repair_failed", removeErr.Error())
				} else {
					report.Repaired++
				}
			}
		}
	}
}
