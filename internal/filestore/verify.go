// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"encoding/json"
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

	// Objects are enumerated by scanning the manifest key space in the metadata
	// LSM; they no longer have on-disk directories.
	err := store.metaIterPrefix("m/", func(key string, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		digest := contracts.ObjectDigest(strings.TrimPrefix(key, "m/"))
		report.Checked++
		if validateObjectDigest(digest) != nil {
			report.addFinding(
				digest,
				"",
				"object_digest_invalid",
				"manifest key is not a valid object digest",
			)

			return nil
		}

		var manifest contracts.Manifest
		if err := json.Unmarshal(value, &manifest); err != nil {
			report.addFinding(digest, "", "manifest_read_failed", err.Error())

			return nil
		}

		store.verifyObject(ctx, &report, request, digest, manifest)

		return nil
	})
	if err != nil {
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
	manifest contracts.Manifest,
) {
	// objectRef is a stable identifier for findings; objects no longer have a
	// physical directory, so it is used only as a human-readable locator.
	objectRef := store.objectDir(digest)

	// Content is stored as chunks reconstructed from the recipe; there is no
	// single compressed blob, so compressed-blob checks are skipped while the
	// reassembled content is still verified.
	var blob []byte
	if content, ok, contentErr := store.readBlobContent(digest); contentErr != nil {
		report.addFinding(digest, objectRef, "blob_read_failed", contentErr.Error())
	} else if ok {
		blob = content
	} else {
		report.addFinding(
			digest,
			objectRef,
			"blob_read_failed",
			"object content recipe missing",
		)
	}

	if recovery, found, readErr := store.readPackedRecovery(ctx, digest); readErr != nil {
		report.addFinding(digest, objectRef, "recovery_read_failed", readErr.Error())
	} else if found {
		store.verifyRecoveryRecord(report, digest, objectRef, recovery, blob, nil)
	} else {
		report.addFinding(digest, objectRef, "recovery_missing", "no recovery sidecar")
	}

	if manifest.ObjectDigest != digest {
		report.addFinding(
			digest,
			objectRef,
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
					objectRef,
					"compound_digest_mismatch",
					fmt.Sprintf("expected %s", expected),
				)
			}
		default:
			if actual := blake3Hex(blob); actual != string(digest) {
				report.addFinding(
					digest,
					objectRef,
					"blob_digest_mismatch",
					fmt.Sprintf("expected %s got %s", digest, actual),
				)
			}
		}
	}

	if len(manifest.Facets) == 0 {
		report.addFinding(
			digest,
			objectRef,
			"manifest_missing_facets",
			"manifest must have at least one facet",
		)
	}

	for _, part := range manifest.Compound.Parts {
		err := validateCompoundPart(part)
		if err != nil {
			report.addFinding(digest, objectRef, "compound_invalid_part", err.Error())

			continue
		}

		hasContent, recErr := store.hasRecipe(part.Digest)
		if recErr != nil {
			report.addFinding(
				digest,
				objectRef,
				"compound_dangling_part",
				fmt.Sprintf("part %s: %v", part.Digest, recErr),
			)

			continue
		}
		if !hasContent {
			report.addFinding(
				digest,
				objectRef,
				"compound_dangling_part",
				fmt.Sprintf("part %s: missing content", part.Digest),
			)
		}
	}

	store.verifyAnnotations(report, digest)
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
) {
	// Annotations are packed by digest in the metadata LSM; resolving them also
	// reads any externalized payloads back from the content-addressed chunk
	// store.
	if _, err := store.readPackedAnnotations(digest); err != nil {
		report.addFinding(
			digest,
			store.objectDir(digest),
			"annotation_read_failed",
			err.Error(),
		)
	}
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
