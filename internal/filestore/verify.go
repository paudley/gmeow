// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/observability"
)

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
	observability.DefaultMetrics().
		SetGauge("gmeow_corrupt_objects", float64(len(report.Findings)))

	return report, nil
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
