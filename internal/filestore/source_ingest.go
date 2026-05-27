// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func (store *FilesystemStore) LookupSourceObject(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}

	if err := validateSourceObjectRef(ref); err != nil {
		return "", false, err
	}

	var found contracts.ObjectDigest

	err := store.WalkProjection(ctx, func(object ProjectionObject) error {
		if len(object.Findings) > 0 {
			return nil
		}

		for _, provenance := range object.Manifest.Provenance {
			if sourceRefMatches(provenance, ref) {
				found = object.Manifest.ObjectDigest

				return errStopWalk
			}
		}

		return nil
	})
	if errors.Is(err, errStopWalk) {
		return found, true, nil
	}

	if err != nil {
		return "", false, err
	}

	return "", false, nil
}

func (store *FilesystemStore) TryAcquireSourceIngest(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.SourceIngestClaim, bool, error) {
	if err := ctx.Err(); err != nil {
		return contracts.SourceIngestClaim{}, false, err
	}

	if err := validateSourceObjectRef(ref); err != nil {
		return contracts.SourceIngestClaim{}, false, err
	}

	acquiredAt := time.Now().UTC()
	claim := contracts.SourceIngestClaim{
		SourceObject: ref,
		ClaimID:      sourceObjectLockID(ref, acquiredAt),
		AcquiredAt:   acquiredAt,
	}

	lockPath := store.sourceObjectLockPath(ref)
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o750); err != nil {
		return contracts.SourceIngestClaim{}, false, err
	}

	file, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if errors.Is(err, os.ErrExist) {
		return contracts.SourceIngestClaim{}, false, nil
	}

	if err != nil {
		return contracts.SourceIngestClaim{}, false, err
	}

	encoded, encodeErr := canonicalJSON(claim)
	if encodeErr != nil {
		_ = file.Close()
		_ = os.Remove(lockPath)

		return contracts.SourceIngestClaim{}, false, encodeErr
	}

	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		_ = os.Remove(lockPath)

		return contracts.SourceIngestClaim{}, false, err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(lockPath)

		return contracts.SourceIngestClaim{}, false, err
	}

	if err := file.Close(); err != nil {
		_ = os.Remove(lockPath)

		return contracts.SourceIngestClaim{}, false, err
	}

	if err := fsyncDir(filepath.Dir(lockPath)); err != nil {
		_ = os.Remove(lockPath)

		return contracts.SourceIngestClaim{}, false, err
	}

	return claim, true, nil
}

func (store *FilesystemStore) ReleaseSourceIngest(
	ctx context.Context,
	claim contracts.SourceIngestClaim,
) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	err = validateSourceObjectRef(claim.SourceObject)
	if err != nil {
		return err
	}

	if strings.TrimSpace(claim.ClaimID) == "" {
		return errors.New("source ingest claim_id is required")
	}

	lockPath := store.sourceObjectLockPath(claim.SourceObject)

	var existing contracts.SourceIngestClaim
	err = readJSON(lockPath, &existing)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	if existing.ClaimID != claim.ClaimID {
		return errors.New("source ingest claim is owned by another writer")
	}

	err = os.Remove(lockPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return fsyncDir(filepath.Dir(lockPath))
}

func validateSourceObjectRef(ref contracts.SourceObjectRef) error {
	if strings.TrimSpace(ref.SourceKind) == "" {
		return errors.New("source object kind is required")
	}

	if strings.TrimSpace(ref.SourceName) == "" {
		return errors.New("source object name is required")
	}

	if strings.TrimSpace(ref.ExternalID) == "" {
		return errors.New("source object external_id is required")
	}

	return nil
}

func sourceObjectRefFromProvenance(
	provenance contracts.Provenance,
) contracts.SourceObjectRef {
	return contracts.SourceObjectRef{
		SourceKind:      provenance.SourceKind,
		SourceName:      provenance.SourceName,
		ExternalID:      provenance.ExternalID,
		ExternalVersion: provenance.ExternalVersion,
	}
}

func sourceRefMatches(
	provenance contracts.Provenance,
	ref contracts.SourceObjectRef,
) bool {
	return provenance.SourceKind == ref.SourceKind &&
		provenance.SourceName == ref.SourceName &&
		provenance.ExternalID == ref.ExternalID &&
		provenance.ExternalVersion == ref.ExternalVersion
}

func sourceObjectRefKey(ref contracts.SourceObjectRef) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		ref.SourceKind,
		ref.SourceName,
		ref.ExternalID,
		ref.ExternalVersion,
	}, "\x00")))

	return hex.EncodeToString(sum[:])
}

func sourceObjectLockID(ref contracts.SourceObjectRef, acquiredAt time.Time) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		sourceObjectRefKey(ref),
		acquiredAt.Format(time.RFC3339Nano),
	}, "\x00")))

	return hex.EncodeToString(sum[:])
}
