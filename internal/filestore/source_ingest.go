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
	"sort"
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

	var entry sourceObjectIndexEntry
	err := readJSON(store.sourceObjectIndexPath(ref), &entry)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !sourceObjectRefsEqual(entry.SourceObject, ref) {
		return "", false, errors.New("source object index key does not match payload")
	}
	if err := validateObjectDigest(entry.ObjectDigest); err != nil {
		return "", false, err
	}

	manifest, err := store.ReadManifest(ctx, entry.ObjectDigest)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	for _, provenance := range manifest.Provenance {
		if sourceRefMatches(provenance, ref) {
			return entry.ObjectDigest, true, nil
		}
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
	return sourceObjectRefsEqual(sourceObjectRefFromProvenance(provenance), ref)
}

func sourceObjectRefsEqual(
	left contracts.SourceObjectRef,
	right contracts.SourceObjectRef,
) bool {
	return left.SourceKind == right.SourceKind &&
		left.SourceName == right.SourceName &&
		left.ExternalID == right.ExternalID &&
		left.ExternalVersion == right.ExternalVersion
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

type sourceObjectIndexEntry struct {
	SourceObject contracts.SourceObjectRef `json:"source_object"`
	ObjectDigest contracts.ObjectDigest    `json:"object_digest"`
	UpdatedAt    time.Time                 `json:"updated_at"`
}

type compoundParentIndexRecord struct {
	SchemaVersion int                    `json:"schema_version"`
	ChildDigest   contracts.ObjectDigest `json:"child_digest"`
	Parents       []compoundParentEdge   `json:"parents"`
}

type compoundParentEdge struct {
	ParentDigest contracts.ObjectDigest `json:"parent_digest"`
	Role         string                 `json:"role"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

func (store *FilesystemStore) recordSourceObjectIndexes(
	ctx context.Context,
	digest contracts.ObjectDigest,
	provenance []contracts.Provenance,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(provenance) == 0 {
		return nil
	}
	if err := validateObjectDigest(digest); err != nil {
		return err
	}

	updatedAt := time.Now().UTC()
	for _, item := range provenance {
		ref := sourceObjectRefFromProvenance(item)
		if ref.SourceKind == "" && ref.SourceName == "" && ref.ExternalID == "" {
			continue
		}
		if ref.SourceKind == "" || ref.SourceName == "" || ref.ExternalID == "" {
			continue
		}
		if err := validateSourceObjectRef(ref); err != nil {
			return err
		}
		entry := sourceObjectIndexEntry{
			SourceObject: ref,
			ObjectDigest: digest,
			UpdatedAt:    updatedAt,
		}
		if err := atomicWriteJSON(store.sourceObjectIndexPath(ref), entry); err != nil {
			return err
		}
	}

	return nil
}

func (store *FilesystemStore) recordCompoundParentIndexes(
	ctx context.Context,
	parentDigest contracts.ObjectDigest,
	parts []contracts.CompoundPart,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(parts) == 0 {
		return nil
	}
	if err := validateObjectDigest(parentDigest); err != nil {
		return err
	}

	updatedAt := time.Now().UTC()
	byChild := map[contracts.ObjectDigest][]compoundParentEdge{}
	for _, part := range parts {
		if err := validateCompoundPart(part); err != nil {
			return err
		}
		byChild[part.Digest] = append(byChild[part.Digest], compoundParentEdge{
			ParentDigest: parentDigest,
			Role:         part.Role,
			UpdatedAt:    updatedAt,
		})
	}

	for childDigest, edges := range byChild {
		if err := store.mergeCompoundParentIndex(ctx, childDigest, edges); err != nil {
			return err
		}
	}

	return nil
}

func (store *FilesystemStore) mergeCompoundParentIndex(
	ctx context.Context,
	childDigest contracts.ObjectDigest,
	edges []compoundParentEdge,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateObjectDigest(childDigest); err != nil {
		return err
	}

	unlock := store.lockKey("compound-parent-index:" + string(childDigest))
	defer unlock()

	path := store.compoundParentIndexPath(childDigest)
	record := compoundParentIndexRecord{
		SchemaVersion: int(contracts.SchemaVersionPhase00),
		ChildDigest:   childDigest,
	}
	if err := readJSON(path, &record); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if record.ChildDigest != "" && record.ChildDigest != childDigest {
		return errors.New("compound parent index key does not match payload")
	}
	record.SchemaVersion = int(contracts.SchemaVersionPhase00)
	record.ChildDigest = childDigest

	merged := map[string]compoundParentEdge{}
	for _, parent := range record.Parents {
		if err := validateObjectDigest(parent.ParentDigest); err != nil {
			continue
		}
		merged[compoundParentEdgeKey(parent)] = parent
	}
	for _, edge := range edges {
		if err := validateObjectDigest(edge.ParentDigest); err != nil {
			return err
		}
		merged[compoundParentEdgeKey(edge)] = edge
	}

	record.Parents = make([]compoundParentEdge, 0, len(merged))
	for _, edge := range merged {
		record.Parents = append(record.Parents, edge)
	}
	sortCompoundParentEdges(record.Parents)

	return atomicWriteJSON(path, record)
}

func (store *FilesystemStore) compoundParentsForChild(
	ctx context.Context,
	childDigest contracts.ObjectDigest,
) ([]compoundParentEdge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateObjectDigest(childDigest); err != nil {
		return nil, err
	}

	var record compoundParentIndexRecord
	err := readJSON(store.compoundParentIndexPath(childDigest), &record)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if record.ChildDigest != childDigest {
		return nil, errors.New("compound parent index key does not match payload")
	}

	return append([]compoundParentEdge{}, record.Parents...), nil
}

func compoundParentEdgeKey(edge compoundParentEdge) string {
	return string(edge.ParentDigest) + "\x00" + edge.Role
}

func sortCompoundParentEdges(edges []compoundParentEdge) {
	sort.SliceStable(edges, func(left, right int) bool {
		if edges[left].ParentDigest != edges[right].ParentDigest {
			return edges[left].ParentDigest < edges[right].ParentDigest
		}

		return edges[left].Role < edges[right].Role
	})
}
