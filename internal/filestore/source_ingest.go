// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

const sourceIngestClaimTTL = 15 * time.Minute

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

	entry, found, err := store.readPackedSourceObjectIndex(ctx, ref)
	if err != nil {
		return "", false, err
	}
	if !found {
		err = readJSON(store.sourceObjectIndexPath(ref), &entry)
		if errors.Is(err, os.ErrNotExist) {
			return store.lookupSourceObjectViaAlias(ctx, ref)
		}
		if err != nil {
			return "", false, err
		}
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

func (store *FilesystemStore) lookupSourceObjectViaAlias(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	aliasDigest, found, err := store.readPackedSourceAlias(ctx, ref)
	if err != nil || !found {
		return "", false, err
	}
	if err := validateObjectDigest(aliasDigest); err != nil {
		return "", false, err
	}
	manifest, err := store.ReadManifest(ctx, aliasDigest)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	for _, prov := range manifest.Provenance {
		if prov.SourceKind == ref.SourceKind &&
			prov.SourceName == ref.SourceName &&
			prov.ExternalID == ref.ExternalID {
			return aliasDigest, true, nil
		}
	}
	return "", false, nil
}

func sourceLockKey(ref contracts.SourceObjectRef) string {
	return "lk/" + sourceObjectRefKey(ref)
}

// sourceClaimLive reports whether a claim is still within its TTL. A zero
// AcquiredAt (legacy/unknown age) is treated conservatively as live.
func sourceClaimLive(claim contracts.SourceIngestClaim, now time.Time) bool {
	if claim.AcquiredAt.IsZero() {
		return true
	}

	return now.Sub(claim.AcquiredAt) <= sourceIngestClaimTTL
}

// TryAcquireSourceIngest claims exclusive ingest rights for a source object.
// Claims live in the metadata LSM (lk/). FILESTORE is the sole writer of that
// store, so an in-process key lock plus a Pebble read/modify/write is an atomic
// test-and-set — it replaces the former O_EXCL lock file, which only guarded
// against multiple processes that the exclusive Pebble lock already prevents.
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

	unlock := store.lockKey("source-ingest:" + sourceObjectRefKey(ref))
	defer unlock()

	key := sourceLockKey(ref)
	var existing contracts.SourceIngestClaim
	found, err := store.metaGet(key, &existing)
	if err != nil {
		return contracts.SourceIngestClaim{}, false, err
	}
	if found && sourceClaimLive(existing, acquiredAt) {
		return contracts.SourceIngestClaim{}, false, nil
	}
	if !found {
		// Honor a live legacy on-disk lock on a pre-migration root.
		if held, legacyErr := store.legacyClaimHeld(ref, acquiredAt); legacyErr != nil {
			return contracts.SourceIngestClaim{}, false, legacyErr
		} else if held {
			return contracts.SourceIngestClaim{}, false, nil
		}
	}

	if err := store.metaPut(key, claim); err != nil {
		return contracts.SourceIngestClaim{}, false, err
	}

	return claim, true, nil
}

// legacyClaimHeld reports whether a pre-migration on-disk lock file holds a live
// claim; expired legacy files are removed best-effort.
func (store *FilesystemStore) legacyClaimHeld(
	ref contracts.SourceObjectRef,
	now time.Time,
) (bool, error) {
	lockPath := store.sourceObjectLockPath(ref)
	var existing contracts.SourceIngestClaim
	if err := readJSON(lockPath, &existing); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No legacy lock file: nothing to honor.
			return false, nil
		}
		// A present-but-unreadable lock (corrupt/permission) is ambiguous;
		// surface it rather than silently allowing a second writer to acquire.
		return false, fmt.Errorf("read legacy source lock %s: %w", lockPath, err)
	}
	if sourceClaimLive(existing, now) {
		return true, nil
	}
	_ = os.Remove(lockPath)

	return false, nil
}

func (store *FilesystemStore) ReleaseSourceIngest(
	ctx context.Context,
	claim contracts.SourceIngestClaim,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := validateSourceObjectRef(claim.SourceObject); err != nil {
		return err
	}

	if strings.TrimSpace(claim.ClaimID) == "" {
		return errors.New("source ingest claim_id is required")
	}

	unlock := store.lockKey("source-ingest:" + sourceObjectRefKey(claim.SourceObject))
	defer unlock()

	key := sourceLockKey(claim.SourceObject)
	var existing contracts.SourceIngestClaim
	found, err := store.metaGet(key, &existing)
	if err != nil {
		return err
	}
	if found {
		if existing.ClaimID != claim.ClaimID {
			return errors.New("source ingest claim is owned by another writer")
		}

		return store.metaDelete(key)
	}

	// Legacy on-disk lock fallback for a pre-migration root.
	lockPath := store.sourceObjectLockPath(claim.SourceObject)
	var legacy contracts.SourceIngestClaim
	if err := readJSON(lockPath, &legacy); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}
	if legacy.ClaimID != claim.ClaimID {
		return errors.New("source ingest claim is owned by another writer")
	}
	if err := os.Remove(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
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

func sourceAliasKey(ref contracts.SourceObjectRef) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		ref.SourceKind,
		ref.SourceName,
		ref.ExternalID,
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
	return store.recordSourceObjectIndexesTo(ctx, store.syncSink(), digest, provenance)
}

func (store *FilesystemStore) recordSourceObjectIndexesTo(
	ctx context.Context,
	sink metaSink,
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
		if err := store.writePackedSourceObjectIndexTo(ctx, sink, entry); err != nil {
			return err
		}
		if err := store.writePackedSourceAliasTo(ctx, sink, ref, digest); err != nil {
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

	record := compoundParentIndexRecord{
		SchemaVersion: int(contracts.SchemaVersionPhase00),
		ChildDigest:   childDigest,
	}
	if packed, found, err := store.readPackedCompoundParentIndex(
		ctx,
		childDigest,
	); err != nil {
		return err
	} else if found {
		record = packed
	} else if err := readJSON(
		store.compoundParentIndexPath(childDigest),
		&record,
	); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
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

	return store.writePackedCompoundParentIndex(ctx, record)
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

	record, found, err := store.readPackedCompoundParentIndex(ctx, childDigest)
	if err != nil {
		return nil, err
	}
	if !found {
		err = readJSON(store.compoundParentIndexPath(childDigest), &record)
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
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
