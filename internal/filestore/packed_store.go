// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	packedSourceIndexDir         = "source-index-v2"
	packedSourceAliasIndexDir    = "source-alias-index-v2"
	packedCompoundParentIndexDir = "compound-parent-index-v2"
	packedRecoveryDir            = "recovery-v2"
	packedShardFilename          = "records.jsonl"
	// packedRecordMaxBytes caps each JSON record written into a shard. Reject
	// writes above this so that a single bloated record cannot brick a shard
	// (G10). Readers tolerate larger lines up to packedShardMaxLineBytes so
	// that legacy data can still be read out for compaction.
	packedRecordMaxBytes = 1 << 20 // 1 MiB
	// packedShardMaxLineBytes is the read-side maximum line length. Anything
	// above this is treated as a torn-tail-like read error.
	packedShardMaxLineBytes = 2 << 20 // 2 MiB
	// packedRecordLengthPrefixWidth is the number of hex digits in the framing
	// prefix: <8-hex-length>\t<json-payload>\n
	packedRecordLengthPrefixWidth = 8
)

// ErrPackedRecordTooLarge is returned when an encoded record would exceed
// packedRecordMaxBytes (G10).
var ErrPackedRecordTooLarge = errors.New("packed record exceeds size limit")

type packedSourceObjectIndexEntry struct {
	SourceObject contracts.SourceObjectRef `json:"source_object"`
	ObjectDigest contracts.ObjectDigest    `json:"object_digest"`
	UpdatedAt    time.Time                 `json:"updated_at"`
}

type packedCompoundParentIndexEntry struct {
	ChildDigest contracts.ObjectDigest `json:"child_digest"`
	Parents     []compoundParentEdge   `json:"parents"`
	UpdatedAt   time.Time              `json:"updated_at"`
}

type packedSourceAliasEntry struct {
	SourceKind string                 `json:"source_kind"`
	SourceName string                 `json:"source_name"`
	ExternalID string                 `json:"external_id"`
	Digest     contracts.ObjectDigest `json:"digest"`
	UpdatedAt  time.Time              `json:"updated_at"`
}

type packedRecoveryEntry struct {
	Digest    contracts.ObjectDigest `json:"digest"`
	Recovery  recoverySidecar        `json:"recovery"`
	UpdatedAt time.Time              `json:"updated_at"`
}

func (store *FilesystemStore) writePackedSourceObjectIndex(
	ctx context.Context,
	entry sourceObjectIndexEntry,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sourceObjectRefKey(entry.SourceObject)
	return store.appendPackedRecord(
		ctx,
		"source-index-v2:"+key,
		store.packedSourceIndexShardPath(key),
		packedSourceObjectIndexEntry(entry),
	)
}

func (store *FilesystemStore) readPackedSourceObjectIndex(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (sourceObjectIndexEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return sourceObjectIndexEntry{}, false, err
	}
	key := sourceObjectRefKey(ref)
	path := store.packedSourceIndexShardPath(key)
	var found sourceObjectIndexEntry
	ok := false
	err := scanPackedRecords(path, func(record packedSourceObjectIndexEntry) error {
		if sourceObjectRefsEqual(record.SourceObject, ref) {
			found = sourceObjectIndexEntry(record)
			ok = true
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return sourceObjectIndexEntry{}, false, nil
	}
	if err != nil {
		return sourceObjectIndexEntry{}, false, err
	}

	return found, ok, nil
}

func (store *FilesystemStore) writePackedSourceAlias(
	ctx context.Context,
	ref contracts.SourceObjectRef,
	digest contracts.ObjectDigest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sourceAliasKey(ref)
	return store.appendPackedRecord(
		ctx,
		"source-alias-index-v2:"+key,
		store.packedSourceAliasIndexShardPath(key),
		packedSourceAliasEntry{
			SourceKind: ref.SourceKind,
			SourceName: ref.SourceName,
			ExternalID: ref.ExternalID,
			Digest:     digest,
			UpdatedAt:  time.Now().UTC(),
		},
	)
}

func (store *FilesystemStore) readPackedSourceAlias(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	key := sourceAliasKey(ref)
	path := store.packedSourceAliasIndexShardPath(key)
	var found contracts.ObjectDigest
	ok := false
	err := scanPackedRecords(path, func(record packedSourceAliasEntry) error {
		if record.SourceKind == ref.SourceKind &&
			record.SourceName == ref.SourceName &&
			record.ExternalID == ref.ExternalID {
			found = record.Digest
			ok = true
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return found, ok, nil
}

func (store *FilesystemStore) writePackedCompoundParentIndex(
	ctx context.Context,
	record compoundParentIndexRecord,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := string(record.ChildDigest)
	return store.appendPackedRecord(
		ctx,
		"compound-parent-index-v2:"+key,
		store.packedCompoundParentShardPath(key),
		packedCompoundParentIndexEntry{
			ChildDigest: record.ChildDigest,
			Parents:     append([]compoundParentEdge{}, record.Parents...),
			UpdatedAt:   time.Now().UTC(),
		},
	)
}

func (store *FilesystemStore) readPackedCompoundParentIndex(
	ctx context.Context,
	childDigest contracts.ObjectDigest,
) (compoundParentIndexRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return compoundParentIndexRecord{}, false, err
	}
	key := string(childDigest)
	path := store.packedCompoundParentShardPath(key)
	var found compoundParentIndexRecord
	ok := false
	err := scanPackedRecords(path, func(record packedCompoundParentIndexEntry) error {
		if record.ChildDigest == childDigest {
			found = compoundParentIndexRecord{
				SchemaVersion: int(contracts.SchemaVersionPhase00),
				ChildDigest:   record.ChildDigest,
				Parents:       append([]compoundParentEdge{}, record.Parents...),
			}
			ok = true
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return compoundParentIndexRecord{}, false, nil
	}
	if err != nil {
		return compoundParentIndexRecord{}, false, err
	}

	return found, ok, nil
}

func (store *FilesystemStore) writePackedRecovery(
	ctx context.Context,
	digest contracts.ObjectDigest,
	recovery recoverySidecar,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := string(digest)
	return store.appendPackedRecord(
		ctx,
		"recovery-v2:"+key,
		store.packedRecoveryShardPath(key),
		packedRecoveryEntry{
			Digest:    digest,
			Recovery:  recovery,
			UpdatedAt: time.Now().UTC(),
		},
	)
}

func (store *FilesystemStore) readPackedRecovery(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (recoverySidecar, bool, error) {
	if err := ctx.Err(); err != nil {
		return recoverySidecar{}, false, err
	}
	key := string(digest)
	path := store.packedRecoveryShardPath(key)
	var found recoverySidecar
	ok := false
	err := scanPackedRecords(path, func(record packedRecoveryEntry) error {
		if record.Digest == digest {
			found = record.Recovery
			ok = true
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return recoverySidecar{}, false, nil
	}
	if err != nil {
		return recoverySidecar{}, false, err
	}

	return found, ok, nil
}

// appendPackedRecord serializes a record, enforces the per-record size cap
// (G10), and appends it to the shard with the length-prefixed framing (G1).
// Inter-process exclusion is provided by an advisory file lock on the shard
// itself (G2/G3) — the kernel releases the lock on process exit, so there is
// no on-disk lock state that could survive a backup or a crash.
func (store *FilesystemStore) appendPackedRecord(
	ctx context.Context,
	lockKey string,
	path string,
	record any,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock := store.lockKey(lockKey)
	defer unlock()

	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(encoded) > packedRecordMaxBytes {
		return fmt.Errorf(
			"%w: %d bytes exceeds %d",
			ErrPackedRecordTooLarge, len(encoded), packedRecordMaxBytes,
		)
	}
	framed := encodePackedShardLine(encoded)

	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if err := flockExclusive(ctx, file); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(framed); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	// Closing the fd releases the advisory lock; the kernel guarantees this
	// even if the process is killed before reaching this line, which is the
	// property that makes flock backup-safe.
	if err := file.Close(); err != nil {
		return err
	}

	return fsyncDir(filepath.Dir(path))
}

// flockExclusive acquires an advisory exclusive lock on the file, polling
// every 100ms so a cancelled context is honored. The lock is released when
// the file descriptor is closed (whether by us or by the kernel on process
// exit).
func flockExclusive(ctx context.Context, file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("flock %s: %w", file.Name(), err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// encodePackedShardLine builds the on-disk framing for one record:
//
//	<8-hex-length>\t<json-payload>\n
//
// Length is the byte count of the JSON payload. The framing lets the reader
// detect a torn tail (length prefix missing, malformed, or payload shorter
// than declared) without confusing it with mid-file corruption.
func encodePackedShardLine(payload []byte) []byte {
	line := make([]byte, 0, packedRecordLengthPrefixWidth+1+len(payload)+1)
	prefix := fmt.Sprintf("%08x", len(payload))
	line = append(line, prefix...)
	line = append(line, '\t')
	line = append(line, payload...)
	line = append(line, '\n')
	return line
}

// decodePackedShardLine extracts the JSON payload bytes from a single line.
// New-format lines carry the length-prefixed framing; legacy-format lines
// (pre-framing) are accepted as bare JSON for backward compatibility.
func decodePackedShardLine(line []byte) ([]byte, error) {
	if len(line) >= packedRecordLengthPrefixWidth+1 &&
		line[packedRecordLengthPrefixWidth] == '\t' {
		prefix := string(line[:packedRecordLengthPrefixWidth])
		length, err := strconv.ParseUint(prefix, 16, 32)
		if err == nil {
			payload := line[packedRecordLengthPrefixWidth+1:]
			if int64(len(payload)) != int64(length) {
				return nil, fmt.Errorf(
					"length prefix %d does not match payload bytes %d",
					length, len(payload),
				)
			}
			if !json.Valid(payload) {
				return nil, errors.New("payload is not valid JSON")
			}
			return payload, nil
		}
	}
	if json.Valid(line) {
		return line, nil
	}
	return nil, errors.New("line is neither length-prefixed nor valid JSON")
}

// packedShardScanResult describes what scanPackedShard observed. A non-zero
// TornTailBytes means the file ends with a partial/garbled record that can be
// safely truncated by Verify's repair mode (G4 + G1). LastIntactEnd is the
// byte offset to truncate to.
type packedShardScanResult struct {
	Records       int
	LastIntactEnd int64
	TornTailBytes int64
	TornTailErr   error
}

// scanPackedShard streams a packed JSONL shard. For every intact record it
// calls handle with the decoded JSON payload bytes. A torn tail (unparseable
// last record) is reported via the returned result without producing an
// error, so ordinary readers can continue past it and verify --repair can
// truncate it. A mid-file corrupt record IS surfaced as an error because
// truncation cannot recover it.
func scanPackedShard(
	path string,
	handle func([]byte) error,
) (packedShardScanResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return packedShardScanResult{}, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 64*1024)
	var result packedShardScanResult

	var (
		pendingPayload   []byte
		pendingDecodeErr error
		pendingRawLen    int64
		havePending      bool
		cursor           int64
	)

	commitPending := func(asLast bool) error {
		if !havePending {
			return nil
		}
		if pendingDecodeErr != nil {
			if asLast {
				result.TornTailBytes = pendingRawLen
				result.TornTailErr = pendingDecodeErr
				havePending = false
				return nil
			}
			return fmt.Errorf(
				"decode packed record at offset %d in %s: %w",
				cursor, path, pendingDecodeErr,
			)
		}
		if err := handle(pendingPayload); err != nil {
			return err
		}
		result.Records++
		cursor += pendingRawLen
		result.LastIntactEnd = cursor
		havePending = false
		return nil
	}

	for {
		line, readErr := reader.ReadSlice('\n')
		atEOF := errors.Is(readErr, io.EOF)
		hitTooLong := errors.Is(readErr, bufio.ErrBufferFull)
		if !atEOF && !hitTooLong && readErr != nil {
			return packedShardScanResult{}, readErr
		}

		// ReadSlice returns a slice into the reader's internal buffer; we must
		// copy if we want to hold it past the next read.
		copied := append([]byte(nil), line...)

		if hitTooLong {
			// ReadSlice hit its buffer limit, not the packed record limit.
			// Collect the whole line and then apply packedShardMaxLineBytes.
			fullLine, terminated, tooLarge, drainErr := drainPackedLongLine(reader, copied)
			if drainErr != nil {
				return packedShardScanResult{}, drainErr
			}
			if err := commitPending(false); err != nil {
				return packedShardScanResult{}, err
			}
			if tooLarge && terminated {
				return packedShardScanResult{}, fmt.Errorf(
					"packed record at offset %d in %s exceeds %d bytes",
					cursor, path, packedShardMaxLineBytes,
				)
			}
			if !terminated {
				result.TornTailBytes = int64(len(fullLine))
				if tooLarge {
					result.TornTailErr = fmt.Errorf(
						"torn tail exceeds %d bytes",
						packedShardMaxLineBytes,
					)
				} else {
					result.TornTailErr = errors.New("packed record not terminated by newline")
				}
				return result, nil
			}

			rawLen := int64(len(fullLine))
			payload := fullLine[:len(fullLine)-1]
			decoded, decodeErr := decodePackedShardLine(payload)
			pendingPayload = decoded
			pendingDecodeErr = decodeErr
			pendingRawLen = rawLen
			havePending = true
			continue
		}

		if len(copied) == 0 && atEOF {
			if err := commitPending(true); err != nil {
				return packedShardScanResult{}, err
			}
			return result, nil
		}

		terminated := len(copied) > 0 && copied[len(copied)-1] == '\n'
		rawLen := int64(len(copied))
		payload := copied
		if terminated {
			payload = copied[:len(copied)-1]
		}

		if len(payload) == 0 {
			// Empty line (just a stray \n). Commit any pending and skip.
			if err := commitPending(false); err != nil {
				return packedShardScanResult{}, err
			}
			cursor += rawLen
			result.LastIntactEnd = cursor
			if atEOF {
				return result, nil
			}
			continue
		}

		// A new non-empty payload arrived. Anything pending is therefore not
		// the last record → commit it (with strict mid-file semantics).
		if err := commitPending(false); err != nil {
			return packedShardScanResult{}, err
		}

		if !terminated {
			// Only possible at EOF: unterminated final line is always torn.
			if !atEOF {
				return packedShardScanResult{}, fmt.Errorf(
					"unexpected partial line at offset %d in %s", cursor, path,
				)
			}
			decoded, decodeErr := decodePackedShardLine(payload)
			result.TornTailBytes = rawLen
			if decodeErr != nil {
				result.TornTailErr = decodeErr
			} else {
				// Even if it decoded as bare JSON, the missing \n means a torn
				// write — we always write \n, so this record cannot be
				// considered durable.
				_ = decoded
				result.TornTailErr = errors.New(
					"packed record not terminated by newline",
				)
			}
			return result, nil
		}

		decoded, decodeErr := decodePackedShardLine(payload)
		pendingPayload = decoded
		pendingDecodeErr = decodeErr
		pendingRawLen = rawLen
		havePending = true

		if atEOF {
			if err := commitPending(true); err != nil {
				return packedShardScanResult{}, err
			}
			return result, nil
		}
	}
}

// drainPackedLongLine continues reading after ReadSlice hit its buffer limit.
// The line is valid if it terminates before packedShardMaxLineBytes.
func drainPackedLongLine(
	reader *bufio.Reader,
	initial []byte,
) ([]byte, bool, bool, error) {
	collected := append([]byte(nil), initial...)
	tooLarge := int64(len(collected)) > packedShardMaxLineBytes
	for {
		chunk, err := reader.ReadSlice('\n')
		collected = append(collected, chunk...)
		if int64(len(collected)) > packedShardMaxLineBytes {
			tooLarge = true
		}
		if errors.Is(err, io.EOF) {
			return collected, false, tooLarge, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, false, false, err
		}
		return collected, true, tooLarge, nil
	}
}

// scanPackedRecords decodes shard records of type T into fn. Torn tails are
// silently skipped — ordinary readers should never error on a partial write
// at the file tail. Mid-file corruption still surfaces as an error.
func scanPackedRecords[T any](path string, fn func(T) error) error {
	_, err := scanPackedShard(path, func(payload []byte) error {
		var record T
		if err := json.Unmarshal(payload, &record); err != nil {
			return fmt.Errorf("decode packed record %s: %w", path, err)
		}
		return fn(record)
	})
	return err
}

// truncatePackedShardTornTail truncates the shard file at the given offset and
// fsyncs the containing directory. Used by verify --repair when scanPackedShard
// reports a torn tail. The caller is responsible for holding any necessary
// in-process lock.
func truncatePackedShardTornTail(
	ctx context.Context,
	path string,
	lastIntactEnd int64,
) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := flockExclusive(ctx, file); err != nil {
		return err
	}
	if err := file.Truncate(lastIntactEnd); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(path))
}

func (store *FilesystemStore) packedSourceIndexShardPath(key string) string {
	return filepath.Join(
		store.root,
		packedSourceIndexDir,
		key[:2],
		key[2:4],
		packedShardFilename,
	)
}

func (store *FilesystemStore) packedSourceAliasIndexShardPath(key string) string {
	return filepath.Join(
		store.root,
		packedSourceAliasIndexDir,
		key[:2],
		key[2:4],
		packedShardFilename,
	)
}

func (store *FilesystemStore) packedCompoundParentShardPath(key string) string {
	return filepath.Join(
		store.root,
		packedCompoundParentIndexDir,
		"blake3",
		key[:2],
		key[2:4],
		packedShardFilename,
	)
}

func (store *FilesystemStore) packedRecoveryShardPath(key string) string {
	return filepath.Join(
		store.root,
		packedRecoveryDir,
		"blake3",
		key[:2],
		key[2:4],
		packedShardFilename,
	)
}
