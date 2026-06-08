// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
)

// stateMagic versions the combined resolution-state artifact (claim-vector cache
// + entity index + ledger + observation memo). Bump on layout change, but keep
// the previous reader while live DEV states may still carry it.
const (
	stateMagic         = "GMEOWSTATE4"
	previousStateMagic = "GMEOWSTATE3"
)

// SnapshotState serializes the full resolution state — the claim-vector cache,
// the HNSW entity index, and the per-entity ledger — into one artifact the
// EMBEDDING service persists. Restoring it (LoadState) gives cross-run
// idempotency (a re-import resolves to the same entities and NOOPs) and a warm
// cache (no re-embedding). The cache and index are materialized views; the
// ledger is the minimal source-of-record set, so this artifact is the durable
// resolution state.
func (s *Service) SnapshotState() ([]byte, error) {
	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	var buf bytes.Buffer
	if _, err := buf.WriteString(stateMagic); err != nil {
		return nil, err
	}

	cacheBlob, err := encodeCache(s.cacheEntries())
	if err != nil {
		return nil, err
	}

	if err := writeLenBytes(&buf, cacheBlob); err != nil {
		return nil, err
	}

	indexBlob, err := s.index.Snapshot()
	if err != nil {
		return nil, err
	}

	if err := writeLenBytes(&buf, indexBlob); err != nil {
		return nil, err
	}

	ledgerBlob, err := encodeLedger(s.ledger)
	if err != nil {
		return nil, err
	}

	if err := writeLenBytes(&buf, ledgerBlob); err != nil {
		return nil, err
	}

	memoBlob, err := encodeStringMap(s.seenObs)
	if err != nil {
		return nil, err
	}

	if err := writeLenBytes(&buf, memoBlob); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// LoadState restores the cache, index, and ledger from a SnapshotState artifact,
// replacing the current state. It is intended to run at service startup before
// serving.
func (s *Service) LoadState(data []byte) error {
	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	reader := bytes.NewReader(data)

	magic := make([]byte, len(stateMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return fmt.Errorf("read state magic: %w", err)
	}

	magicValue := string(magic)
	if magicValue != stateMagic && magicValue != previousStateMagic {
		return errors.New("unrecognized resolution-state artifact magic")
	}

	cacheBlob, err := readLenBytes(reader)
	if err != nil {
		return err
	}

	entries, err := decodeCache(cacheBlob)
	if err != nil {
		return err
	}

	indexBlob, err := readLenBytes(reader)
	if err != nil {
		return err
	}

	index, err := LoadEntityIndex(indexBlob)
	if err != nil {
		return err
	}

	ledgerBlob, err := readLenBytes(reader)
	if err != nil {
		return err
	}

	ledger, err := decodeLedger(ledgerBlob, magicValue == stateMagic)
	if err != nil {
		return err
	}

	memoBlob, err := readLenBytes(reader)
	if err != nil {
		return err
	}

	memo, err := decodeStringMap(memoBlob)
	if err != nil {
		return err
	}

	if cache, ok := s.resolver.Cache().(*MemoryCache); ok {
		cache.load(entries)
	}

	s.index = index
	s.ledger = ledger
	s.seenObs = memo
	s.entityClaims = make(
		map[string][]scoredClaim,
	) // rebuilt lazily from the loaded ledger

	return nil
}

// encodeStringMap serializes a string->string map (sorted keys) as a
// length-prefixed block.
func encodeStringMap(m map[string]string) ([]byte, error) {
	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(m))); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		if err := writeLenString(&buf, k); err != nil {
			return nil, err
		}

		if err := writeLenString(&buf, m[k]); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func decodeStringMap(data []byte) (map[string]string, error) {
	reader := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	out := make(map[string]string, count)
	for range count {
		key, err := readLenString(reader)
		if err != nil {
			return nil, err
		}

		value, err := readLenString(reader)
		if err != nil {
			return nil, err
		}

		out[key] = value
	}

	return out, nil
}

// cacheEntries returns the claim-vector cache contents if it is a MemoryCache,
// else nil (a non-snapshotable cache persists no vectors).
func (s *Service) cacheEntries() map[string]Vector {
	if cache, ok := s.resolver.Cache().(*MemoryCache); ok {
		return cache.snapshot()
	}

	return nil
}

func encodeCache(entries map[string]Vector) ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, uint32(len(entries)))
	if err != nil {
		return nil, err
	}

	hashes := make([]string, 0, len(entries))
	for hash := range entries {
		hashes = append(hashes, hash)
	}

	sort.Strings(hashes)

	for _, hash := range hashes {
		err := writeLenString(&buf, hash)
		if err != nil {
			return nil, err
		}

		err = writeVector(&buf, entries[hash])
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func decodeCache(data []byte) (map[string]Vector, error) {
	reader := bytes.NewReader(data)

	var count uint32

	err := binary.Read(reader, binary.LittleEndian, &count)
	if err != nil {
		return nil, err
	}

	entries := make(map[string]Vector, count)
	for range count {
		hash, err := readLenString(reader)
		if err != nil {
			return nil, err
		}

		vector, err := readVector(reader)
		if err != nil {
			return nil, err
		}

		entries[hash] = vector
	}

	return entries, nil
}

func encodeLedger(ledger *entityLedger) ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(
		&buf,
		binary.LittleEndian,
		uint32(len(ledger.claims)),
	)
	if err != nil {
		return nil, err
	}

	entities := make([]string, 0, len(ledger.claims))
	for entity := range ledger.claims {
		entities = append(entities, entity)
	}

	sort.Strings(entities)

	for _, entity := range entities {
		err := writeLenString(&buf, entity)
		if err != nil {
			return nil, err
		}

		claims := ledger.claims[entity]

		err = binary.Write(&buf, binary.LittleEndian, uint32(len(claims)))
		if err != nil {
			return nil, err
		}

		hashes := make([]string, 0, len(claims))
		for hash := range claims {
			hashes = append(hashes, hash)
		}

		sort.Strings(hashes)

		for _, hash := range hashes {
			entry := claims[hash]
			if err := writeLenString(&buf, hash); err != nil {
				return nil, err
			}
			if err := writeLenString(&buf, entry.Text); err != nil {
				return nil, err
			}
			if err := writeLenString(&buf, entry.ValidFrom); err != nil {
				return nil, err
			}
			if err := writeLenString(&buf, entry.ValidUntil); err != nil {
				return nil, err
			}
		}
	}

	if err := binary.Write(
		&buf,
		binary.LittleEndian,
		uint32(ledger.observations),
	); err != nil {
		return nil, err
	}

	hashes := make([]string, 0, len(ledger.occurrences))
	for hash := range ledger.occurrences {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)

	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(hashes))); err != nil {
		return nil, err
	}
	for _, hash := range hashes {
		if err := writeLenString(&buf, hash); err != nil {
			return nil, err
		}
		if err := binary.Write(
			&buf,
			binary.LittleEndian,
			uint32(ledger.occurrences[hash]),
		); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func decodeLedger(data []byte, hasOccurrenceStats bool) (*entityLedger, error) {
	reader := bytes.NewReader(data)
	ledger := newEntityLedger()

	var entityCount uint32

	err := binary.Read(reader, binary.LittleEndian, &entityCount)
	if err != nil {
		return nil, err
	}

	for range entityCount {
		entity, err := readLenString(reader)
		if err != nil {
			return nil, err
		}

		var claimCount uint32
		if err := binary.Read(reader, binary.LittleEndian, &claimCount); err != nil {
			return nil, err
		}

		set := make(map[string]claimEntry, claimCount)
		for range claimCount {
			hash, err := readLenString(reader)
			if err != nil {
				return nil, err
			}

			text, err := readLenString(reader)
			if err != nil {
				return nil, err
			}

			validFrom, err := readLenString(reader)
			if err != nil {
				return nil, err
			}

			validUntil, err := readLenString(reader)
			if err != nil {
				return nil, err
			}

			set[hash] = claimEntry{Text: text, ValidFrom: validFrom, ValidUntil: validUntil}
		}

		ledger.claims[entity] = set
	}

	if hasOccurrenceStats {
		var observations uint32
		if err := binary.Read(reader, binary.LittleEndian, &observations); err != nil {
			return nil, err
		}
		ledger.observations = int(observations)

		var occurrenceCount uint32
		if err := binary.Read(reader, binary.LittleEndian, &occurrenceCount); err != nil {
			return nil, err
		}
		for range occurrenceCount {
			hash, err := readLenString(reader)
			if err != nil {
				return nil, err
			}

			var count uint32
			if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
				return nil, err
			}

			ledger.occurrences[hash] = int(count)
		}
	}

	ledger.rebuild() // df + identifier index are not serialized; recompute from the claims
	if !hasOccurrenceStats {
		ledger.seedObservationStatsFromEntities()
	}

	return ledger, nil
}

func writeVector(w io.Writer, vector Vector) error {
	err := binary.Write(w, binary.LittleEndian, uint32(len(vector)))
	if err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, vector)
}

// maxVectorDim bounds a deserialized vector dimension so a corrupt or hostile
// state file cannot trigger a huge allocation (OOM DoS). It sits far above any
// real embedding width (FullDim is 768).
const maxVectorDim = 1 << 16

// errVectorDimTooLarge guards readVector against an OOM from a corrupt dimension.
var errVectorDimTooLarge = errors.New("vector dimension exceeds limit")

func readVector(r io.Reader) (Vector, error) {
	var dim uint32

	err := binary.Read(r, binary.LittleEndian, &dim)
	if err != nil {
		return nil, err
	}

	if dim > maxVectorDim {
		return nil, fmt.Errorf("%w: %d (max %d)", errVectorDimTooLarge, dim, maxVectorDim)
	}

	vector := make(Vector, dim)

	err = binary.Read(r, binary.LittleEndian, vector)
	if err != nil {
		return nil, err
	}

	return vector, nil
}
