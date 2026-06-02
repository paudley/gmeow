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
// + entity index + ledger). Bump on layout change.
const stateMagic = "GMEOWSTATE1"

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
	if string(magic) != stateMagic {
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
	ledger, err := decodeLedger(ledgerBlob)
	if err != nil {
		return err
	}

	if cache, ok := s.resolver.Cache().(*MemoryCache); ok {
		cache.load(entries)
	}
	s.index = index
	s.ledger = ledger

	return nil
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
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(entries))); err != nil {
		return nil, err
	}
	hashes := make([]string, 0, len(entries))
	for hash := range entries {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	for _, hash := range hashes {
		if err := writeLenString(&buf, hash); err != nil {
			return nil, err
		}
		if err := writeVector(&buf, entries[hash]); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func decodeCache(data []byte) (map[string]Vector, error) {
	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
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
	if err := binary.Write(
		&buf,
		binary.LittleEndian,
		uint32(len(ledger.claims)),
	); err != nil {
		return nil, err
	}
	entities := make([]string, 0, len(ledger.claims))
	for entity := range ledger.claims {
		entities = append(entities, entity)
	}
	sort.Strings(entities)
	for _, entity := range entities {
		if err := writeLenString(&buf, entity); err != nil {
			return nil, err
		}
		claims := ledger.claims[entity]
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(claims))); err != nil {
			return nil, err
		}
		hashes := make([]string, 0, len(claims))
		for hash := range claims {
			hashes = append(hashes, hash)
		}
		sort.Strings(hashes)
		for _, hash := range hashes {
			if err := writeLenString(&buf, hash); err != nil {
				return nil, err
			}
			if err := writeLenString(&buf, claims[hash]); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

func decodeLedger(data []byte) (*entityLedger, error) {
	reader := bytes.NewReader(data)
	ledger := newEntityLedger()
	var entityCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &entityCount); err != nil {
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
		set := make(map[string]string, claimCount)
		for range claimCount {
			hash, err := readLenString(reader)
			if err != nil {
				return nil, err
			}
			text, err := readLenString(reader)
			if err != nil {
				return nil, err
			}
			set[hash] = text
		}
		ledger.claims[entity] = set
	}

	return ledger, nil
}

func writeVector(w io.Writer, vector Vector) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(vector))); err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, vector)
}

func readVector(r io.Reader) (Vector, error) {
	var dim uint32
	if err := binary.Read(r, binary.LittleEndian, &dim); err != nil {
		return nil, err
	}
	vector := make(Vector, dim)
	if err := binary.Read(r, binary.LittleEndian, vector); err != nil {
		return nil, err
	}

	return vector, nil
}
