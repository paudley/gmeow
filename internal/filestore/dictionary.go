// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Chunk content is zstd-compressed with an optional trained dictionary (roadmap
// Phase 4). A dictionary lets small, similar records (email bodies, headers)
// compress far better than they can in isolation, while keeping each chunk
// independently addressable and decompressible. Each chunk records the id of
// the dictionary it was compressed with (empty = none), so dictionaries can be
// rotated/versioned and old and new chunks coexist; a future recompaction pass
// can re-compress old chunks under a newer dictionary.
//
// Dictionaries live under <root>/dictionaries/: `<id>.dict` holds the raw zstd
// dictionary bytes and the `current` marker names the active id. With no marker
// (the common bootstrap state) compression falls back to plain zstd. Training a
// dictionary requires a representative corpus and is an operator/recompaction
// step; this code is the mechanism that uses one once it exists.
const (
	dictionariesDir   = "dictionaries"
	currentDictMarker = "current"
)

type dictionaryCache struct {
	mu       sync.Mutex
	id       string
	bytes    []byte
	loaded   bool
	encoders map[string]*zstd.Encoder
	decoders map[string]*zstd.Decoder
}

func (store *FilesystemStore) activeDictionary() (string, []byte, error) {
	store.dicts.mu.Lock()
	defer store.dicts.mu.Unlock()

	if !store.dicts.loaded {
		id, dictBytes, err := store.loadActiveDictionaryLocked()
		if err != nil {
			return "", nil, err
		}
		store.dicts.id = id
		store.dicts.bytes = dictBytes
		store.dicts.loaded = true
	}

	return store.dicts.id, store.dicts.bytes, nil
}

func (store *FilesystemStore) loadActiveDictionaryLocked() (string, []byte, error) {
	marker := filepath.Join(store.root, dictionariesDir, currentDictMarker)
	data, err := os.ReadFile(marker)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	id := strings.TrimSpace(string(data))
	if id == "" {
		return "", nil, nil
	}
	dictBytes, err := os.ReadFile(store.dictionaryPath(id))
	if err != nil {
		return "", nil, err
	}

	return id, dictBytes, nil
}

// reloadDictionaries forces the next activeDictionary call to re-read the
// current marker, so a freshly trained dictionary takes effect for new chunks
// without a process restart. Cached encoders/decoders are dropped (not closed,
// to avoid racing in-flight EncodeAll/DecodeAll calls — the old codecs are
// concurrency-safe and become unreferenced).
func (store *FilesystemStore) reloadDictionaries() {
	store.dicts.mu.Lock()
	defer store.dicts.mu.Unlock()
	store.dicts.loaded = false
	store.dicts.id = ""
	store.dicts.bytes = nil
	store.dicts.encoders = nil
	store.dicts.decoders = nil
}

func (store *FilesystemStore) dictionaryPath(id string) string {
	return filepath.Join(store.root, dictionariesDir, id+".dict")
}

func (store *FilesystemStore) dictEncoder(
	id string,
	dict []byte,
) (*zstd.Encoder, error) {
	store.dicts.mu.Lock()
	defer store.dicts.mu.Unlock()

	if store.dicts.encoders == nil {
		store.dicts.encoders = map[string]*zstd.Encoder{}
	}
	if encoder, ok := store.dicts.encoders[id]; ok {
		return encoder, nil
	}

	var options []zstd.EOption
	if len(dict) > 0 {
		rawID, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid dictionary id %q: %w", id, err)
		}
		options = append(options, zstd.WithEncoderDictRaw(uint32(rawID), dict))
	}
	encoder, err := zstd.NewWriter(nil, options...)
	if err != nil {
		return nil, err
	}
	store.dicts.encoders[id] = encoder

	return encoder, nil
}

func (store *FilesystemStore) dictDecoder(id string) (*zstd.Decoder, error) {
	store.dicts.mu.Lock()
	defer store.dicts.mu.Unlock()

	if store.dicts.decoders == nil {
		store.dicts.decoders = map[string]*zstd.Decoder{}
	}
	if decoder, ok := store.dicts.decoders[id]; ok {
		return decoder, nil
	}

	var options []zstd.DOption
	if id != "" {
		dict, err := os.ReadFile(store.dictionaryPath(id))
		if err != nil {
			return nil, err
		}
		rawID, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid dictionary id %q: %w", id, err)
		}
		options = append(options, zstd.WithDecoderDictRaw(uint32(rawID), dict))
	}
	decoder, err := zstd.NewReader(nil, options...)
	if err != nil {
		return nil, err
	}
	store.dicts.decoders[id] = decoder

	return decoder, nil
}

// compressChunkContent compresses a chunk with the active dictionary (if any)
// and returns the compressed bytes plus the dictionary id used (empty = none).
func (store *FilesystemStore) compressChunkContent(
	chunk []byte,
) ([]byte, string, error) {
	id, dict, err := store.activeDictionary()
	if err != nil {
		return nil, "", err
	}
	encoder, err := store.dictEncoder(id, dict)
	if err != nil {
		return nil, "", err
	}

	return encoder.EncodeAll(chunk, nil), id, nil
}

// decompressChunkContent decompresses chunk bytes that were compressed with the
// dictionary identified by dictID (empty = plain zstd).
func (store *FilesystemStore) decompressChunkContent(
	compressed []byte,
	dictID string,
) ([]byte, error) {
	decoder, err := store.dictDecoder(dictID)
	if err != nil {
		return nil, err
	}

	return decoder.DecodeAll(compressed, nil)
}
