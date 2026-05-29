// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zeebo/blake3"

	"blackcat.ca/gmeow/internal/contracts"
)

// The chunk store is the content-addressed data tier (roadmap Phase 2). Blob
// content is split into content-defined chunks (gear-hash FastCDC), each chunk
// is addressed by BLAKE3 and stored once (dedup), and chunks are appended into
// large immutable pack segments rather than one file per chunk. An object's
// content is reconstructed from an ordered recipe of chunk hashes.
//
// Storage layout under the FILESTORE root:
//
//	chunk-packs/<id>.pack            append-only zstd-compressed chunk bytes
//	chunk-index-v1/<2>/<2>/records.jsonl   chunk hash -> pack id, offset, length
//	object-recipe-v1/<2>/<2>/records.jsonl object digest -> ordered chunk hashes
//
// The index and recipe shards reuse the recovery-grade packed-shard primitives
// (length framing, flock, fsync, torn-tail repair) in packed_store.go. The pack
// trailer is implicit: every chunk's location is recorded in the index, and the
// index is itself rebuildable by scanning packs (each pack entry is independently
// zstd-framed). Packs are sealed at packTargetBytes and never mutated, so they
// are incremental-backup friendly.
const (
	chunkPacksDir     = "chunk-packs"
	packFilenameWidth = 10
	// packTargetBytes seals the active pack once it reaches this size.
	packTargetBytes = 256 * 1024 * 1024

	chunkMinSize = 2 * 1024
	chunkAvgSize = 8 * 1024
	chunkMaxSize = 64 * 1024
)

var gearTable = buildGearTable()

type chunkIndexEntry struct {
	UpdatedAt time.Time `json:"updated_at"`
	ChunkHash string    `json:"chunk_hash"`
	DictID    string    `json:"dict_id,omitempty"`
	PackID    uint64    `json:"pack_id"`
	Offset    int64     `json:"offset"`
	Length    int64     `json:"length"`
}

type objectRecipeEntry struct {
	UpdatedAt    time.Time              `json:"updated_at"`
	Digest       contracts.ObjectDigest `json:"digest"`
	ChunkHashes  []string               `json:"chunk_hashes"`
	ContentBytes int64                  `json:"content_bytes"`
}

func chunkIndexKey(chunkHash string) string {
	return "c/" + chunkHash
}

func objectRecipeKey(digest contracts.ObjectDigest) string {
	return "r/" + string(digest)
}

func (store *FilesystemStore) packPath(id uint64) string {
	return filepath.Join(
		store.root,
		chunkPacksDir,
		fmt.Sprintf("%0*d.pack", packFilenameWidth, id),
	)
}

// storeBlobContent chunks content, stores each unique chunk once, persists the
// recipe for the object digest, and returns the recipe. It is the write entry
// point for the chunk store.
func (store *FilesystemStore) storeBlobContent(
	ctx context.Context,
	digest contracts.ObjectDigest,
	content []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	recipe := objectRecipeEntry{
		UpdatedAt:    time.Now().UTC(),
		Digest:       digest,
		ContentBytes: int64(len(content)),
		ChunkHashes:  []string{},
	}

	for _, chunk := range chunkContent(content) {
		hash, err := store.storeChunk(ctx, chunk)
		if err != nil {
			return err
		}
		recipe.ChunkHashes = append(recipe.ChunkHashes, hash)
	}

	return store.metaPut(objectRecipeKey(digest), recipe)
}

// streamChunks splits content read from r into content-defined chunks — the
// same FastCDC boundaries chunkContent produces, since both only ever consider
// the first chunkMaxSize bytes when choosing a cut — and calls fn for each. It
// buffers at most ~chunkMaxSize bytes, so arbitrarily large objects stream
// without being held in memory. It returns the total content length.
func streamChunks(reader io.Reader, fn func(chunk []byte) error) (int64, error) {
	const (
		maskS = 0x0003590703530000
		maskL = 0x0000d90003530000
	)

	buffer := make([]byte, 0, chunkMaxSize*2)
	scratch := make([]byte, chunkMaxSize)
	var total int64
	atEOF := false

	for {
		for len(buffer) < chunkMaxSize && !atEOF {
			read, err := reader.Read(scratch)
			if read > 0 {
				buffer = append(buffer, scratch[:read]...)
			}
			if errors.Is(err, io.EOF) {
				atEOF = true
			} else if err != nil {
				return total, err
			}
		}
		if len(buffer) == 0 {
			return total, nil
		}

		cut := nextChunkCut(buffer, maskS, maskL)
		if err := fn(buffer[:cut]); err != nil {
			return total, err
		}
		total += int64(cut)
		remaining := copy(buffer, buffer[cut:])
		buffer = buffer[:remaining]
	}
}

// storeBlobReader streams content from r into the chunk store, computing the
// object's BLAKE3 (its content-addressed identity) and SHA-256 in the same
// pass, writes the recipe, and returns the digest, its uncompressed SHA-256, and
// its length. Content is never fully buffered, so multi-gigabyte objects are
// supported.
func (store *FilesystemStore) storeBlobReader(
	ctx context.Context,
	reader io.Reader,
) (contracts.ObjectDigest, string, int64, error) {
	if err := ctx.Err(); err != nil {
		return "", "", 0, err
	}

	blakeHash := blake3.New()
	shaHash := sha256.New()
	teed := io.TeeReader(reader, io.MultiWriter(blakeHash, shaHash))

	chunkHashes := []string{}
	contentBytes, err := streamChunks(teed, func(chunk []byte) error {
		hash, storeErr := store.storeChunk(ctx, chunk)
		if storeErr != nil {
			return storeErr
		}
		chunkHashes = append(chunkHashes, hash)

		return nil
	})
	if err != nil {
		return "", "", 0, err
	}

	digest := contracts.ObjectDigest(hex.EncodeToString(blakeHash.Sum(nil)))
	if err := store.metaPut(objectRecipeKey(digest), objectRecipeEntry{
		UpdatedAt:    time.Now().UTC(),
		Digest:       digest,
		ChunkHashes:  chunkHashes,
		ContentBytes: contentBytes,
	}); err != nil {
		return "", "", 0, err
	}

	return digest, hex.EncodeToString(shaHash.Sum(nil)), contentBytes, nil
}

// openBlobReader returns a streaming reader over an object's content,
// reconstructed chunk-by-chunk from its recipe (each chunk verified by its hash
// on read). When verifyDigest is non-empty the reader also accumulates the
// whole-content BLAKE3 and fails the final Read if it does not match the digest,
// preserving the content-addressed integrity guarantee without buffering the
// whole object. ok is false when the object has no recipe.
func (store *FilesystemStore) openBlobReader(
	digest contracts.ObjectDigest,
	verifyDigest contracts.ObjectDigest,
) (io.ReadCloser, bool, error) {
	recipe, ok, err := store.readRecipe(digest)
	if err != nil || !ok {
		return nil, ok, err
	}

	return &chunkRecipeReader{
		store:        store,
		hashes:       recipe.ChunkHashes,
		verifyDigest: string(verifyDigest),
		hasher:       blake3.New(),
	}, true, nil
}

type chunkRecipeReader struct {
	store        *FilesystemStore
	hasher       hash.Hash
	pending      []byte
	hashes       []string
	verifyDigest string
	index        int
	verified     bool
}

func (reader *chunkRecipeReader) Read(out []byte) (int, error) {
	for len(reader.pending) == 0 {
		if reader.index >= len(reader.hashes) {
			if reader.verifyDigest != "" && !reader.verified {
				reader.verified = true
				actual := hex.EncodeToString(reader.hasher.Sum(nil))
				if actual != reader.verifyDigest {
					return 0, fmt.Errorf(
						"CAS digest mismatch for %s: got %s",
						reader.verifyDigest, actual,
					)
				}
			}

			return 0, io.EOF
		}

		chunk, err := reader.store.readChunk(reader.hashes[reader.index])
		if err != nil {
			return 0, err
		}
		reader.index++
		if reader.verifyDigest != "" {
			_, _ = reader.hasher.Write(chunk)
		}
		reader.pending = chunk
	}

	written := copy(out, reader.pending)
	reader.pending = reader.pending[written:]

	return written, nil
}

func (reader *chunkRecipeReader) Close() error { return nil }

// hasRecipe reports whether the object's content is stored as chunks.
func (store *FilesystemStore) hasRecipe(digest contracts.ObjectDigest) (bool, error) {
	return store.metaHas(objectRecipeKey(digest))
}

func (store *FilesystemStore) readRecipe(
	digest contracts.ObjectDigest,
) (objectRecipeEntry, bool, error) {
	var entry objectRecipeEntry
	ok, err := store.metaGet(objectRecipeKey(digest), &entry)
	if err != nil {
		return objectRecipeEntry{}, false, err
	}

	return entry, ok, nil
}

// readBlobContent reconstructs an object's content from its chunk recipe.
func (store *FilesystemStore) readBlobContent(
	digest contracts.ObjectDigest,
) ([]byte, bool, error) {
	recipe, ok, err := store.readRecipe(digest)
	if err != nil || !ok {
		return nil, ok, err
	}

	content := make([]byte, 0, recipe.ContentBytes)
	for _, hash := range recipe.ChunkHashes {
		chunk, err := store.readChunk(hash)
		if err != nil {
			return nil, false, err
		}
		content = append(content, chunk...)
	}

	return content, true, nil
}

// storeChunk stores a single chunk if not already present and returns its hash.
func (store *FilesystemStore) storeChunk(
	ctx context.Context,
	chunk []byte,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	sum := blake3.Sum256(chunk)
	hash := hex.EncodeToString(sum[:])

	if _, ok, err := store.lookupChunk(hash); err != nil {
		return "", err
	} else if ok {
		return hash, nil
	}

	compressed, dictID, err := store.compressChunkContent(chunk)
	if err != nil {
		return "", err
	}

	store.packMu.Lock()
	defer store.packMu.Unlock()

	// Re-check under the lock: a concurrent writer may have stored it.
	if _, ok, err := store.lookupChunk(hash); err != nil {
		return "", err
	} else if ok {
		return hash, nil
	}

	packID, offset, err := store.appendToActivePack(compressed)
	if err != nil {
		return "", err
	}

	if err := store.metaPut(chunkIndexKey(hash), chunkIndexEntry{
		UpdatedAt: time.Now().UTC(),
		ChunkHash: hash,
		DictID:    dictID,
		PackID:    packID,
		Offset:    offset,
		Length:    int64(len(compressed)),
	}); err != nil {
		return "", err
	}

	return hash, nil
}

func (store *FilesystemStore) lookupChunk(hash string) (chunkIndexEntry, bool, error) {
	var entry chunkIndexEntry
	ok, err := store.metaGet(chunkIndexKey(hash), &entry)
	if err != nil {
		return chunkIndexEntry{}, false, err
	}

	return entry, ok, nil
}

func (store *FilesystemStore) readChunk(hash string) ([]byte, error) {
	entry, ok, err := store.lookupChunk(hash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("chunk %s not found in index", hash)
	}

	chunk, err := store.readChunkAt(hash, entry)
	if errors.Is(err, os.ErrNotExist) {
		// The pack was superseded by a concurrent repack (which updates the chunk
		// index before deleting the old pack). Re-resolve the chunk's new location
		// and retry once.
		store.packs.invalidate(entry.PackID)
		fresh, found, lookupErr := store.lookupChunk(hash)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if !found {
			return nil, fmt.Errorf("chunk %s not found in index", hash)
		}

		return store.readChunkAt(hash, fresh)
	}

	return chunk, err
}

func (store *FilesystemStore) readChunkAt(
	hash string,
	entry chunkIndexEntry,
) ([]byte, error) {
	file, release, err := store.packs.acquire(
		entry.PackID,
		func(id uint64) (*os.File, error) {
			return os.Open(store.packPath(id))
		},
	)
	if err != nil {
		return nil, err
	}
	defer release()

	compressed := make([]byte, entry.Length)
	if _, err := file.ReadAt(compressed, entry.Offset); err != nil {
		return nil, fmt.Errorf("read chunk %s from pack %d: %w", hash, entry.PackID, err)
	}

	chunk, err := store.decompressChunkContent(compressed, entry.DictID)
	if err != nil {
		return nil, fmt.Errorf("decompress chunk %s: %w", hash, err)
	}
	if blake3HexBytes(chunk) != hash {
		return nil, fmt.Errorf("chunk %s failed content verification", hash)
	}

	return chunk, nil
}

// readPackBytes reads raw (still-compressed) chunk bytes from a pack at a known
// offset/length, used by repack to relocate chunks verbatim.
func (store *FilesystemStore) readPackBytes(
	packID uint64,
	offset, length int64,
) ([]byte, error) {
	file, release, err := store.packs.acquire(packID, func(id uint64) (*os.File, error) {
		return os.Open(store.packPath(id))
	})
	if err != nil {
		return nil, err
	}
	defer release()

	raw := make([]byte, length)
	if _, err := file.ReadAt(raw, offset); err != nil {
		return nil, fmt.Errorf("read %d bytes from pack %d: %w", length, packID, err)
	}

	return raw, nil
}

// appendToActivePack appends compressed chunk bytes to the active (newest,
// not-yet-full) pack, sealing and rotating when it reaches packTargetBytes.
// Callers must hold store.packMu. It returns the pack id and the byte offset
// the chunk was written at.
func (store *FilesystemStore) appendToActivePack(
	compressed []byte,
) (uint64, int64, error) {
	packsDir := filepath.Join(store.root, chunkPacksDir)
	if err := os.MkdirAll(packsDir, 0o750); err != nil {
		return 0, 0, err
	}

	id, size, err := store.activePack(packsDir)
	if err != nil {
		return 0, 0, err
	}
	if size >= packTargetBytes {
		id++
		size = 0
	}

	path := store.packPath(id)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	if _, err := file.Write(compressed); err != nil {
		return 0, 0, err
	}
	if err := file.Sync(); err != nil {
		return 0, 0, err
	}
	if err := fsyncDir(packsDir); err != nil {
		return 0, 0, err
	}

	return id, size, nil
}

// activePack returns the highest-numbered pack id and its current size, or
// (0, 0) when no pack exists yet.
func (store *FilesystemStore) activePack(packsDir string) (uint64, int64, error) {
	entries, err := os.ReadDir(packsDir)
	if err != nil {
		return 0, 0, err
	}

	var (
		maxID uint64
		found bool
	)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".pack" {
			continue
		}
		var id uint64
		if _, scanErr := fmt.Sscanf(entry.Name(), "%d.pack", &id); scanErr != nil {
			continue
		}
		if !found || id > maxID {
			maxID = id
			found = true
		}
	}
	if !found {
		return 0, 0, nil
	}

	info, err := os.Stat(store.packPath(maxID))
	if err != nil {
		return 0, 0, err
	}

	return maxID, info.Size(), nil
}

// chunkContent splits content into content-defined chunks using the package
// gear table and the standard FastCDC normalized min/avg/max scheme.
func chunkContent(data []byte) [][]byte {
	const (
		maskS = 0x0003590703530000
		maskL = 0x0000d90003530000
	)

	chunks := make([][]byte, 0, len(data)/chunkAvgSize+1)
	for len(data) > 0 {
		cut := nextChunkCut(data, maskS, maskL)
		chunks = append(chunks, data[:cut])
		data = data[cut:]
	}

	return chunks
}

func nextChunkCut(data []byte, maskS, maskL uint64) int {
	n := len(data)
	if n <= chunkMinSize {
		return n
	}
	n = min(n, chunkMaxSize)
	normal := min(chunkAvgSize, n)

	var hash uint64
	i := chunkMinSize
	for ; i < normal; i++ {
		hash = (hash << 1) + gearTable[data[i]]
		if hash&maskS == 0 {
			return i
		}
	}
	for ; i < n; i++ {
		hash = (hash << 1) + gearTable[data[i]]
		if hash&maskL == 0 {
			return i
		}
	}

	return n
}

func buildGearTable() [256]uint64 {
	var table [256]uint64
	state := uint64(0x9e3779b97f4a7c15)
	for index := range table {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		table[index] = z
	}

	return table
}

func blake3HexBytes(content []byte) string {
	sum := blake3.Sum256(content)

	return hex.EncodeToString(sum[:])
}
