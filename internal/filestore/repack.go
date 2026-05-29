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
	"time"
)

// RepackReport summarizes a repack pass.
type RepackReport struct {
	PacksScanned  int   `json:"packs_scanned"`
	PacksRepacked int   `json:"packs_repacked"`
	PacksRemoved  int   `json:"packs_removed"`
	ChunksMoved   int   `json:"chunks_moved"`
	BytesBefore   int64 `json:"bytes_before"`
	BytesAfter    int64 `json:"bytes_after"`
}

// Repack reclaims the on-disk bytes Gc left behind. Gc removes dead chunks from
// the index but not from the append-only packs; Repack copies each sealed pack's
// still-live chunks (verbatim, preserving their compression/dictionary) into the
// active pack, updates the chunk index to the new locations, and deletes the old
// pack. The chunk index is updated before the old pack is removed, and readChunk
// retries on a superseded pack, so repack runs safely while the store serves.
//
// Run Gc before Repack; do not run them concurrently (Repack would otherwise
// risk re-creating an index entry Gc just removed — it guards against this by
// re-checking the entry before rewriting, dropping the relocated bytes as dead
// space for the next pass).
func (store *FilesystemStore) Repack(ctx context.Context) (RepackReport, error) {
	if err := ctx.Err(); err != nil {
		return RepackReport{}, err
	}

	report := RepackReport{}
	packsDir := filepath.Join(store.root, chunkPacksDir)

	activeID, _, err := store.activePack(packsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return report, nil
		}

		return report, err
	}

	// After Gc, the chunk index holds only live chunks; group them by pack so any
	// pack whose file is larger than its live bytes has reclaimable dead space.
	livePerPack := map[uint64][]chunkIndexEntry{}
	liveBytes := map[uint64]int64{}
	if err := store.metaIterPrefix("c/", func(_ string, value []byte) error {
		var entry chunkIndexEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return err
		}
		livePerPack[entry.PackID] = append(livePerPack[entry.PackID], entry)
		liveBytes[entry.PackID] += entry.Length

		return nil
	}); err != nil {
		return report, err
	}

	entries, err := os.ReadDir(packsDir)
	if err != nil {
		return report, err
	}

	report.BytesBefore, err = totalPackBytes(packsDir)
	if err != nil {
		return report, err
	}

	for _, dirEntry := range entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if dirEntry.IsDir() || filepath.Ext(dirEntry.Name()) != ".pack" {
			continue
		}
		var id uint64
		if _, scanErr := fmt.Sscanf(dirEntry.Name(), "%d.pack", &id); scanErr != nil {
			continue
		}
		report.PacksScanned++
		if id == activeID {
			// The active pack is still being appended to; leave it alone.
			continue
		}

		info, statErr := dirEntry.Info()
		if statErr != nil {
			return report, statErr
		}
		live := livePerPack[id]
		if len(live) > 0 && liveBytes[id] >= info.Size() {
			// Fully live (packs store raw concatenated chunk bytes, so file size
			// equals the sum of its chunk lengths when nothing is dead).
			continue
		}

		if len(live) > 0 {
			moved, moveErr := store.relocateLiveChunks(ctx, live)
			if moveErr != nil {
				return report, moveErr
			}
			report.ChunksMoved += moved
			report.PacksRepacked++
		}

		store.packs.invalidate(id)
		if err := os.Remove(
			store.packPath(id),
		); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return report, err
		}
		report.PacksRemoved++
	}

	report.BytesAfter, err = totalPackBytes(packsDir)
	if err != nil {
		return report, err
	}

	return report, nil
}

// relocateLiveChunks copies each live chunk's bytes verbatim into the active
// pack and points its index entry at the new location. An entry removed by a
// concurrent Gc is skipped (its bytes become dead space reclaimed next pass)
// rather than resurrected.
func (store *FilesystemStore) relocateLiveChunks(
	ctx context.Context,
	live []chunkIndexEntry,
) (int, error) {
	moved := 0
	for _, entry := range live {
		if err := ctx.Err(); err != nil {
			return moved, err
		}

		raw, err := store.readPackBytes(entry.PackID, entry.Offset, entry.Length)
		if err != nil {
			return moved, err
		}

		store.packMu.Lock()
		newID, newOffset, appendErr := store.appendToActivePack(raw)
		store.packMu.Unlock()
		if appendErr != nil {
			return moved, appendErr
		}

		present, err := store.metaHas(chunkIndexKey(entry.ChunkHash))
		if err != nil {
			return moved, err
		}
		if !present {
			// Gc removed this entry while we were copying; do not resurrect it.
			continue
		}

		updated := entry
		updated.PackID = newID
		updated.Offset = newOffset
		updated.UpdatedAt = time.Now().UTC()
		if err := store.metaPut(chunkIndexKey(entry.ChunkHash), updated); err != nil {
			return moved, err
		}
		moved++
	}

	return moved, nil
}

func totalPackBytes(packsDir string) (int64, error) {
	entries, err := os.ReadDir(packsDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var total int64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".pack" {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return 0, infoErr
		}
		total += info.Size()
	}

	return total, nil
}
