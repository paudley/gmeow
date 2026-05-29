// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	defaultDictSampleLimit = 1024
	// dictSampleMaxBytes bounds which objects are sampled for training — the
	// dictionary targets many small similar records (email parts), not media.
	dictSampleMaxBytes = 16 * 1024
	// dictMaxBytes caps the trained dictionary size (~110 KB, the zstd norm).
	dictMaxBytes = 112640
	// minDictSamples is the floor below which training is not worthwhile.
	minDictSamples = 16
)

// TrainDictionaryReport summarizes a dictionary training run.
type TrainDictionaryReport struct {
	DictionaryID    string `json:"dictionary_id"`
	DictionaryBytes int64  `json:"dictionary_bytes"`
	Samples         int    `json:"samples"`
}

// TrainDictionary samples small-object content from a metadata snapshot, trains
// a zstd dictionary over it (shelling out to the `zstd --train` cover trainer),
// installs it as the new current dictionary, and refreshes the in-process cache
// so new chunks compress against it immediately. Existing chunks adopt it on the
// next Repack. It fails closed if the zstd binary is unavailable or too few
// samples exist.
func (store *FilesystemStore) TrainDictionary(
	ctx context.Context,
	sampleLimit int,
) (TrainDictionaryReport, error) {
	if err := ctx.Err(); err != nil {
		return TrainDictionaryReport{}, err
	}
	if sampleLimit <= 0 {
		sampleLimit = defaultDictSampleLimit
	}

	if _, err := exec.LookPath("zstd"); err != nil {
		return TrainDictionaryReport{}, fmt.Errorf(
			"zstd binary is required to train a dictionary: %w", err,
		)
	}

	sampleDir, err := os.MkdirTemp("", "gmdict-samples-")
	if err != nil {
		return TrainDictionaryReport{}, err
	}
	defer func() { _ = os.RemoveAll(sampleDir) }()

	samplePaths, err := store.writeDictionarySamples(ctx, sampleDir, sampleLimit)
	if err != nil {
		return TrainDictionaryReport{}, err
	}
	if len(samplePaths) < minDictSamples {
		return TrainDictionaryReport{}, fmt.Errorf(
			"need at least %d small-object samples to train a dictionary, found %d",
			minDictSamples, len(samplePaths),
		)
	}

	nextID, err := store.nextDictionaryID()
	if err != nil {
		return TrainDictionaryReport{}, err
	}
	dictPath := store.dictionaryPath(nextID)
	if err := os.MkdirAll(filepath.Dir(dictPath), 0o750); err != nil {
		return TrainDictionaryReport{}, err
	}

	args := append([]string{"--train"}, samplePaths...)
	args = append(args, "-o", dictPath, fmt.Sprintf("--maxdict=%d", dictMaxBytes))
	output, runErr := exec.CommandContext(ctx, "zstd", args...).CombinedOutput()
	if runErr != nil {
		return TrainDictionaryReport{}, fmt.Errorf(
			"zstd --train failed: %w: %s", runErr, strings.TrimSpace(string(output)),
		)
	}

	info, err := os.Stat(dictPath)
	if err != nil {
		return TrainDictionaryReport{}, err
	}

	// Flip the current marker atomically, then refresh the in-process cache so
	// new chunks adopt the dictionary without a restart.
	marker := filepath.Join(store.root, dictionariesDir, currentDictMarker)
	if err := atomicWriteFile(marker, []byte(nextID), 0o640); err != nil {
		return TrainDictionaryReport{}, err
	}
	store.reloadDictionaries()

	return TrainDictionaryReport{
		DictionaryID:    nextID,
		DictionaryBytes: info.Size(),
		Samples:         len(samplePaths),
	}, nil
}

func (store *FilesystemStore) writeDictionarySamples(
	ctx context.Context,
	sampleDir string,
	sampleLimit int,
) ([]string, error) {
	snapshot, err := store.metaSnapshot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = snapshot.Close() }()

	var samplePaths []string
	err = snapshotIterPrefix(snapshot, "r/", func(key string, value []byte) error {
		if len(samplePaths) >= sampleLimit {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		var recipe objectRecipeEntry
		if err := json.Unmarshal(value, &recipe); err != nil {
			return err
		}
		if recipe.ContentBytes <= 0 || recipe.ContentBytes > dictSampleMaxBytes {
			return nil
		}

		digest := contracts.ObjectDigest(strings.TrimPrefix(key, "r/"))
		content, ok, readErr := store.readBlobContent(digest)
		if readErr != nil {
			return readErr
		}
		if !ok {
			return nil
		}

		path := filepath.Join(sampleDir, fmt.Sprintf("%06d.sample", len(samplePaths)))
		if err := os.WriteFile(path, content, 0o640); err != nil {
			return err
		}
		samplePaths = append(samplePaths, path)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return samplePaths, nil
}

func (store *FilesystemStore) nextDictionaryID() (string, error) {
	marker := filepath.Join(store.root, dictionariesDir, currentDictMarker)
	data, err := os.ReadFile(marker)
	if errors.Is(err, os.ErrNotExist) {
		return "1", nil
	}
	if err != nil {
		return "", err
	}

	current, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 32)
	if err != nil {
		return "", fmt.Errorf("invalid current dictionary id %q: %w", string(data), err)
	}

	return strconv.FormatUint(current+1, 10), nil
}
