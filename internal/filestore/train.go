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
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	defaultDictSampleLimit = 1024
	dictSampleMaxBytes     = 16 * 1024
	dictMaxBytes           = 112640
	minDictSamples         = 16
)

var ErrTooFewSamples = errors.New("too few samples to train dictionary")

type TrainDictionaryRequest struct {
	Family      string `json:"family,omitempty"`
	AllFamilies bool   `json:"all_families,omitempty"`
	SampleLimit int    `json:"sample_limit,omitempty"`
}

type TrainDictionaryReport struct {
	DictionaryID    string                        `json:"dictionary_id,omitempty"`
	DictionaryBytes int64                         `json:"dictionary_bytes,omitempty"`
	Family          string                        `json:"family,omitempty"`
	Samples         int                           `json:"samples,omitempty"`
	Results         []TrainDictionaryFamilyReport `json:"results,omitempty"`
}

type TrainDictionaryFamilyReport struct {
	DictionaryID    string `json:"dictionary_id"`
	DictionaryBytes int64  `json:"dictionary_bytes"`
	Family          string `json:"family"`
	Samples         int    `json:"samples"`
}

func (store *FilesystemStore) TrainDictionary(
	ctx context.Context,
	request TrainDictionaryRequest,
) (TrainDictionaryReport, error) {
	if err := ctx.Err(); err != nil {
		return TrainDictionaryReport{}, err
	}

	store.maintenanceMu.Lock()
	defer store.maintenanceMu.Unlock()

	if request.SampleLimit <= 0 {
		request.SampleLimit = defaultDictSampleLimit
	}
	if _, err := exec.LookPath("zstd"); err != nil {
		return TrainDictionaryReport{}, fmt.Errorf(
			"zstd binary is required to train a dictionary: %w", err,
		)
	}
	if err := store.ensureDictionaryBootstrap(); err != nil {
		return TrainDictionaryReport{}, err
	}

	family := sanitizeDictionaryFamily(request.Family)
	if request.AllFamilies || family == "" {
		return store.trainAllDictionaryFamilies(ctx, request.SampleLimit)
	}
	if !dictionaryFamilyEligibleForTraining(family) {
		return TrainDictionaryReport{}, fmt.Errorf(
			"dictionary family %q is not trainable",
			request.Family,
		)
	}

	result, err := store.trainDictionaryFamily(ctx, family, request.SampleLimit)
	if err != nil {
		return TrainDictionaryReport{}, err
	}

	return reportFromFamilyResults([]TrainDictionaryFamilyReport{result}), nil
}

func (store *FilesystemStore) trainAllDictionaryFamilies(
	ctx context.Context,
	sampleLimit int,
) (TrainDictionaryReport, error) {
	families, err := store.dictionaryFamiliesWithSamples(ctx, sampleLimit)
	if err != nil {
		return TrainDictionaryReport{}, err
	}

	results := []TrainDictionaryFamilyReport{}
	for _, family := range families {
		result, trainErr := store.trainDictionaryFamily(ctx, family, sampleLimit)
		if trainErr != nil {
			if errors.Is(trainErr, ErrTooFewSamples) {
				continue
			}
			return TrainDictionaryReport{}, trainErr
		}
		results = append(results, result)
	}
	if len(results) == 0 {
		return TrainDictionaryReport{}, fmt.Errorf(
			"%w: need at least %d small-object samples in one trainable dictionary family",
			ErrTooFewSamples,
			minDictSamples,
		)
	}

	return reportFromFamilyResults(results), nil
}

func (store *FilesystemStore) trainDictionaryFamily(
	ctx context.Context,
	family string,
	sampleLimit int,
) (TrainDictionaryFamilyReport, error) {
	sampleDir, err := os.MkdirTemp("", "gmdict-samples-")
	if err != nil {
		return TrainDictionaryFamilyReport{}, err
	}
	defer func() { _ = os.RemoveAll(sampleDir) }()

	samplePaths, err := store.writeDictionarySamples(
		ctx,
		sampleDir,
		sampleLimit,
		family,
	)
	if err != nil {
		return TrainDictionaryFamilyReport{}, err
	}
	if len(samplePaths) < minDictSamples {
		return TrainDictionaryFamilyReport{}, fmt.Errorf(
			"%w: need at least %d small-object samples to train dictionary family %q, found %d",
			ErrTooFewSamples,
			minDictSamples,
			family,
			len(samplePaths),
		)
	}

	nextID, err := store.nextDictionaryID()
	if err != nil {
		return TrainDictionaryFamilyReport{}, err
	}
	dictPath := store.dictionaryPath(nextID)
	if err := os.MkdirAll(filepath.Dir(dictPath), 0o750); err != nil {
		return TrainDictionaryFamilyReport{}, err
	}

	args := append([]string{"--train"}, samplePaths...)
	args = append(args, "-o", dictPath, fmt.Sprintf("--maxdict=%d", dictMaxBytes))
	output, runErr := exec.CommandContext(ctx, "zstd", args...).CombinedOutput()
	if runErr != nil {
		return TrainDictionaryFamilyReport{}, fmt.Errorf(
			"zstd --train failed: %w: %s", runErr, strings.TrimSpace(string(output)),
		)
	}

	info, err := os.Stat(dictPath)
	if err != nil {
		return TrainDictionaryFamilyReport{}, err
	}
	registry, err := store.readDictionaryRegistry()
	if err != nil {
		return TrainDictionaryFamilyReport{}, err
	}
	registry.Dictionaries[nextID] = dictionaryRegistryRecord{
		ID:          nextID,
		Family:      family,
		Version:     "trained-" + time.Now().UTC().Format("20060102T150405Z"),
		Filename:    filepath.Join(dictFilesDir, nextID+".dict"),
		Description: "Operator-trained dictionary for " + family,
		InstalledAt: time.Now().UTC(),
		Samples:     len(samplePaths),
	}
	if err := store.writeDictionaryRegistry(registry); err != nil {
		return TrainDictionaryFamilyReport{}, err
	}
	if err := atomicWriteFile(
		store.dictionaryCurrentPath(family),
		[]byte(nextID),
		0o640,
	); err != nil {
		return TrainDictionaryFamilyReport{}, err
	}
	store.reloadDictionaries()

	return TrainDictionaryFamilyReport{
		DictionaryID:    nextID,
		DictionaryBytes: info.Size(),
		Family:          family,
		Samples:         len(samplePaths),
	}, nil
}

func (store *FilesystemStore) writeDictionarySamples(
	ctx context.Context,
	sampleDir string,
	sampleLimit int,
	family string,
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
		matches, err := store.objectMatchesDictionaryFamily(ctx, digest, family)
		if err != nil {
			return err
		}
		if !matches {
			return nil
		}

		content, ok, readErr := store.readBlobContent(digest)
		if readErr != nil {
			return readErr
		}
		if !ok {
			return nil
		}

		path := filepath.Join(sampleDir, fmt.Sprintf("%06d.sample", len(samplePaths)))
		if err := os.WriteFile(path, content, 0o600); err != nil {
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

func (store *FilesystemStore) objectMatchesDictionaryFamily(
	ctx context.Context,
	digest contracts.ObjectDigest,
	family string,
) (bool, error) {
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}

		return false, nil
	}

	return dictionaryFamilyForObject(
		manifest.MediaType,
		manifest.ContentRoles,
	) == family, nil
}

func (store *FilesystemStore) dictionaryFamiliesWithSamples(
	ctx context.Context,
	sampleLimit int,
) ([]string, error) {
	snapshot, err := store.metaSnapshot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = snapshot.Close() }()

	seen := map[string]bool{}
	trainableFamilies := trainableDictionaryFamilyList()
	err = snapshotIterPrefix(snapshot, "r/", func(key string, value []byte) error {
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
		manifest, err := store.ReadManifest(ctx, digest)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			return nil
		}
		family := dictionaryFamilyForObject(manifest.MediaType, manifest.ContentRoles)
		if dictionaryFamilyEligibleForTraining(family) {
			seen[family] = true
		}
		if len(seen) >= len(trainableFamilies) ||
			len(seen) >= sampleLimit {
			return nil
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	families := make([]string, 0, len(seen))
	for _, family := range trainableFamilies {
		if seen[family] {
			families = append(families, family)
		}
	}

	return families, nil
}

func (store *FilesystemStore) nextDictionaryID() (string, error) {
	registry, err := store.readDictionaryRegistry()
	if err != nil {
		return "", err
	}

	var maxID uint64
	for id := range registry.Dictionaries {
		parsed, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			return "", fmt.Errorf("invalid dictionary id %q: %w", id, err)
		}
		if parsed > maxID {
			maxID = parsed
		}
	}
	if data, err := os.ReadFile(
		filepath.Join(store.root, dictionariesDir, currentDictMarker),
	); err == nil {
		parsed, parseErr := strconv.ParseUint(
			strings.TrimSpace(string(data)),
			10,
			32,
		)
		if parseErr != nil {
			return "", fmt.Errorf("invalid current dictionary id %q: %w", string(data), parseErr)
		}
		if parsed > maxID {
			maxID = parsed
		}
	}

	return strconv.FormatUint(maxID+1, 10), nil
}

func reportFromFamilyResults(
	results []TrainDictionaryFamilyReport,
) TrainDictionaryReport {
	report := TrainDictionaryReport{Results: results}
	if len(results) > 0 {
		report.DictionaryID = results[0].DictionaryID
		report.DictionaryBytes = results[0].DictionaryBytes
		report.Family = results[0].Family
		report.Samples = results[0].Samples
	}

	return report
}
