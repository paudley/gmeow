// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Import runs are recorded as one run.json per run under the import-runs state
// directory (alongside the queued-import state.json), so an operator can list
// past imports — when they ran, from which roots, how many messages, and their
// status — and target one for deletion. The directory is local to the host that
// ran the import; both the direct and queued paths write the same record.
const archiveImportRunFile = "run.json"

// Import run lifecycle statuses.
const (
	ImportRunStatusRunning   = "running"
	ImportRunStatusCompleted = "completed"
	ImportRunStatusFailed    = "failed"
	ImportRunStatusDeleted   = "deleted"
)

// ImportRunRecord is the persisted summary of a single archive import run.
type ImportRunRecord struct {
	RunID      string    `json:"run_id"`
	SourceName string    `json:"source_name"`
	SourceKind string    `json:"source_kind"`
	Roots      []string  `json:"roots"`
	Format     string    `json:"format,omitempty"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
	Scanned    int       `json:"scanned"`
	Parsed     int       `json:"parsed"`
	Imported   int       `json:"imported"`
	Duplicates int       `json:"duplicates"`
	Failures   int       `json:"failures"`
	LowNoise   bool      `json:"low_noise,omitempty"`
	DryRun     bool      `json:"dry_run,omitempty"`
}

// WriteImportRunRecord atomically persists a run record under stateDir. A blank
// stateDir disables the registry (a no-op) so callers without a configured data
// directory still run.
func WriteImportRunRecord(stateDir string, record ImportRunRecord) error {
	if strings.TrimSpace(stateDir) == "" || strings.TrimSpace(record.RunID) == "" {
		return nil
	}

	record.UpdatedAt = time.Now().UTC()
	dir := filepath.Join(stateDir, record.RunID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}

	encoded, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}

	temp, err := os.CreateTemp(dir, ".run.")
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(temp.Name())
		}
	}()
	if _, err := temp.Write(encoded); err != nil {
		_ = temp.Close()

		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(
		temp.Name(),
		filepath.Join(dir, archiveImportRunFile),
	); err != nil {
		return err
	}
	cleanup = false

	return nil
}

// LoadImportRunRecord reads a single run record; found is false when the run id
// is unknown.
func LoadImportRunRecord(stateDir, runID string) (ImportRunRecord, bool, error) {
	if strings.TrimSpace(stateDir) == "" || strings.TrimSpace(runID) == "" {
		return ImportRunRecord{}, false, nil
	}

	content, err := os.ReadFile(filepath.Join(stateDir, runID, archiveImportRunFile))
	if errors.Is(err, os.ErrNotExist) {
		return ImportRunRecord{}, false, nil
	}
	if err != nil {
		return ImportRunRecord{}, false, err
	}

	var record ImportRunRecord
	if err := json.Unmarshal(content, &record); err != nil {
		return ImportRunRecord{}, false, fmt.Errorf(
			"decode import run record %s: %w",
			runID,
			err,
		)
	}

	return record, true, nil
}

// ListImportRunRecords returns all recorded import runs under stateDir, most
// recently started first.
func ListImportRunRecords(stateDir string) ([]ImportRunRecord, error) {
	if strings.TrimSpace(stateDir) == "" {
		return nil, nil
	}

	entries, err := os.ReadDir(stateDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	records := make([]ImportRunRecord, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		record, found, loadErr := LoadImportRunRecord(stateDir, entry.Name())
		if loadErr != nil {
			return nil, loadErr
		}
		if found {
			records = append(records, record)
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].StartedAt.After(records[j].StartedAt)
	})

	return records, nil
}
