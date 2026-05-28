// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	defaultArchiveImportQueueHighWater = 10000
	archiveImportStateFile             = "state.json"
)

type ArchiveImportJobReceipt interface {
	Job() contracts.SourceImportJob
	Ack(context.Context) error
	Retry(context.Context, error) error
}

type ArchiveImportJobSource interface {
	Receive(context.Context) (ArchiveImportJobReceipt, error)
}

type ArchiveImportQueuedRun struct {
	Importer *ArchiveImporter
	Source   ArchiveImportJobSource
}

type archiveImportRunState struct {
	UpdatedAt     time.Time `json:"updated_at"`
	RunID         string    `json:"run_id"`
	SourceName    string    `json:"source_name"`
	LastPath      string    `json:"last_path,omitempty"`
	LastOffset    int64     `json:"last_offset,omitempty"`
	Scanned       int       `json:"scanned"`
	Parsed        int       `json:"parsed"`
	Enqueued      int       `json:"enqueued"`
	Processed     int       `json:"processed"`
	Failures      int       `json:"failures"`
	DiscoveryDone bool      `json:"discovery_done"`
	LowNoise      bool      `json:"low_noise,omitempty"`
	DryRun        bool      `json:"dry_run,omitempty"`
}

func (run ArchiveImportQueuedRun) Run(
	ctx context.Context,
	request ArchiveImportRequest,
) (ArchiveImportReport, error) {
	if run.Importer == nil {
		return ArchiveImportReport{}, errors.New(
			"archive import queued run requires importer",
		)
	}
	if request.Publisher == nil {
		return ArchiveImportReport{}, errors.New(
			"archive import queued run requires publisher",
		)
	}
	if run.Source == nil {
		return ArchiveImportReport{}, errors.New("archive import queued run requires source")
	}

	sourceName := archiveImportSourceName(request)
	runID := archiveImportRunID(request, sourceName)
	var state archiveImportRunState
	if request.Resume {
		var err error
		state, err = loadArchiveImportState(request.StateDir, runID)
		if err != nil {
			return ArchiveImportReport{}, err
		}
	}
	if state.RunID == "" {
		state = archiveImportRunState{
			RunID:      runID,
			SourceName: sourceName,
			LowNoise:   request.LowNoise,
			DryRun:     request.DryRun,
		}
	}

	report := ArchiveImportReport{
		SourceName: sourceName,
		RunID:      runID,
		Scanned:    state.Scanned,
		Parsed:     state.Parsed,
		Enqueued:   state.Enqueued,
		Processed:  state.Processed,
	}
	request.RunID = runID
	if !state.DiscoveryDone {
		if err := run.Importer.enqueueArchiveImportJobs(
			ctx,
			request,
			sourceName,
			runID,
			&state,
			&report,
		); err != nil {
			report.Failures = append(report.Failures, err.Error())
		} else {
			state.DiscoveryDone = true
			if err := saveArchiveImportState(request.StateDir, state); err != nil {
				return report, err
			}
		}
	}

	if len(report.Failures) == 0 {
		if err := run.drain(ctx, request, sourceName, &state, &report); err != nil {
			report.Failures = append(report.Failures, err.Error())
		}
	}
	if len(report.Failures) > 0 {
		return report, fmt.Errorf(
			"archive import completed with %d failure(s)",
			len(report.Failures),
		)
	}

	return report, nil
}

func (run ArchiveImportQueuedRun) drain(
	ctx context.Context,
	request ArchiveImportRequest,
	sourceName string,
	state *archiveImportRunState,
	report *ArchiveImportReport,
) error {
	for report.Processed < report.Enqueued {
		status, err := request.Publisher.SourceImportStatus(ctx)
		if err != nil {
			return err
		}
		if status.Failed > 0 {
			if _, err := request.Publisher.ProcessSourceImportFailures(ctx, 100); err != nil {
				return err
			}
			continue
		}
		if status.Pending == 0 {
			if status.Retry == 0 && status.DeadLetter > 0 {
				return fmt.Errorf("source import dead-lettered %d job(s)", status.DeadLetter)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		receipt, err := run.Source.Receive(ctx)
		if err != nil {
			return err
		}
		job := receipt.Job()
		if job.RunID != report.RunID {
			if err := receipt.Retry(
				ctx,
				fmt.Errorf("source import job belongs to run %s", job.RunID),
			); err != nil {
				return err
			}
			continue
		}
		if err := run.Importer.ProcessSourceImportJob(
			ctx,
			sourceName,
			job,
			request,
			report,
		); err != nil {
			state.Failures++
			if retryErr := receipt.Retry(ctx, err); retryErr != nil {
				return retryErr
			}
			if saveErr := saveArchiveImportState(request.StateDir, *state); saveErr != nil {
				return saveErr
			}
			continue
		}
		if err := receipt.Ack(ctx); err != nil {
			return err
		}
		report.Processed++
		state.Processed = report.Processed
		if err := saveArchiveImportState(request.StateDir, *state); err != nil {
			return err
		}
	}

	return nil
}

func (importer *ArchiveImporter) enqueueArchiveImportJobs(
	ctx context.Context,
	request ArchiveImportRequest,
	sourceName string,
	runID string,
	state *archiveImportRunState,
	report *ArchiveImportReport,
) error {
	highWater := request.QueueHighWater
	if highWater <= 0 {
		highWater = defaultArchiveImportQueueHighWater
	}
	for _, root := range request.Roots {
		if err := importer.walkArchiveJobs(
			ctx,
			request,
			sourceName,
			runID,
			root,
			highWater,
			state,
			report,
		); err != nil {
			report.Failures = append(report.Failures, err.Error())
		}
	}
	if len(report.Failures) > 0 {
		return fmt.Errorf(
			"archive import discovery completed with %d failure(s)",
			len(report.Failures),
		)
	}

	return nil
}

func (importer *ArchiveImporter) walkArchiveJobs(
	ctx context.Context,
	request ArchiveImportRequest,
	sourceName string,
	runID string,
	root string,
	highWater int,
	state *archiveImportRunState,
	report *ArchiveImportReport,
) error {
	root = filepath.Clean(root)
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return importer.enqueueArchiveFileJobs(
			ctx,
			request,
			sourceName,
			runID,
			root,
			root,
			highWater,
			state,
			report,
		)
	}

	return filepath.WalkDir(
		root,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				report.Failures = append(report.Failures, walkErr.Error())
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() {
				if shouldSkipArchiveDir(entry.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if shouldSkipArchiveFile(entry.Name()) {
				report.Skipped++
				return nil
			}
			return importer.enqueueArchiveFileJobs(
				ctx,
				request,
				sourceName,
				runID,
				root,
				path,
				highWater,
				state,
				report,
			)
		},
	)
}

func (importer *ArchiveImporter) enqueueArchiveFileJobs(
	ctx context.Context,
	request ArchiveImportRequest,
	sourceName string,
	runID string,
	root string,
	path string,
	highWater int,
	state *archiveImportRunState,
	report *ArchiveImportReport,
) error {
	format := detectArchiveFileFormat(path, root, request.Format)
	if format == "" {
		report.Skipped++
		return nil
	}
	if shouldSkipArchivePath(path, format, state) {
		return nil
	}
	report.Scanned++
	state.Scanned = report.Scanned
	if format == ArchiveImportFormatMbox {
		return forEachMboxMessageOffset(path, func(offset, size int64) error {
			if path == state.LastPath && offset <= state.LastOffset {
				return nil
			}
			report.Parsed++
			state.Parsed = report.Parsed
			return importer.publishArchiveImportJob(
				ctx,
				request,
				sourceName,
				runID,
				root,
				path,
				format,
				offset,
				size,
				highWater,
				state,
				report,
			)
		})
	}

	report.Parsed++
	state.Parsed = report.Parsed
	return importer.publishArchiveImportJob(
		ctx,
		request,
		sourceName,
		runID,
		root,
		path,
		format,
		0,
		0,
		highWater,
		state,
		report,
	)
}

func (importer *ArchiveImporter) publishArchiveImportJob(
	ctx context.Context,
	request ArchiveImportRequest,
	sourceName string,
	runID string,
	root string,
	path string,
	format string,
	offset int64,
	limit int64,
	highWater int,
	state *archiveImportRunState,
	report *ArchiveImportReport,
) error {
	if err := waitForArchiveImportCapacity(ctx, request.Publisher, highWater); err != nil {
		return err
	}
	relative, _ := filepath.Rel(root, path)
	job := contracts.SourceImportJob{
		SchemaVersion: contracts.SchemaVersionPhase00,
		CreatedAt:     time.Now().UTC(),
		RunID:         runID,
		SourceName:    sourceName,
		Root:          root,
		Path:          path,
		RelativePath:  filepath.ToSlash(relative),
		Format:        format,
		Offset:        offset,
		Limit:         limit,
		LowNoise:      request.LowNoise,
		DryRun:        request.DryRun,
		RequestedBy:   "gmeow-admin source import",
		PriorityClass: contracts.PriorityBackground,
		Priority:      0,
	}
	job.IdempotencyKey = sourceImportJobKey(job)
	job.JobID = job.IdempotencyKey
	if err := request.Publisher.PublishSourceImportJob(ctx, job); err != nil {
		return err
	}
	report.Enqueued++
	state.Enqueued = report.Enqueued
	state.LastPath = path
	state.LastOffset = offset

	return saveArchiveImportState(request.StateDir, *state)
}

func shouldSkipArchivePath(path, format string, state *archiveImportRunState) bool {
	if state == nil || state.LastPath == "" {
		return false
	}
	if path < state.LastPath {
		return true
	}
	if path == state.LastPath && format != ArchiveImportFormatMbox {
		return true
	}

	return false
}

func waitForArchiveImportCapacity(
	ctx context.Context,
	publisher ArchiveImportPublisher,
	highWater int,
) error {
	for {
		status, err := publisher.SourceImportStatus(ctx)
		if err != nil {
			return err
		}
		if status.Pending+status.Retry < highWater {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (importer *ArchiveImporter) ProcessSourceImportJob(
	ctx context.Context,
	sourceName string,
	job contracts.SourceImportJob,
	request ArchiveImportRequest,
	report *ArchiveImportReport,
) error {
	message, err := parseArchiveJobMessage(job)
	if err != nil {
		report.ParseFailures++
		return err
	}
	return importer.ingestArchiveMessage(ctx, sourceName, message, request, report)
}

func parseArchiveJobMessage(job contracts.SourceImportJob) (archiveMessage, error) {
	if job.Format == ArchiveImportFormatMbox {
		return parseMboxMessageAt(job.Path, job.Root, int(job.Offset))
	}

	return parseArchiveFile(job.Path, job.Root, job.Format, 0)
}

func archiveImportSourceName(request ArchiveImportRequest) string {
	sourceName := strings.TrimSpace(request.SourceName)
	if sourceName != "" {
		return sourceName
	}
	if len(request.Roots) == 0 {
		return ""
	}
	return sourceNameFromRoot(request.Roots[0])
}

func archiveImportRunID(request ArchiveImportRequest, sourceName string) string {
	if strings.TrimSpace(request.RunID) != "" {
		return strings.TrimSpace(request.RunID)
	}
	roots := append([]string{}, request.Roots...)
	for index, root := range roots {
		roots[index] = filepath.Clean(root)
	}
	input := strings.Join(append([]string{
		sourceName,
		request.Format,
		fmt.Sprintf("low_noise=%t", request.LowNoise),
		fmt.Sprintf("dry_run=%t", request.DryRun),
	}, roots...), "\x00")
	sum := sha256.Sum256([]byte(input))

	return "archive-" + hex.EncodeToString(sum[:])[:16]
}

func sourceImportJobKey(job contracts.SourceImportJob) string {
	input := strings.Join([]string{
		job.RunID,
		job.SourceName,
		filepath.Clean(job.Root),
		filepath.Clean(job.Path),
		job.Format,
		fmt.Sprintf("%d", job.Offset),
	}, "\x00")
	sum := sha256.Sum256([]byte(input))

	return hex.EncodeToString(sum[:])
}

func loadArchiveImportState(stateDir, runID string) (archiveImportRunState, error) {
	if strings.TrimSpace(stateDir) == "" {
		return archiveImportRunState{}, nil
	}
	path := filepath.Join(stateDir, runID, archiveImportStateFile)
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return archiveImportRunState{}, nil
	}
	if err != nil {
		return archiveImportRunState{}, err
	}

	var state archiveImportRunState
	if err := json.Unmarshal(content, &state); err != nil {
		return archiveImportRunState{}, fmt.Errorf("decode archive import state: %w", err)
	}

	return state, nil
}

func saveArchiveImportState(stateDir string, state archiveImportRunState) error {
	if strings.TrimSpace(stateDir) == "" {
		return nil
	}
	state.UpdatedAt = time.Now().UTC()
	dir := filepath.Join(stateDir, state.RunID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, archiveImportStateFile)
	tmp, err := os.CreateTemp(dir, ".state.")
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmp.Name())
		}
	}()
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	cleanup = false

	return nil
}
