// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import "time"

const (
	SourceImportJobKind = "source_import"
)

type SourceImportJob struct {
	CreatedAt      time.Time     `json:"created_at"`
	Deadline       time.Time     `json:"deadline,omitempty"`
	RunID          string        `json:"run_id"`
	SourceName     string        `json:"source_name"`
	Root           string        `json:"root"`
	Path           string        `json:"path"`
	RelativePath   string        `json:"relative_path"`
	Format         string        `json:"format"`
	RequestedBy    string        `json:"requested_by,omitempty"`
	PriorityClass  string        `json:"priority_class,omitempty"`
	TraceID        string        `json:"trace_id,omitempty"`
	Failure        string        `json:"failure,omitempty"`
	IdempotencyKey string        `json:"idempotency_key"`
	JobID          string        `json:"job_id"`
	SchemaVersion  SchemaVersion `json:"schema_version"`
	Offset         int64         `json:"offset,omitempty"`
	Limit          int64         `json:"limit,omitempty"`
	Attempt        int           `json:"attempt,omitempty"`
	Priority       int           `json:"priority"`
	DryRun         bool          `json:"dry_run,omitempty"`
	LowNoise       bool          `json:"low_noise,omitempty"`
}

type SourceImportQueueStatus struct {
	SchemaVersion SchemaVersion `json:"schema_version"`
	Pending       int           `json:"pending"`
	Retry         int           `json:"retry"`
	Failed        int           `json:"failed"`
	DeadLetter    int           `json:"dead_letter"`
}
