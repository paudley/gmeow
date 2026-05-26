// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"

	"blackcat.ca/gmeow/internal/contracts"
)

type Scheduler interface {
	Scan(ctx context.Context) error
	Enqueue(ctx context.Context, job contracts.AnalyzerJob) error
	Status(ctx context.Context) (Status, error)
}

type Status struct {
	Pending    int `json:"pending"`
	InFlight   int `json:"in_flight"`
	DeadLetter int `json:"dead_letter"`
}
