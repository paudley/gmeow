// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"

	"blackat.ca/gmeow/internal/contracts"
)

type Analyzer interface {
	Spec() contracts.AnalyzerSpec
	Analyze(ctx context.Context, job contracts.AnalyzerJob) (contracts.Annotation, error)
}

type Worker interface {
	Run(ctx context.Context) error
}
