// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"io"

	"blackcat.ca/gmeow/internal/contracts"
)

type ObjectStore interface {
	Open(context.Context, contracts.ObjectDigest) (io.ReadCloser, error)
	ReadManifest(context.Context, contracts.ObjectDigest) (contracts.Manifest, error)
	WriteAnnotation(context.Context, contracts.Annotation) error
}

type Analyzer interface {
	Spec() contracts.AnalyzerSpec
	Analyze(
		ctx context.Context,
		store ObjectStore,
		job contracts.AnalyzerJob,
	) (contracts.Annotation, error)
}

type JobReceipt interface {
	Job() contracts.AnalyzerJob
	Ack(ctx context.Context) error
	Retry(ctx context.Context, cause error) error
}

type JobSource interface {
	Receive(ctx context.Context) (JobReceipt, error)
}

type Worker interface {
	Run(ctx context.Context) error
}
