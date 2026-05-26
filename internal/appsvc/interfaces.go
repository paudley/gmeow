// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"
	"io"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/source"
)

type QueryReader interface {
	Search(
		ctx context.Context,
		request contracts.SearchRequest,
	) (contracts.SearchResponse, error)
	Structure(ctx context.Context, digest contracts.ObjectDigest) (contracts.Structure, error)
	Relationships(
		ctx context.Context,
		request contracts.RelationshipRequest,
	) (contracts.RelationshipResponse, error)
	Graph(ctx context.Context, request contracts.GraphRequest) (contracts.GraphResponse, error)
	AnalysisStatus(
		ctx context.Context,
		request contracts.AnalysisStatusRequest,
	) (contracts.AnalysisStatusResponse, error)
	SourceCursors(
		ctx context.Context,
		request contracts.SourceCursorRequest,
	) (contracts.SourceCursorResponse, error)
}

type ObjectReader interface {
	ReadManifest(ctx context.Context, digest contracts.ObjectDigest) (contracts.Manifest, error)
	GetStructure(ctx context.Context, digest contracts.ObjectDigest) (contracts.Structure, error)
	Open(ctx context.Context, digest contracts.ObjectDigest) (io.ReadCloser, error)
}

type SchedulerClient interface {
	Force(
		ctx context.Context,
		digest contracts.ObjectDigest,
		analyzerNames []string,
		requestedBy string,
		traceID string,
	) (contracts.SchedulerScanResponse, error)
	Status(ctx context.Context) (contracts.SchedulerStatus, error)
}

type SourceRegistry interface {
	LiveSearchBackends(facet string) []source.LiveSearchAdapter
	ActionBackend(kind, name string) (source.ActionAdapter, bool)
}

type SourceIngestService interface {
	Ingest(ctx context.Context, object source.IngestObject) (contracts.ObjectDigest, bool, error)
	LookupSourceObject(ctx context.Context, ref contracts.SourceObjectRef) (contracts.ObjectDigest, bool, error)
}
