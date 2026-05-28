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
	Structure(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (contracts.Structure, error)
	Relationships(
		ctx context.Context,
		request contracts.RelationshipRequest,
	) (contracts.RelationshipResponse, error)
	Graph(
		ctx context.Context,
		request contracts.GraphRequest,
	) (contracts.GraphResponse, error)
	AnalysisStatus(
		ctx context.Context,
		request contracts.AnalysisStatusRequest,
	) (contracts.AnalysisStatusResponse, error)
	SourceCursors(
		ctx context.Context,
		request contracts.SourceCursorRequest,
	) (contracts.SourceCursorResponse, error)
}

type JMAPQueryReader interface {
	JMAPMailboxes(ctx context.Context) ([]contracts.JMAPMailbox, error)
	JMAPEmailStates(
		ctx context.Context,
		digests []contracts.ObjectDigest,
	) (map[contracts.ObjectDigest]contracts.JMAPEmailState, error)
	JMAPEmailQuery(
		ctx context.Context,
		request contracts.JMAPEmailQueryRequest,
	) (contracts.JMAPEmailQueryResponse, error)
	JMAPThreads(ctx context.Context, ids []string) (map[string]contracts.JMAPThread, error)
	JMAPBlobLookup(
		ctx context.Context,
		request contracts.JMAPBlobLookupRequest,
	) (contracts.JMAPBlobLookupResponse, error)
	UpdateJMAPMailboxCatalog(
		ctx context.Context,
		update contracts.JMAPMailboxCatalogUpdate,
	) ([]contracts.JMAPMailbox, error)
	JMAPMailboxEmailCounts(
		ctx context.Context,
		request contracts.JMAPMailboxEmailCountRequest,
	) (contracts.JMAPMailboxEmailCountResponse, error)
	UpdateJMAPEmailState(
		ctx context.Context,
		update contracts.JMAPEmailStateUpdate,
	) (contracts.JMAPEmailState, error)
}

type ObjectReader interface {
	ReadManifest(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (contracts.Manifest, error)
	GetStructure(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (contracts.Structure, error)
	Open(ctx context.Context, digest contracts.ObjectDigest) (io.ReadCloser, error)
}

type ObjectWriter interface {
	WriteOverlays(
		ctx context.Context,
		digest contracts.ObjectDigest,
		overlays map[string]any,
	) error
	WriteSourceCursor(ctx context.Context, cursor contracts.SourceCursor) error
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
	Ingest(
		ctx context.Context,
		object source.IngestObject,
	) (contracts.ObjectDigest, bool, error)
	LookupSourceObject(
		ctx context.Context,
		ref contracts.SourceObjectRef,
	) (contracts.ObjectDigest, bool, error)
}
