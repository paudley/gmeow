// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package query

import (
	"context"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

type ProjectionSource interface {
	WalkProjection(ctx context.Context, fn filestore.ProjectionFunc) error
}

type IncrementalProjectionSource interface {
	WalkChangedProjection(
		ctx context.Context,
		since time.Time,
		fn filestore.ProjectionFunc,
	) error
}

type SourceCursorProjectionSource interface {
	WalkSourceCursors(ctx context.Context, fn filestore.SourceCursorProjectionFunc) error
}

type Index interface {
	Project(
		ctx context.Context,
		manifest contracts.Manifest,
		annotations []contracts.Annotation,
	) error
	ProjectObject(ctx context.Context, object filestore.ProjectionObject) error
	ProjectSourceCursor(ctx context.Context, cursor contracts.SourceCursor) error
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
	VectorSearch(
		ctx context.Context,
		request contracts.VectorSearchRequest,
	) (contracts.VectorSearchResponse, error)
	SourceCursors(
		ctx context.Context,
		request contracts.SourceCursorRequest,
	) (contracts.SourceCursorResponse, error)
	Rebuild(ctx context.Context) error
	ProjectChanged(ctx context.Context, since time.Time) error
}
