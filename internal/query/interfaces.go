// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package query

import (
	"context"

	"blackat.ca/gmeow/internal/contracts"
	"blackat.ca/gmeow/internal/filestore"
)

type ProjectionSource interface {
	WalkProjection(ctx context.Context, fn filestore.ProjectionFunc) error
}

type Index interface {
	Project(
		ctx context.Context,
		manifest contracts.Manifest,
		annotations []contracts.Annotation,
	) error
	ProjectObject(ctx context.Context, object filestore.ProjectionObject) error
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
	Rebuild(ctx context.Context) error
}
