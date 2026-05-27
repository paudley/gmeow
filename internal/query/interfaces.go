// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package query

import (
	"context"
	"encoding/json"
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
	UpdateJMAPEmailState(
		ctx context.Context,
		update contracts.JMAPEmailStateUpdate,
	) (contracts.JMAPEmailState, error)
	CreateOrGet(
		ctx context.Context,
		request contracts.CreateOperationRequest,
	) (contracts.OperationRecord, bool, error)
	AppendProgress(
		ctx context.Context,
		operationID string,
		event contracts.OperationProgressEvent,
	) error
	Complete(ctx context.Context, operationID string, result json.RawMessage) error
	Fail(ctx context.Context, operationID, message string) error
	Get(ctx context.Context, operationID string) (contracts.OperationRecord, bool, error)
	GetByRequestHash(
		ctx context.Context,
		requestHash string,
	) (contracts.OperationRecord, bool, error)
	Rebuild(ctx context.Context) error
	ProjectChanged(ctx context.Context, since time.Time) error
}
