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
	ObjectBreakdown(ctx context.Context) (contracts.ObjectBreakdown, error)
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
	RelatedObjects(
		ctx context.Context,
		request contracts.RelatedObjectsRequest,
	) (contracts.RelatedObjectsResponse, error)
	SourceCursors(
		ctx context.Context,
		request contracts.SourceCursorRequest,
	) (contracts.SourceCursorResponse, error)
	MailArchiveMissingGmail(
		ctx context.Context,
		request contracts.MailIdentityReportRequest,
	) (contracts.MailIdentityReportResponse, error)
	ResolveMailIdentity(
		ctx context.Context,
		request contracts.MailIdentityResolveRequest,
	) (contracts.MailIdentityResolveResponse, error)
	ContactAggregate(
		ctx context.Context,
		request contracts.ContactAggregateRequest,
	) (contracts.ContactAggregate, error)
	ContactSearch(
		ctx context.Context,
		request contracts.ContactSearchRequest,
	) (contracts.ContactSearchResponse, error)
	ResolveContactIdentity(
		ctx context.Context,
		request contracts.ContactIdentityResolveRequest,
	) (contracts.ContactIdentityResolveResponse, error)
	ContactFacts(
		ctx context.Context,
		request contracts.ContactFactRequest,
	) (contracts.ContactFactResponse, error)
	ContactIdentityDetails(
		ctx context.Context,
		request contracts.ContactIdentityDetailRequest,
	) (contracts.ContactIdentityDetailResponse, error)
	ContactNeighborhood(
		ctx context.Context,
		request contracts.ContactNeighborhoodRequest,
	) (contracts.ContactNeighborhoodResponse, error)
	ContactAnalysisInputs(
		ctx context.Context,
		request contracts.ContactAnalysisInputRequest,
	) (contracts.ContactAnalysisInputResponse, error)
	ContactAnalysisStatus(
		ctx context.Context,
		request contracts.ContactAnalysisStatusRequest,
	) (contracts.ContactAnalysisStatusResponse, error)
	StoreContactEmbedding(
		ctx context.Context,
		record contracts.ContactEmbeddingUpsert,
	) error
	ContactVectorSearch(
		ctx context.Context,
		request contracts.ContactVectorSearchRequest,
	) (contracts.ContactVectorSearchResponse, error)
	SimilarContacts(
		ctx context.Context,
		request contracts.SimilarContactsRequest,
	) (contracts.SimilarContactsResponse, error)
	ContactMessages(
		ctx context.Context,
		request contracts.ContactMessageRequest,
	) (contracts.ContactMessageResponse, error)
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
	ValidateBearerToken(ctx context.Context, token string) (string, bool, error)
}
