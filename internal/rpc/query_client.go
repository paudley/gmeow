// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"encoding/json"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

type QueryClient struct {
	connection grpcClientConn
	client     pb.QueryServiceClient
}

type grpcClientConn interface {
	Close() error
}

func NewQueryClient(ctx context.Context, endpoint Endpoint) (*QueryClient, error) {
	connection, err := dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	return &QueryClient{
		connection: connection,
		client:     pb.NewQueryServiceClient(connection),
	}, nil
}

func (client *QueryClient) Close() error {
	if client.connection == nil {
		return nil
	}

	return client.connection.Close()
}

func (client *QueryClient) Project(
	ctx context.Context,
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) error {
	converted, err := ToPBManifest(manifest)
	if err != nil {
		return err
	}
	pbAnnotations, err := ToPBAnnotations(annotations)
	if err != nil {
		return err
	}

	_, err = client.client.Project(ctx, &pb.ProjectRequest{
		Manifest:    converted,
		Annotations: pbAnnotations,
	})

	return err
}

func (client *QueryClient) ProjectObject(
	ctx context.Context,
	object filestore.ProjectionObject,
) error {
	converted, err := ToPBProjectionObject(object)
	if err != nil {
		return err
	}

	_, err = client.client.ProjectObject(ctx, &pb.ProjectObjectRequest{Object: converted})

	return err
}

func (client *QueryClient) ProjectSourceCursor(
	ctx context.Context,
	cursor contracts.SourceCursor,
) error {
	converted, err := ToPBSourceCursor(cursor)
	if err != nil {
		return err
	}

	_, err = client.client.ProjectSourceCursor(ctx, &pb.ProjectSourceCursorRequest{
		Cursor: converted,
	})

	return err
}

func (client *QueryClient) Search(
	ctx context.Context,
	request contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	response, err := client.client.Search(ctx, &pb.SearchRequest{
		SchemaVersion: int32(request.SchemaVersion),
		Query:         request.Query,
		Facets:        append([]string{}, request.Facets...),
		Provenance: &pb.ProvenanceFilter{
			SourceKinds: append([]string{}, request.Provenance.SourceKinds...),
			SourceNames: append([]string{}, request.Provenance.SourceNames...),
			ExternalIds: append([]string{}, request.Provenance.ExternalIDs...),
		},
		Relationships: toPBRelationshipFilter(request.Relationships),
		CompoundRoles: append([]string{}, request.CompoundRoles...),
		AnalyzerNames: append([]string{}, request.AnalyzerNames...),
		MediaTypes:    append([]string{}, request.MediaTypes...),
		Limit:         int32(request.Limit),
		Offset:        int32(request.Offset),
	})
	if err != nil {
		return contracts.SearchResponse{}, err
	}

	results := make([]contracts.SearchResult, 0, len(response.GetResults()))
	for _, result := range response.GetResults() {
		attributes, err := decodeMap(result.GetAttributesJson())
		if err != nil {
			return contracts.SearchResponse{}, err
		}
		results = append(results, contracts.SearchResult{
			Attributes:   attributes,
			ObjectDigest: contracts.ObjectDigest(result.GetObjectDigest()),
			Title:        result.GetTitle(),
			Snippet:      result.GetSnippet(),
			Facets:       append([]string{}, result.GetFacets()...),
			Score:        result.GetScore(),
		})
	}

	return contracts.SearchResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       results,
		Total:         int(response.GetTotal()),
	}, nil
}

func (client *QueryClient) Structure(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Structure, error) {
	response, err := client.client.Structure(
		ctx,
		&pb.StructureRequest{Digest: string(digest)},
	)
	if err != nil {
		return contracts.Structure{}, err
	}

	return FromPBStructure(response.GetStructure())
}

func (client *QueryClient) Relationships(
	ctx context.Context,
	request contracts.RelationshipRequest,
) (contracts.RelationshipResponse, error) {
	response, err := client.client.Relationships(ctx, &pb.RelationshipRequest{
		SchemaVersion: int32(request.SchemaVersion),
		Filter:        toPBRelationshipFilter(request.Filter),
		Limit:         int32(request.Limit),
	})
	if err != nil {
		return contracts.RelationshipResponse{}, err
	}

	return contracts.RelationshipResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Relationships: FromPBRelationships(response.GetRelationships()),
	}, nil
}

func (client *QueryClient) Graph(
	ctx context.Context,
	request contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	response, err := client.client.Graph(ctx, &pb.GraphRequest{
		SchemaVersion: int32(request.SchemaVersion),
		Node:          request.Node,
		Predicate:     request.Predicate,
		Limit:         int32(request.Limit),
	})
	if err != nil {
		return contracts.GraphResponse{}, err
	}

	facts, err := FromPBGraphFacts(response.GetFacts())
	if err != nil {
		return contracts.GraphResponse{}, err
	}

	return contracts.GraphResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Facts:         facts,
	}, nil
}

func (client *QueryClient) AnalysisStatus(
	ctx context.Context,
	request contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	digests := make([]string, 0, len(request.ObjectDigests))
	for _, digest := range request.ObjectDigests {
		digests = append(digests, string(digest))
	}
	analyzers := make([]*pb.AnalyzerSpec, 0, len(request.Analyzers))
	for _, analyzer := range request.Analyzers {
		analyzers = append(analyzers, ToPBAnalyzerSpec(analyzer))
	}

	response, err := client.client.AnalysisStatus(ctx, &pb.AnalysisStatusRequest{
		SchemaVersion: int32(request.SchemaVersion),
		ObjectDigests: digests,
		AnalyzerNames: append([]string{}, request.AnalyzerNames...),
		Analyzers:     analyzers,
		Limit:         int32(request.Limit),
	})
	if err != nil {
		return contracts.AnalysisStatusResponse{}, err
	}

	statuses := make([]contracts.AnalysisStatus, 0, len(response.GetStatuses()))
	for _, status := range response.GetStatuses() {
		data, err := decodeMap(status.GetDataJson())
		if err != nil {
			return contracts.AnalysisStatusResponse{}, err
		}
		generatedAt, err := parseTime(status.GetGeneratedAt())
		if err != nil {
			return contracts.AnalysisStatusResponse{}, err
		}
		statuses = append(statuses, contracts.AnalysisStatus{
			GeneratedAt:  generatedAt,
			Data:         data,
			ObjectDigest: contracts.ObjectDigest(status.GetObjectDigest()),
			AnalyzerName: status.GetAnalyzerName(),
			AnalyzerVer:  status.GetAnalyzerVersion(),
			Status:       status.GetStatus(),
		})
	}

	return contracts.AnalysisStatusResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Statuses:      statuses,
	}, nil
}

func (client *QueryClient) VectorSearch(
	ctx context.Context,
	request contracts.VectorSearchRequest,
) (contracts.VectorSearchResponse, error) {
	response, err := client.client.VectorSearch(ctx, &pb.VectorSearchRequest{
		SchemaVersion: int32(request.SchemaVersion),
		Model:         request.Model,
		Vector:        append([]float32{}, request.Vector...),
		Facets:        append([]string{}, request.Facets...),
		Dimensions:    int32(request.Dimensions),
		Limit:         int32(request.Limit),
	})
	if err != nil {
		return contracts.VectorSearchResponse{}, err
	}

	results := make([]contracts.VectorSearchResult, 0, len(response.GetResults()))
	for _, result := range response.GetResults() {
		results = append(results, contracts.VectorSearchResult{
			ObjectDigest: contracts.ObjectDigest(result.GetObjectDigest()),
			Model:        result.GetModel(),
			EmbeddingID:  result.GetEmbeddingId(),
			Kind:         result.GetKind(),
			SourceDigest: contracts.ObjectDigest(result.GetSourceDigest()),
			TextPreview:  result.GetTextPreview(),
			Distance:     result.GetDistance(),
		})
	}

	return contracts.VectorSearchResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       results,
	}, nil
}

func (client *QueryClient) SourceCursors(
	ctx context.Context,
	request contracts.SourceCursorRequest,
) (contracts.SourceCursorResponse, error) {
	response, err := client.client.SourceCursors(ctx, &pb.SourceCursorRequest{
		SchemaVersion: int32(request.SchemaVersion),
		SourceKinds:   append([]string{}, request.SourceKinds...),
		SourceNames:   append([]string{}, request.SourceNames...),
		Limit:         int32(request.Limit),
	})
	if err != nil {
		return contracts.SourceCursorResponse{}, err
	}

	cursors := make([]contracts.SourceCursor, 0, len(response.GetCursors()))
	for _, cursor := range response.GetCursors() {
		converted, err := FromPBSourceCursor(cursor)
		if err != nil {
			return contracts.SourceCursorResponse{}, err
		}
		cursors = append(cursors, converted)
	}

	return contracts.SourceCursorResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Cursors:       cursors,
	}, nil
}

func (client *QueryClient) JMAPMailboxes(
	ctx context.Context,
) ([]contracts.JMAPMailbox, error) {
	response, err := client.client.JMAPMailboxes(ctx, &pb.JMAPMailboxRequest{})
	if err != nil {
		return nil, err
	}

	mailboxes := make([]contracts.JMAPMailbox, 0, len(response.GetMailboxes()))
	for _, mailbox := range response.GetMailboxes() {
		createdAt, err := parseTime(mailbox.GetCreatedAt())
		if err != nil {
			return nil, err
		}
		updatedAt, err := parseTime(mailbox.GetUpdatedAt())
		if err != nil {
			return nil, err
		}
		mailboxes = append(mailboxes, contracts.JMAPMailbox{
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt,
			MailboxID:   mailbox.GetMailboxId(),
			Name:        mailbox.GetName(),
			Role:        mailbox.GetRole(),
			ParentID:    mailbox.GetParentId(),
			SortOrder:   int(mailbox.GetSortOrder()),
			IsSystem:    mailbox.GetIsSystem(),
			IsDestroyed: mailbox.GetIsDestroyed(),
		})
	}

	return mailboxes, nil
}

func (client *QueryClient) JMAPEmailStates(
	ctx context.Context,
	digests []contracts.ObjectDigest,
) (map[contracts.ObjectDigest]contracts.JMAPEmailState, error) {
	values := make([]string, 0, len(digests))
	for _, digest := range digests {
		values = append(values, string(digest))
	}
	response, err := client.client.JMAPEmailStates(ctx, &pb.JMAPEmailStateRequest{
		ObjectDigests: values,
	})
	if err != nil {
		return nil, err
	}

	states := make(map[contracts.ObjectDigest]contracts.JMAPEmailState)
	for _, item := range response.GetStates() {
		state, err := fromPBJMAPEmailState(item)
		if err != nil {
			return nil, err
		}
		states[state.ObjectDigest] = state
	}

	return states, nil
}

func (client *QueryClient) JMAPEmailQuery(
	ctx context.Context,
	request contracts.JMAPEmailQueryRequest,
) (contracts.JMAPEmailQueryResponse, error) {
	response, err := client.client.JMAPEmailQuery(ctx, &pb.JMAPEmailQueryRequest{
		Text:       request.Text,
		InMailbox:  request.InMailbox,
		HasKeyword: request.HasKeyword,
		NotKeyword: request.NotKeyword,
		Limit:      int32(request.Limit),
		Offset:     int32(request.Offset),
	})
	if err != nil {
		return contracts.JMAPEmailQueryResponse{}, err
	}

	ids := make([]contracts.ObjectDigest, 0, len(response.GetIds()))
	for _, id := range response.GetIds() {
		ids = append(ids, contracts.ObjectDigest(id))
	}

	return contracts.JMAPEmailQueryResponse{
		IDs:    ids,
		Total:  int(response.GetTotal()),
		Offset: int(response.GetOffset()),
		Limit:  int(response.GetLimit()),
	}, nil
}

func (client *QueryClient) UpdateJMAPEmailState(
	ctx context.Context,
	update contracts.JMAPEmailStateUpdate,
) (contracts.JMAPEmailState, error) {
	response, err := client.client.UpdateJMAPEmailState(
		ctx,
		&pb.UpdateJMAPEmailStateRequest{
			ObjectDigest: string(update.ObjectDigest),
			MailboxIds:   append([]string{}, update.MailboxIDs...),
			Keywords:     append([]string{}, update.Keywords...),
		},
	)
	if err != nil {
		return contracts.JMAPEmailState{}, err
	}

	return fromPBJMAPEmailState(response)
}

func (client *QueryClient) Rebuild(ctx context.Context) error {
	_, err := client.client.Rebuild(ctx, &pb.Empty{})

	return err
}

func fromPBJMAPEmailState(state *pb.JMAPEmailState) (contracts.JMAPEmailState, error) {
	if state == nil {
		return contracts.JMAPEmailState{}, nil
	}
	receivedAt, err := parseTime(state.GetReceivedAt())
	if err != nil {
		return contracts.JMAPEmailState{}, err
	}
	digest := contracts.ObjectDigest(state.GetObjectDigest())

	return contracts.JMAPEmailState{
		ReceivedAt:    receivedAt,
		ObjectDigest:  digest,
		ThreadID:      state.GetThreadId(),
		MailboxIDs:    append([]string{}, state.GetMailboxIds()...),
		Keywords:      append([]string{}, state.GetKeywords()...),
		StateSequence: state.GetStateSequence(),
	}, nil
}

func (client *QueryClient) ProjectChanged(ctx context.Context, since time.Time) error {
	_, err := client.client.ProjectChanged(ctx, &pb.ProjectChangedRequest{
		Since: formatTime(since),
	})

	return err
}

func (client *QueryClient) CreateOrGet(
	ctx context.Context,
	request contracts.CreateOperationRequest,
) (contracts.OperationRecord, bool, error) {
	response, err := client.client.CreateOrGetOperation(ctx, &pb.CreateOperationRequest{
		OperationId: request.OperationID,
		RequestHash: request.RequestHash,
		Name:        request.Name,
		RequestJson: append([]byte{}, request.Request...),
	})
	if err != nil {
		return contracts.OperationRecord{}, false, err
	}

	record, err := FromPBOperationRecord(response.GetOperation())
	if err != nil {
		return contracts.OperationRecord{}, false, err
	}

	return record, response.GetCreated(), nil
}

func (client *QueryClient) AppendProgress(
	ctx context.Context,
	operationID string,
	event contracts.OperationProgressEvent,
) error {
	_, err := client.client.AppendOperationProgress(
		ctx,
		&pb.AppendOperationProgressRequest{
			OperationId: operationID,
			Event:       ToPBOperationProgress([]contracts.OperationProgressEvent{event})[0],
		},
	)

	return err
}

func (client *QueryClient) Complete(
	ctx context.Context,
	operationID string,
	result json.RawMessage,
) error {
	_, err := client.client.CompleteOperation(ctx, &pb.CompleteOperationRequest{
		OperationId: operationID,
		ResultJson:  append([]byte{}, result...),
	})

	return err
}

func (client *QueryClient) Fail(
	ctx context.Context,
	operationID string,
	message string,
) error {
	_, err := client.client.FailOperation(ctx, &pb.FailOperationRequest{
		OperationId: operationID,
		Message:     message,
	})

	return err
}

func (client *QueryClient) Get(
	ctx context.Context,
	operationID string,
) (contracts.OperationRecord, bool, error) {
	response, err := client.client.GetOperation(ctx, &pb.OperationLookupRequest{
		OperationId: operationID,
	})
	if err != nil {
		return contracts.OperationRecord{}, false, err
	}
	if !response.GetFound() {
		return contracts.OperationRecord{}, false, nil
	}

	record, err := FromPBOperationRecord(response.GetOperation())
	if err != nil {
		return contracts.OperationRecord{}, false, err
	}

	return record, true, nil
}

func (client *QueryClient) GetByRequestHash(
	ctx context.Context,
	requestHash string,
) (contracts.OperationRecord, bool, error) {
	response, err := client.client.GetOperationByRequestHash(
		ctx,
		&pb.OperationByRequestHashRequest{RequestHash: requestHash},
	)
	if err != nil {
		return contracts.OperationRecord{}, false, err
	}
	if !response.GetFound() {
		return contracts.OperationRecord{}, false, nil
	}

	record, err := FromPBOperationRecord(response.GetOperation())
	if err != nil {
		return contracts.OperationRecord{}, false, err
	}

	return record, true, nil
}

func toPBRelationshipFilter(
	filter contracts.RelationshipFilter,
) *pb.RelationshipFilter {
	return &pb.RelationshipFilter{
		Types: append([]string{}, filter.Types...),
		From:  string(filter.From),
		To:    string(filter.To),
		Roles: append([]string{}, filter.Roles...),
		Any:   string(filter.Any),
	}
}
