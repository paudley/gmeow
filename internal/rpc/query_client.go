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

func (client *QueryClient) ObjectBreakdown(
	ctx context.Context,
) (contracts.ObjectBreakdown, error) {
	response, err := client.client.ObjectBreakdown(ctx, &pb.ObjectBreakdownRequest{})
	if err != nil {
		return contracts.ObjectBreakdown{}, err
	}

	return contracts.ObjectBreakdown{
		TotalObjects:        response.GetTotalObjects(),
		TotalSizeBytes:      response.GetTotalSizeBytes(),
		CompoundObjects:     response.GetCompoundObjects(),
		SimpleObjects:       response.GetSimpleObjects(),
		ObjectsWithAnalysis: response.GetObjectsWithAnalysis(),
		ByFacet:             fromPBBreakdownCounts(response.GetByFacet()),
		BySource:            fromPBBreakdownSources(response.GetBySource()),
		ByMediaType:         fromPBBreakdownCounts(response.GetByMediaType()),
		ByIdentityStrategy:  fromPBBreakdownCounts(response.GetByIdentityStrategy()),
		ByAnalyzer:          fromPBBreakdownCounts(response.GetByAnalyzer()),
	}, nil
}

func fromPBBreakdownCounts(rows []*pb.BreakdownCount) []contracts.BreakdownCount {
	out := make([]contracts.BreakdownCount, 0, len(rows))
	for _, row := range rows {
		out = append(out, contracts.BreakdownCount{
			Label: row.GetLabel(),
			Count: row.GetCount(),
		})
	}

	return out
}

func fromPBBreakdownSources(rows []*pb.BreakdownSource) []contracts.BreakdownSource {
	out := make([]contracts.BreakdownSource, 0, len(rows))
	for _, row := range rows {
		out = append(out, contracts.BreakdownSource{
			SourceKind: row.GetSourceKind(),
			SourceName: row.GetSourceName(),
			Objects:    row.GetObjects(),
		})
	}

	return out
}

func (client *QueryClient) ResolveMailIdentity(
	ctx context.Context,
	request contracts.MailIdentityResolveRequest,
) (contracts.MailIdentityResolveResponse, error) {
	response, err := client.client.ResolveMailIdentity(
		ctx,
		&pb.MailIdentityResolveRequest{
			MessageId: request.MessageID,
			Limit:     int32(request.Limit),
		},
	)
	if err != nil {
		return contracts.MailIdentityResolveResponse{}, err
	}
	digests := make([]contracts.ObjectDigest, 0, len(response.GetObjectDigests()))
	for _, digest := range response.GetObjectDigests() {
		digests = append(digests, contracts.ObjectDigest(digest))
	}

	return contracts.MailIdentityResolveResponse{
		MessageID: response.GetMessageId(),
		Digests:   digests,
		Total:     int(response.GetTotal()),
		Limit:     int(response.GetLimit()),
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

func (client *QueryClient) RelatedObjects(
	ctx context.Context,
	request contracts.RelatedObjectsRequest,
) (contracts.RelatedObjectsResponse, error) {
	response, err := client.client.RelatedObjects(ctx, &pb.RelatedObjectsRequest{
		SchemaVersion: int32(request.SchemaVersion),
		Digest:        string(request.Digest),
		Facet:         request.Facet,
		Limit:         int32(request.Limit),
	})
	if err != nil {
		return contracts.RelatedObjectsResponse{}, err
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

	return contracts.RelatedObjectsResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Seed:          contracts.ObjectDigest(response.GetSeedDigest()),
		Results:       results,
	}, nil
}

func (client *QueryClient) ContactSearch(
	ctx context.Context,
	request contracts.ContactSearchRequest,
) (contracts.ContactSearchResponse, error) {
	response, err := client.client.ContactSearch(ctx, &pb.ContactSearchRequest{
		Query:  request.Query,
		Limit:  int32(request.Limit),
		Offset: int32(request.Offset),
	})
	if err != nil {
		return contracts.ContactSearchResponse{}, err
	}

	results := make([]contracts.ContactSearchResult, 0, len(response.GetResults()))
	for _, result := range response.GetResults() {
		firstSeenAt, err := parseTime(result.GetFirstSeenAt())
		if err != nil {
			return contracts.ContactSearchResponse{}, err
		}
		lastSeenAt, err := parseTime(result.GetLastSeenAt())
		if err != nil {
			return contracts.ContactSearchResponse{}, err
		}
		results = append(results, contracts.ContactSearchResult{
			ContactID:        result.GetContactId(),
			DisplayName:      result.GetDisplayName(),
			PrimaryEmail:     result.GetPrimaryEmail(),
			FirstSeenAt:      firstSeenAt,
			LastSeenAt:       lastSeenAt,
			Score:            result.GetScore(),
			FactCount:        int(result.GetFactCount()),
			ImportanceLevel:  int(result.GetImportanceLevel()),
			MessageCount:     int(result.GetMessageCount()),
			ParticipantCount: int(result.GetParticipantCount()),
		})
	}

	return contracts.ContactSearchResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       results,
		Total:         int(response.GetTotal()),
		Limit:         int(response.GetLimit()),
		Offset:        int(response.GetOffset()),
	}, nil
}

func (client *QueryClient) ContactAggregate(
	ctx context.Context,
	request contracts.ContactAggregateRequest,
) (contracts.ContactAggregate, error) {
	response, err := client.client.ContactAggregate(ctx, &pb.ContactAggregateRequest{
		ContactId: request.ContactID,
	})
	if err != nil {
		return contracts.ContactAggregate{}, err
	}
	firstSeenAt, err := parseTime(response.GetFirstSeenAt())
	if err != nil {
		return contracts.ContactAggregate{}, err
	}
	lastSeenAt, err := parseTime(response.GetLastSeenAt())
	if err != nil {
		return contracts.ContactAggregate{}, err
	}

	return contracts.ContactAggregate{
		SchemaVersion:    contracts.SchemaVersion(response.GetSchemaVersion()),
		ContactID:        response.GetContactId(),
		DisplayName:      response.GetDisplayName(),
		PrimaryEmail:     response.GetPrimaryEmail(),
		FirstSeenAt:      firstSeenAt,
		LastSeenAt:       lastSeenAt,
		FactCount:        int(response.GetFactCount()),
		ImportanceLevel:  int(response.GetImportanceLevel()),
		MessageCount:     int(response.GetMessageCount()),
		ParticipantCount: int(response.GetParticipantCount()),
		Facts:            fromPBContactFacts(response.GetFacts()),
	}, nil
}

func (client *QueryClient) ResolveContactIdentity(
	ctx context.Context,
	request contracts.ContactIdentityResolveRequest,
) (contracts.ContactIdentityResolveResponse, error) {
	response, err := client.client.ResolveContactIdentity(
		ctx,
		&pb.ContactIdentityResolveRequest{Identity: request.Identity},
	)
	if err != nil {
		return contracts.ContactIdentityResolveResponse{}, err
	}

	return contracts.ContactIdentityResolveResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		ContactIDs:    append([]string{}, response.GetContactIds()...),
	}, nil
}

func (client *QueryClient) ContactFacts(
	ctx context.Context,
	request contracts.ContactFactRequest,
) (contracts.ContactFactResponse, error) {
	response, err := client.client.ContactFacts(ctx, &pb.ContactFactRequest{
		ContactIds: append([]string{}, request.ContactIDs...),
		FactKinds:  append([]string{}, request.FactKinds...),
		At:         request.At,
		From:       request.From,
		Until:      request.Until,
		Current:    request.Current,
		Limit:      int32(request.Limit),
		Offset:     int32(request.Offset),
	})
	if err != nil {
		return contracts.ContactFactResponse{}, err
	}

	return contracts.ContactFactResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Facts:         fromPBContactFacts(response.GetFacts()),
		Total:         int(response.GetTotal()),
		Limit:         int(response.GetLimit()),
		Offset:        int(response.GetOffset()),
	}, nil
}

func (client *QueryClient) ContactIdentityDetails(
	ctx context.Context,
	request contracts.ContactIdentityDetailRequest,
) (contracts.ContactIdentityDetailResponse, error) {
	response, err := client.client.ContactIdentityDetails(
		ctx,
		&pb.ContactIdentityDetailRequest{
			Identities: append([]string{}, request.Identities...),
			ContactIds: append([]string{}, request.ContactIDs...),
			Limit:      int32(request.Limit),
			Offset:     int32(request.Offset),
		},
	)
	if err != nil {
		return contracts.ContactIdentityDetailResponse{}, err
	}

	results := make([]contracts.ContactIdentityDetail, 0, len(response.GetResults()))
	for _, result := range response.GetResults() {
		results = append(results, contracts.ContactIdentityDetail{
			SourceDigest:  contracts.ObjectDigest(result.GetSourceDigest()),
			MatchedToken:  result.GetMatchedToken(),
			Token:         result.GetToken(),
			TokenHash:     result.GetTokenHash(),
			ContactID:     result.GetContactId(),
			StatementHash: result.GetStatementHash(),
			ValidFrom:     result.GetValidFrom(),
			ValidUntil:    result.GetValidUntil(),
		})
	}

	return contracts.ContactIdentityDetailResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       results,
		Total:         int(response.GetTotal()),
		Limit:         int(response.GetLimit()),
		Offset:        int(response.GetOffset()),
	}, nil
}

func (client *QueryClient) ContactNeighborhood(
	ctx context.Context,
	request contracts.ContactNeighborhoodRequest,
) (contracts.ContactNeighborhoodResponse, error) {
	response, err := client.client.ContactNeighborhood(
		ctx,
		&pb.ContactNeighborhoodRequest{
			ContactId: request.ContactID,
			FactKinds: append(
				[]string{},
				request.FactKinds...,
			),
			Limit:  int32(request.Limit),
			Offset: int32(request.Offset),
		},
	)
	if err != nil {
		return contracts.ContactNeighborhoodResponse{}, err
	}

	results := make(
		[]contracts.ContactNeighborhoodResult,
		0,
		len(response.GetResults()),
	)
	for _, result := range response.GetResults() {
		results = append(results, contracts.ContactNeighborhoodResult{
			SourceDigest:  contracts.ObjectDigest(result.GetSourceDigest()),
			StatementHash: result.GetStatementHash(),
			ContactID:     result.GetContactId(),
			FactKind:      result.GetFactKind(),
			Value:         result.GetValue(),
			Predicate:     result.GetPredicate(),
			ValidFrom:     result.GetValidFrom(),
			ValidUntil:    result.GetValidUntil(),
		})
	}

	return contracts.ContactNeighborhoodResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       results,
		Total:         int(response.GetTotal()),
		Limit:         int(response.GetLimit()),
		Offset:        int(response.GetOffset()),
	}, nil
}

func (client *QueryClient) ContactAnalysisInputs(
	ctx context.Context,
	request contracts.ContactAnalysisInputRequest,
) (contracts.ContactAnalysisInputResponse, error) {
	response, err := client.client.ContactAnalysisInputs(
		ctx,
		&pb.ContactAnalysisInputRequest{
			ContactIds: append([]string{}, request.ContactIDs...),
			FactKinds:  append([]string{}, request.FactKinds...),
			Limit:      int32(request.Limit),
			Offset:     int32(request.Offset),
		},
	)
	if err != nil {
		return contracts.ContactAnalysisInputResponse{}, err
	}

	results := make([]contracts.ContactAnalysisInputResult, 0, len(response.GetResults()))
	for _, result := range response.GetResults() {
		firstSeenAt, err := parseTime(result.GetFirstSeenAt())
		if err != nil {
			return contracts.ContactAnalysisInputResponse{}, err
		}
		lastSeenAt, err := parseTime(result.GetLastSeenAt())
		if err != nil {
			return contracts.ContactAnalysisInputResponse{}, err
		}
		results = append(results, contracts.ContactAnalysisInputResult{
			ContactID:        result.GetContactId(),
			DisplayName:      result.GetDisplayName(),
			PrimaryEmail:     result.GetPrimaryEmail(),
			FirstSeenAt:      firstSeenAt,
			LastSeenAt:       lastSeenAt,
			InputText:        result.GetInputText(),
			FactCount:        int(result.GetFactCount()),
			ImportanceLevel:  int(result.GetImportanceLevel()),
			MessageCount:     int(result.GetMessageCount()),
			ParticipantCount: int(result.GetParticipantCount()),
		})
	}

	return contracts.ContactAnalysisInputResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       results,
		Total:         int(response.GetTotal()),
		Limit:         int(response.GetLimit()),
		Offset:        int(response.GetOffset()),
	}, nil
}

func (client *QueryClient) ContactAnalysisStatus(
	ctx context.Context,
	request contracts.ContactAnalysisStatusRequest,
) (contracts.ContactAnalysisStatusResponse, error) {
	response, err := client.client.ContactAnalysisStatus(
		ctx,
		&pb.ContactAnalysisStatusRequest{
			ContactIds:      append([]string{}, request.ContactIDs...),
			InputHashes:     append([]string{}, request.InputHashes...),
			AnalyzerName:    request.AnalyzerName,
			AnalyzerVersion: request.AnalyzerVersion,
			Model:           request.Model,
			Limit:           int32(request.Limit),
			Offset:          int32(request.Offset),
		},
	)
	if err != nil {
		return contracts.ContactAnalysisStatusResponse{}, err
	}

	results := make([]contracts.ContactAnalysisStatusResult, 0, len(response.GetResults()))
	for _, result := range response.GetResults() {
		metadata, err := decodeMap(result.GetMetadataJson())
		if err != nil {
			return contracts.ContactAnalysisStatusResponse{}, err
		}
		generatedAt, err := parseTime(result.GetGeneratedAt())
		if err != nil {
			return contracts.ContactAnalysisStatusResponse{}, err
		}
		results = append(results, contracts.ContactAnalysisStatusResult{
			GeneratedAt:     generatedAt,
			Metadata:        metadata,
			ContactID:       result.GetContactId(),
			AnalyzerName:    result.GetAnalyzerName(),
			AnalyzerVersion: result.GetAnalyzerVersion(),
			Status:          result.GetStatus(),
			Model:           result.GetModel(),
			InputHash:       result.GetInputHash(),
			InputBytes:      int(result.GetInputBytes()),
		})
	}

	return contracts.ContactAnalysisStatusResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       results,
		Total:         int(response.GetTotal()),
		Limit:         int(response.GetLimit()),
		Offset:        int(response.GetOffset()),
	}, nil
}

func (client *QueryClient) StoreContactEmbedding(
	ctx context.Context,
	record contracts.ContactEmbeddingUpsert,
) error {
	metadata, err := encodeMap(record.Metadata)
	if err != nil {
		return err
	}
	_, err = client.client.StoreContactEmbedding(ctx, &pb.ContactEmbeddingUpsert{
		ContactId:       record.ContactID,
		AnalyzerName:    record.AnalyzerName,
		AnalyzerVersion: record.AnalyzerVersion,
		Status:          record.Status,
		Model:           record.Model,
		InputHash:       record.InputHash,
		InputBytes:      int32(record.InputBytes),
		GeneratedAt:     formatTime(record.GeneratedAt),
		TextPreview:     record.TextPreview,
		Vector:          append([]float32{}, record.Vector...),
		MetadataJson:    metadata,
	})

	return err
}

func (client *QueryClient) ContactVectorSearch(
	ctx context.Context,
	request contracts.ContactVectorSearchRequest,
) (contracts.ContactVectorSearchResponse, error) {
	response, err := client.client.ContactVectorSearch(
		ctx,
		&pb.ContactVectorSearchRequest{
			Vector:     append([]float32{}, request.Vector...),
			Model:      request.Model,
			ContactIds: append([]string{}, request.ContactIDs...),
			Dimensions: int32(request.Dimensions),
			Limit:      int32(request.Limit),
		},
	)
	if err != nil {
		return contracts.ContactVectorSearchResponse{}, err
	}

	return contracts.ContactVectorSearchResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       fromPBContactVectorResults(response.GetResults()),
	}, nil
}

func (client *QueryClient) SimilarContacts(
	ctx context.Context,
	request contracts.SimilarContactsRequest,
) (contracts.SimilarContactsResponse, error) {
	response, err := client.client.SimilarContacts(ctx, &pb.SimilarContactsRequest{
		ContactId: request.ContactID,
		Model:     request.Model,
		Limit:     int32(request.Limit),
	})
	if err != nil {
		return contracts.SimilarContactsResponse{}, err
	}

	return contracts.SimilarContactsResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       fromPBContactVectorResults(response.GetResults()),
	}, nil
}

func (client *QueryClient) ContactMessages(
	ctx context.Context,
	request contracts.ContactMessageRequest,
) (contracts.ContactMessageResponse, error) {
	response, err := client.client.ContactMessages(ctx, &pb.ContactMessageRequest{
		ContactId: request.ContactID,
		Role:      request.Role,
		Limit:     int32(request.Limit),
		Offset:    int32(request.Offset),
	})
	if err != nil {
		return contracts.ContactMessageResponse{}, err
	}

	results := make([]contracts.ContactMessageResult, 0, len(response.GetResults()))
	for _, result := range response.GetResults() {
		messageTime, err := parseTime(result.GetMessageTime())
		if err != nil {
			return contracts.ContactMessageResponse{}, err
		}
		results = append(results, contracts.ContactMessageResult{
			MessageDigest: contracts.ObjectDigest(result.GetMessageDigest()),
			MessageTime:   messageTime,
			MessageID:     result.GetMessageId(),
			MessageDate:   result.GetMessageDate(),
			Role:          result.GetRole(),
			Token:         result.GetToken(),
			DisplayName:   result.GetDisplayName(),
			RawValue:      result.GetRawValue(),
		})
	}

	return contracts.ContactMessageResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Results:       results,
		Total:         int(response.GetTotal()),
		Limit:         int(response.GetLimit()),
		Offset:        int(response.GetOffset()),
	}, nil
}

func fromPBContactFacts(facts []*pb.ContactFact) []contracts.ContactFact {
	converted := make([]contracts.ContactFact, 0, len(facts))
	for _, fact := range facts {
		converted = append(converted, contracts.ContactFact{
			SourceDigest:  contracts.ObjectDigest(fact.GetSourceDigest()),
			StatementHash: fact.GetStatementHash(),
			ContactID:     fact.GetContactId(),
			FactKind:      fact.GetFactKind(),
			Value:         fact.GetValue(),
			Predicate:     fact.GetPredicate(),
			ValidFrom:     fact.GetValidFrom(),
			ValidUntil:    fact.GetValidUntil(),
			Historical:    fact.GetHistorical(),
		})
	}

	return converted
}

func fromPBContactVectorResults(
	results []*pb.ContactVectorSearchResult,
) []contracts.ContactVectorSearchResult {
	converted := make([]contracts.ContactVectorSearchResult, 0, len(results))
	for _, result := range results {
		converted = append(converted, contracts.ContactVectorSearchResult{
			ContactID:    result.GetContactId(),
			DisplayName:  result.GetDisplayName(),
			PrimaryEmail: result.GetPrimaryEmail(),
			Model:        result.GetModel(),
			EmbeddingID:  result.GetEmbeddingId(),
			InputHash:    result.GetInputHash(),
			TextPreview:  result.GetTextPreview(),
			Distance:     result.GetDistance(),
			Dimensions:   int(result.GetDimensions()),
		})
	}

	return converted
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

	return fromPBJMAPMailboxes(response.GetMailboxes())
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

func (client *QueryClient) JMAPThreads(
	ctx context.Context,
	ids []string,
) (map[string]contracts.JMAPThread, error) {
	response, err := client.client.JMAPThreads(ctx, &pb.JMAPThreadRequest{
		Ids: append([]string{}, ids...),
	})
	if err != nil {
		return nil, err
	}

	threads := make(map[string]contracts.JMAPThread)
	for _, thread := range response.GetThreads() {
		emailIDs := make([]contracts.ObjectDigest, 0, len(thread.GetEmailIds()))
		for _, emailID := range thread.GetEmailIds() {
			emailIDs = append(emailIDs, contracts.ObjectDigest(emailID))
		}
		threads[thread.GetId()] = contracts.JMAPThread{
			ID:       thread.GetId(),
			EmailIDs: emailIDs,
		}
	}

	return threads, nil
}

func (client *QueryClient) JMAPBlobLookup(
	ctx context.Context,
	request contracts.JMAPBlobLookupRequest,
) (contracts.JMAPBlobLookupResponse, error) {
	blobIDs := make([]string, 0, len(request.BlobIDs))
	for _, blobID := range request.BlobIDs {
		blobIDs = append(blobIDs, string(blobID))
	}
	response, err := client.client.JMAPBlobLookup(ctx, &pb.JMAPBlobLookupRequest{
		TypeNames: append([]string{}, request.TypeNames...),
		BlobIds:   blobIDs,
	})
	if err != nil {
		return contracts.JMAPBlobLookupResponse{}, err
	}

	blobs := make(map[contracts.ObjectDigest]contracts.JMAPBlobReferences)
	for _, blob := range response.GetBlobs() {
		blobID := contracts.ObjectDigest(blob.GetBlobId())
		emailIDs := make([]contracts.ObjectDigest, 0, len(blob.GetEmailIds()))
		for _, emailID := range blob.GetEmailIds() {
			emailIDs = append(emailIDs, contracts.ObjectDigest(emailID))
		}
		blobs[blobID] = contracts.JMAPBlobReferences{
			EmailIDs:   emailIDs,
			ThreadIDs:  append([]string{}, blob.GetThreadIds()...),
			MailboxIDs: append([]string{}, blob.GetMailboxIds()...),
		}
	}

	return contracts.JMAPBlobLookupResponse{Blobs: blobs}, nil
}

func (client *QueryClient) UpdateJMAPMailboxCatalog(
	ctx context.Context,
	update contracts.JMAPMailboxCatalogUpdate,
) ([]contracts.JMAPMailbox, error) {
	mailboxes := make([]*pb.JMAPMailbox, 0, len(update.Mailboxes))
	for _, mailbox := range update.Mailboxes {
		mailboxes = append(mailboxes, toPBJMAPMailbox(mailbox))
	}
	response, err := client.client.UpdateJMAPMailboxCatalog(
		ctx,
		&pb.UpdateJMAPMailboxCatalogRequest{Mailboxes: mailboxes},
	)
	if err != nil {
		return nil, err
	}

	return fromPBJMAPMailboxes(response.GetMailboxes())
}

func (client *QueryClient) JMAPMailboxEmailCounts(
	ctx context.Context,
	request contracts.JMAPMailboxEmailCountRequest,
) (contracts.JMAPMailboxEmailCountResponse, error) {
	response, err := client.client.JMAPMailboxEmailCounts(
		ctx,
		&pb.JMAPMailboxEmailCountRequest{
			MailboxIds: append([]string{}, request.MailboxIDs...),
		},
	)
	if err != nil {
		return contracts.JMAPMailboxEmailCountResponse{}, err
	}

	counts := make(map[string]int, len(response.GetCounts()))
	for _, count := range response.GetCounts() {
		counts[count.GetMailboxId()] = int(count.GetEmailCount())
	}

	return contracts.JMAPMailboxEmailCountResponse{Counts: counts}, nil
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

func fromPBJMAPMailboxes(
	values []*pb.JMAPMailbox,
) ([]contracts.JMAPMailbox, error) {
	mailboxes := make([]contracts.JMAPMailbox, 0, len(values))
	for _, mailbox := range values {
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

func (client *QueryClient) Rebuild(ctx context.Context) error {
	_, err := client.client.Rebuild(ctx, &pb.Empty{})

	return err
}

func (client *QueryClient) ValidateBearerToken(
	ctx context.Context,
	token string,
) (string, bool, error) {
	response, err := client.client.ValidateBearerToken(ctx, &pb.ValidateBearerTokenRequest{
		Token: token,
	})
	if err != nil {
		return "", false, err
	}

	return response.GetClientId(), response.GetValid(), nil
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
