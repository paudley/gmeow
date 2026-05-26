// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/query"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

type QueryServer struct {
	pb.UnimplementedQueryServiceServer
	index query.Index
}

func NewQueryServer(index query.Index) *QueryServer {
	return &QueryServer{index: index}
}

func (server *QueryServer) Project(
	ctx context.Context,
	request *pb.ProjectRequest,
) (*pb.Empty, error) {
	manifest, err := FromPBManifest(request.GetManifest())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	annotations, err := FromPBAnnotations(request.GetAnnotations())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.Empty{}, server.index.Project(ctx, manifest, annotations)
}

func (server *QueryServer) ProjectObject(
	ctx context.Context,
	request *pb.ProjectObjectRequest,
) (*pb.Empty, error) {
	object, err := FromPBProjectionObject(request.GetObject())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.Empty{}, server.index.ProjectObject(ctx, object)
}

func (server *QueryServer) ProjectSourceCursor(
	ctx context.Context,
	request *pb.ProjectSourceCursorRequest,
) (*pb.Empty, error) {
	cursor, err := FromPBSourceCursor(request.GetCursor())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.Empty{}, server.index.ProjectSourceCursor(ctx, cursor)
}

func (server *QueryServer) Search(
	ctx context.Context,
	request *pb.SearchRequest,
) (*pb.SearchResponse, error) {
	response, err := server.index.Search(ctx, contracts.SearchRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		Query:         request.GetQuery(),
		Facets:        append([]string{}, request.GetFacets()...),
		Provenance: contracts.ProvenanceFilter{
			SourceKinds: append([]string{}, request.GetProvenance().GetSourceKinds()...),
			SourceNames: append([]string{}, request.GetProvenance().GetSourceNames()...),
			ExternalIDs: append([]string{}, request.GetProvenance().GetExternalIds()...),
		},
		Relationships: fromPBRelationshipFilter(request.GetRelationships()),
		CompoundRoles: append([]string{}, request.GetCompoundRoles()...),
		AnalyzerNames: append([]string{}, request.GetAnalyzerNames()...),
		MediaTypes:    append([]string{}, request.GetMediaTypes()...),
		Limit:         int(request.GetLimit()),
		Offset:        int(request.GetOffset()),
	})
	if err != nil {
		return nil, err
	}
	results := make([]*pb.SearchResult, 0, len(response.Results))
	for _, result := range response.Results {
		attributes, err := encodeMap(result.Attributes)
		if err != nil {
			return nil, err
		}
		results = append(results, &pb.SearchResult{
			ObjectDigest:   string(result.ObjectDigest),
			Score:          result.Score,
			Title:          result.Title,
			Snippet:        result.Snippet,
			Facets:         append([]string{}, result.Facets...),
			AttributesJson: attributes,
		})
	}
	return &pb.SearchResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Results:       results,
		Total:         int32(response.Total),
	}, nil
}

func (server *QueryServer) Structure(
	ctx context.Context,
	request *pb.StructureRequest,
) (*pb.StructureResponse, error) {
	structure, err := server.index.Structure(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
	)
	if err != nil {
		return nil, err
	}
	converted, err := ToPBStructure(structure)
	if err != nil {
		return nil, err
	}
	return &pb.StructureResponse{Structure: converted}, nil
}

func (server *QueryServer) Relationships(
	ctx context.Context,
	request *pb.RelationshipRequest,
) (*pb.RelationshipResponse, error) {
	response, err := server.index.Relationships(ctx, contracts.RelationshipRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		Filter:        fromPBRelationshipFilter(request.GetFilter()),
		Limit:         int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}
	return &pb.RelationshipResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Relationships: ToPBRelationships(response.Relationships),
	}, nil
}

func (server *QueryServer) Graph(
	ctx context.Context,
	request *pb.GraphRequest,
) (*pb.GraphResponse, error) {
	response, err := server.index.Graph(ctx, contracts.GraphRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		Node:          request.GetNode(),
		Predicate:     request.GetPredicate(),
		Limit:         int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}
	facts, err := ToPBGraphFacts(response.Facts)
	if err != nil {
		return nil, err
	}
	return &pb.GraphResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Facts:         facts,
	}, nil
}

func (server *QueryServer) AnalysisStatus(
	ctx context.Context,
	request *pb.AnalysisStatusRequest,
) (*pb.AnalysisStatusResponse, error) {
	analyzers := make([]contracts.AnalyzerSpec, 0, len(request.GetAnalyzers()))
	for _, analyzer := range request.GetAnalyzers() {
		analyzers = append(analyzers, FromPBAnalyzerSpec(analyzer))
	}
	digests := make([]contracts.ObjectDigest, 0, len(request.GetObjectDigests()))
	for _, digest := range request.GetObjectDigests() {
		digests = append(digests, contracts.ObjectDigest(digest))
	}
	response, err := server.index.AnalysisStatus(ctx, contracts.AnalysisStatusRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		ObjectDigests: digests,
		AnalyzerNames: append([]string{}, request.GetAnalyzerNames()...),
		Analyzers:     analyzers,
		Limit:         int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}
	statuses := make([]*pb.AnalysisStatus, 0, len(response.Statuses))
	for _, item := range response.Statuses {
		data, err := encodeMap(item.Data)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, &pb.AnalysisStatus{
			ObjectDigest:    string(item.ObjectDigest),
			AnalyzerName:    item.AnalyzerName,
			AnalyzerVersion: item.AnalyzerVer,
			Status:          item.Status,
			GeneratedAt:     formatTime(item.GeneratedAt),
			DataJson:        data,
		})
	}
	return &pb.AnalysisStatusResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Statuses:      statuses,
	}, nil
}

func (server *QueryServer) VectorSearch(
	ctx context.Context,
	request *pb.VectorSearchRequest,
) (*pb.VectorSearchResponse, error) {
	response, err := server.index.VectorSearch(ctx, contracts.VectorSearchRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		Model:         request.GetModel(),
		Dimensions:    int(request.GetDimensions()),
		Vector:        append([]float32{}, request.GetVector()...),
		Facets:        append([]string{}, request.GetFacets()...),
		Limit:         int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}
	results := make([]*pb.VectorSearchResult, 0, len(response.Results))
	for _, result := range response.Results {
		results = append(results, &pb.VectorSearchResult{
			ObjectDigest: string(result.ObjectDigest),
			Model:        result.Model,
			Distance:     result.Distance,
		})
	}
	return &pb.VectorSearchResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Results:       results,
	}, nil
}

func (server *QueryServer) SourceCursors(
	ctx context.Context,
	request *pb.SourceCursorRequest,
) (*pb.SourceCursorResponse, error) {
	response, err := server.index.SourceCursors(ctx, contracts.SourceCursorRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		SourceKinds:   append([]string{}, request.GetSourceKinds()...),
		SourceNames:   append([]string{}, request.GetSourceNames()...),
		Limit:         int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}
	cursors := make([]*pb.SourceCursor, 0, len(response.Cursors))
	for _, cursor := range response.Cursors {
		converted, err := ToPBSourceCursor(cursor)
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, converted)
	}
	return &pb.SourceCursorResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Cursors:       cursors,
	}, nil
}

func (server *QueryServer) Rebuild(
	ctx context.Context,
	_ *pb.Empty,
) (*pb.Empty, error) {
	return &pb.Empty{}, server.index.Rebuild(ctx)
}

func (server *QueryServer) ProjectChanged(
	ctx context.Context,
	request *pb.ProjectChangedRequest,
) (*pb.Empty, error) {
	since, err := parseTime(request.GetSince())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("parse since: %v", err))
	}
	return &pb.Empty{}, server.index.ProjectChanged(ctx, since)
}

func fromPBRelationshipFilter(
	filter *pb.RelationshipFilter,
) contracts.RelationshipFilter {
	if filter == nil {
		return contracts.RelationshipFilter{}
	}
	return contracts.RelationshipFilter{
		Types: append([]string{}, filter.GetTypes()...),
		From:  contracts.ObjectDigest(filter.GetFrom()),
		To:    contracts.ObjectDigest(filter.GetTo()),
		Roles: append([]string{}, filter.GetRoles()...),
		Any:   contracts.ObjectDigest(filter.GetAny()),
	}
}
