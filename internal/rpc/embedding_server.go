// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"blackcat.ca/gmeow/internal/embedding"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

// EmbeddingServer adapts the embedding.Service to the EmbeddingService gRPC
// contract. It is a thin marshaller: vectors cross the wire as float32, and all
// caching/ANN logic lives in the embedding package.
type EmbeddingServer struct {
	pb.UnimplementedEmbeddingServiceServer

	service *embedding.Service
}

func NewEmbeddingServer(service *embedding.Service) *EmbeddingServer {
	return &EmbeddingServer{service: service}
}

func (server *EmbeddingServer) Embed(
	ctx context.Context,
	request *pb.EmbedRequest,
) (*pb.EmbedResponse, error) {
	vectors, misses, err := server.service.Embed(ctx, request.GetTexts())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.EmbedResponse{
		Vectors:        vectorsToProto(vectors),
		EmbedderMisses: int32(misses),
	}, nil
}

func (server *EmbeddingServer) Pool(
	ctx context.Context,
	request *pb.PoolRequest,
) (*pb.PoolResponse, error) {
	centroid, misses, err := server.service.Pool(
		ctx,
		request.GetTexts(),
		request.GetWeights(),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.PoolResponse{
		Centroid:       vectorToProto(centroid),
		EmbedderMisses: int32(misses),
	}, nil
}

func (server *EmbeddingServer) Match(
	_ context.Context,
	request *pb.MatchRequest,
) (*pb.MatchResponse, error) {
	matches, err := server.service.Match(
		vectorFromProto(request.GetCentroid()),
		int(request.GetK()),
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	candidates := make([]*pb.MatchCandidate, 0, len(matches))
	for _, match := range matches {
		candidates = append(candidates, &pb.MatchCandidate{
			Entity:     match.Entity,
			Similarity: match.Similarity,
		})
	}

	return &pb.MatchResponse{Candidates: candidates}, nil
}

func (server *EmbeddingServer) Resolve(
	ctx context.Context,
	request *pb.ResolveRequest,
) (*pb.ResolveResponse, error) {
	claims := make([]embedding.ClaimInput, 0, len(request.GetClaims()))
	for _, claim := range request.GetClaims() {
		claims = append(claims, embedding.ClaimInput{
			Text:   claim.GetText(),
			Hash:   claim.GetHash(),
			IsName: claim.GetIsName(),
		})
	}
	resolution, err := server.service.Resolve(
		ctx,
		claims,
		request.GetThreshold(),
		request.GetNameThreshold(),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.ResolveResponse{
		Entity:         resolution.Entity,
		NewClaimHashes: resolution.NewClaimHashes,
		Similarity:     resolution.Similarity,
		IsNew:          resolution.IsNew,
		IsNoop:         resolution.IsNoop,
	}, nil
}

func (server *EmbeddingServer) Reset(
	_ context.Context,
	_ *pb.Empty,
) (*pb.Empty, error) {
	server.service.ResetEntities()

	return &pb.Empty{}, nil
}

func (server *EmbeddingServer) Upsert(
	_ context.Context,
	request *pb.UpsertRequest,
) (*pb.Empty, error) {
	names := make([]embedding.NamedVec, 0, len(request.GetNames()))
	for _, name := range request.GetNames() {
		names = append(names, embedding.NamedVec{
			Key:    name.GetNameKey(),
			Vector: vectorFromProto(name.GetVector()),
		})
	}
	if err := server.service.Upsert(
		request.GetEntity(),
		vectorFromProto(request.GetCentroid()),
		names,
	); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &pb.Empty{}, nil
}

func (server *EmbeddingServer) NearestName(
	_ context.Context,
	request *pb.NearestNameRequest,
) (*pb.NearestNameResponse, error) {
	sim, found := server.service.NearestName(
		request.GetEntity(),
		vectorFromProto(request.GetQuery()),
	)

	return &pb.NearestNameResponse{Similarity: sim, Found: found}, nil
}

func (server *EmbeddingServer) Snapshot(
	_ context.Context,
	_ *pb.Empty,
) (*pb.SnapshotResponse, error) {
	artifact, err := server.service.Snapshot()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.SnapshotResponse{Artifact: artifact}, nil
}

func (server *EmbeddingServer) Status(
	_ context.Context,
	_ *pb.Empty,
) (*pb.EmbeddingStatus, error) {
	cached, entities, model := server.service.Status()

	return &pb.EmbeddingStatus{
		CachedClaims: int32(cached),
		Entities:     int32(entities),
		Model:        model,
	}, nil
}

func vectorToProto(v embedding.Vector) *pb.EmbeddingVector {
	return &pb.EmbeddingVector{Values: append([]float32(nil), v...)}
}

func vectorsToProto(vectors []embedding.Vector) []*pb.EmbeddingVector {
	out := make([]*pb.EmbeddingVector, len(vectors))
	for i, v := range vectors {
		out[i] = vectorToProto(v)
	}

	return out
}

func vectorFromProto(v *pb.EmbeddingVector) embedding.Vector {
	if v == nil {
		return nil
	}

	return append(embedding.Vector(nil), v.GetValues()...)
}
