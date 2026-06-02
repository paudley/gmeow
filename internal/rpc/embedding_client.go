// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"

	"blackcat.ca/gmeow/internal/embedding"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

// EmbeddingClient is the IMPORT/FILESTORE side of the EMBEDDING service. It
// speaks the embedding package's own types so callers never touch the proto
// layer. Method shapes mirror embedding.Service so ingest can swap an in-process
// Service for this client without code changes.
type EmbeddingClient struct {
	connection grpcClientConn
	client     pb.EmbeddingServiceClient
}

func NewEmbeddingClient(ctx context.Context, endpoint Endpoint) (*EmbeddingClient, error) {
	connection, err := dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	return &EmbeddingClient{
		connection: connection,
		client:     pb.NewEmbeddingServiceClient(connection),
	}, nil
}

func (client *EmbeddingClient) Close() error {
	if client.connection == nil {
		return nil
	}

	return client.connection.Close()
}

func (client *EmbeddingClient) Embed(
	ctx context.Context,
	texts []string,
) ([]embedding.Vector, int, error) {
	response, err := client.client.Embed(ctx, &pb.EmbedRequest{Texts: texts})
	if err != nil {
		return nil, 0, err
	}

	return protoVectors(response.GetVectors()), int(response.GetEmbedderMisses()), nil
}

func (client *EmbeddingClient) Pool(
	ctx context.Context,
	texts []string,
	weights []float64,
) (embedding.Vector, int, error) {
	response, err := client.client.Pool(ctx, &pb.PoolRequest{Texts: texts, Weights: weights})
	if err != nil {
		return nil, 0, err
	}

	return protoVector(response.GetCentroid()), int(response.GetEmbedderMisses()), nil
}

func (client *EmbeddingClient) Match(
	ctx context.Context,
	centroid embedding.Vector,
	k int,
) ([]embedding.Match, error) {
	response, err := client.client.Match(ctx, &pb.MatchRequest{
		Centroid: &pb.EmbeddingVector{Values: centroid},
		K:        int32(k),
	})
	if err != nil {
		return nil, err
	}

	matches := make([]embedding.Match, 0, len(response.GetCandidates()))
	for _, candidate := range response.GetCandidates() {
		matches = append(matches, embedding.Match{
			Entity:     candidate.GetEntity(),
			Similarity: candidate.GetSimilarity(),
		})
	}

	return matches, nil
}

func (client *EmbeddingClient) Reset(ctx context.Context) error {
	_, err := client.client.Reset(ctx, &pb.Empty{})

	return err
}

func (client *EmbeddingClient) Resolve(
	ctx context.Context,
	claims []embedding.ClaimInput,
	threshold, nameThreshold float64,
) (embedding.Resolution, error) {
	pbClaims := make([]*pb.ClaimInput, 0, len(claims))
	for _, claim := range claims {
		pbClaims = append(pbClaims, &pb.ClaimInput{
			Text:   claim.Text,
			Hash:   claim.Hash,
			IsName: claim.IsName,
		})
	}
	response, err := client.client.Resolve(ctx, &pb.ResolveRequest{
		Claims:        pbClaims,
		Threshold:     threshold,
		NameThreshold: nameThreshold,
	})
	if err != nil {
		return embedding.Resolution{}, err
	}

	return embedding.Resolution{
		Entity:         response.GetEntity(),
		NewClaimHashes: response.GetNewClaimHashes(),
		Similarity:     response.GetSimilarity(),
		IsNew:          response.GetIsNew(),
		IsNoop:         response.GetIsNoop(),
	}, nil
}

func (client *EmbeddingClient) Upsert(
	ctx context.Context,
	entity string,
	centroid embedding.Vector,
	names []embedding.NamedVec,
) error {
	pbNames := make([]*pb.NamedVector, 0, len(names))
	for _, name := range names {
		pbNames = append(pbNames, &pb.NamedVector{
			NameKey: name.Key,
			Vector:  &pb.EmbeddingVector{Values: name.Vector},
		})
	}
	_, err := client.client.Upsert(ctx, &pb.UpsertRequest{
		Entity:   entity,
		Centroid: &pb.EmbeddingVector{Values: centroid},
		Names:    pbNames,
	})

	return err
}

func (client *EmbeddingClient) NearestName(
	ctx context.Context,
	entity string,
	query embedding.Vector,
) (float64, bool, error) {
	response, err := client.client.NearestName(ctx, &pb.NearestNameRequest{
		Entity: entity,
		Query:  &pb.EmbeddingVector{Values: query},
	})
	if err != nil {
		return 0, false, err
	}

	return response.GetSimilarity(), response.GetFound(), nil
}

func (client *EmbeddingClient) Snapshot(ctx context.Context) ([]byte, error) {
	response, err := client.client.Snapshot(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}

	return response.GetArtifact(), nil
}

func protoVector(v *pb.EmbeddingVector) embedding.Vector {
	if v == nil {
		return nil
	}

	return append(embedding.Vector(nil), v.GetValues()...)
}

func protoVectors(vectors []*pb.EmbeddingVector) []embedding.Vector {
	out := make([]embedding.Vector, len(vectors))
	for i, v := range vectors {
		out[i] = protoVector(v)
	}

	return out
}
