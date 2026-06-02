// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/embedding"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

// stubEmbedder returns a deterministic unit vector per text so the round-trip is
// reproducible without a live model endpoint.
type stubEmbedder struct{ dim int }

func (s stubEmbedder) Embed(
	_ context.Context,
	texts []string,
) ([]embedding.Vector, error) {
	out := make([]embedding.Vector, len(texts))
	for i, text := range texts {
		v := make(embedding.Vector, s.dim)
		var seed uint32 = 2166136261
		for _, b := range []byte(text) {
			seed = (seed ^ uint32(b)) * 16777619
		}
		for j := range v {
			seed = seed*1664525 + 1013904223
			v[j] = float32(int32(seed%2000)-1000) / 1000.0
		}
		out[i] = embedding.Normalize(v)
	}

	return out, nil
}

func serveTestEmbedding(t *testing.T) (Endpoint, *embedding.Service, func()) {
	t.Helper()
	service := embedding.NewService(
		embedding.NewResolver(
			embedding.NewMemoryCache(),
			stubEmbedder{dim: embedding.FullDim},
		),
		embedding.NewEntityIndex(embedding.FullDim, embedding.CoarseDim),
		"stub-model",
	)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterEmbeddingServiceServer(server, NewEmbeddingServer(service))
	done := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(done)
	}()

	return Endpoint{Network: "tcp", Address: listener.Addr().String()}, service, func() {
		server.Stop()
		_ = listener.Close()
		<-done
	}
}

func TestEmbeddingClientResolveRPCRoundTrip(t *testing.T) {
	ctx := context.Background()
	endpoint, _, cleanup := serveTestEmbedding(t)
	defer cleanup()

	client, err := NewEmbeddingClient(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// A fresh person mints an entity; re-resolving the same claims is a NOOP —
	// the importer's mint/delta/NOOP signal travels over gRPC.
	claims := []embedding.ClaimInput{
		{
			Text:   "name: ada lovelace",
			Hash:   embedding.StatementHash("name: ada lovelace"),
			IsName: true,
		},
		{
			Text: "email: ada@analytical.example",
			Hash: embedding.StatementHash("email: ada@analytical.example"),
		},
	}
	resolution, err := client.Resolve(ctx, claims, 0.5, 0.5)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !resolution.IsNew || resolution.Entity == "" ||
		len(resolution.NewClaimHashes) != 2 {
		t.Fatalf("resolve: want a new entity with 2 new claims, got %+v", resolution)
	}
	noop, err := client.Resolve(ctx, claims, 0.5, 0.5)
	if err != nil {
		t.Fatalf("re-resolve: %v", err)
	}
	if !noop.IsNoop || noop.Entity != resolution.Entity {
		t.Fatalf("re-resolve: want NOOP on %s, got %+v", resolution.Entity, noop)
	}
}

func TestEmbeddingClientResolveDeltaNoopRoundTrip(t *testing.T) {
	ctx := context.Background()
	endpoint, _, cleanup := serveTestEmbedding(t)
	defer cleanup()

	client, err := NewEmbeddingClient(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	claims := []string{"name:patrick audley", "email:paudley@blackcat.ca", "org:blackcat"}

	// Seed an entity from the pooled centroid (the index.ttl -> 1 entity case).
	centroid, misses, err := client.Pool(ctx, claims, nil)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if misses != 3 {
		t.Fatalf("seed pool misses=%d, want 3", misses)
	}
	if err := client.Upsert(ctx, "01PAUDLEY", centroid, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// A re-ingest of the same claims must match the entity AND hit the embedder
	// zero times (the NOOP signal) — the cache served every claim.
	again, misses, err := client.Pool(ctx, claims, nil)
	if err != nil {
		t.Fatalf("re-pool: %v", err)
	}
	if misses != 0 {
		t.Fatalf("redundant re-ingest misses=%d, want 0 (NOOP)", misses)
	}
	matches, err := client.Match(ctx, again, 1)
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(matches) == 0 || matches[0].Entity != "01PAUDLEY" ||
		matches[0].Similarity < 0.99 {
		t.Fatalf("match=%+v, want 01PAUDLEY ~1.0", matches)
	}

	// Snapshot must round-trip the index out of the service.
	artifact, err := client.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	loaded, err := embedding.LoadEntityIndex(artifact)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if loaded.Len() != 1 {
		t.Fatalf("loaded index len=%d, want 1", loaded.Len())
	}
}
