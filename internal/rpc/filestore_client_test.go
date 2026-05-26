// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

func TestFilestoreClientUsesServiceForObjectAndAnnotationAccess(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("grpc object access"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file", Version: "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, cleanup := serveTestFilestore(t, store)
	defer cleanup()
	client, err := NewFilestoreClient(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	manifest, err := client.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ObjectDigest != digest {
		t.Fatalf("expected manifest digest %s, got %s", digest, manifest.ObjectDigest)
	}
	reader, err := client.Open(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "grpc object access" {
		t.Fatalf("unexpected object content %q", string(content))
	}
	if err := client.WriteAnnotation(ctx, contracts.Annotation{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		Kind:          "analysis",
		AnalyzerName:  "grpc.checked",
		AnalyzerVer:   "v1",
		Data:          map[string]any{"status": "complete"},
	}); err != nil {
		t.Fatal(err)
	}
	found := false
	if err := store.WalkProjection(ctx, func(object filestore.ProjectionObject) error {
		for _, annotation := range object.Annotations {
			if annotation.AnalyzerName == "grpc.checked" {
				found = true
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected annotation written through gRPC client")
	}
}

func serveTestFilestore(
	t *testing.T,
	store filestore.Store,
) (Endpoint, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterFilestoreServiceServer(server, NewFilestoreServer(store))
	done := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(done)
	}()
	return Endpoint{Network: "tcp", Address: listener.Addr().String()}, func() {
		server.Stop()
		_ = listener.Close()
		<-done
	}
}
