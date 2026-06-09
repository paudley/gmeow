// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

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
	hasAnnotation, err := client.HasAnalysisAnnotation(
		ctx,
		digest,
		"grpc.checked",
		"v1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !hasAnnotation {
		t.Fatal("expected matching analysis annotation through gRPC client")
	}
	hasAnnotation, err = client.HasAnalysisAnnotation(
		ctx,
		digest,
		"grpc.checked",
		"v2",
	)
	if err != nil {
		t.Fatal(err)
	}
	if hasAnnotation {
		t.Fatal("stale analysis annotation version must not match")
	}
	if !found {
		t.Fatal("expected annotation written through gRPC client")
	}
	if err := client.WriteSourceCursor(ctx, contracts.SourceCursor{
		SourceKind: "gmail",
		SourceName: "primary",
		Cursor:     map[string]any{"page_token": "next"},
	}); err != nil {
		t.Fatal(err)
	}
	cursor, found, err := client.ReadSourceCursor(ctx, contracts.SourceCursorRef{
		SourceKind: "gmail",
		SourceName: "primary",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found || cursor.Cursor["page_token"] != "next" {
		t.Fatalf(
			"expected cursor round trip through gRPC, found=%t cursor=%#v",
			found,
			cursor,
		)
	}
}

func TestFilestoreClientWritesLargeAnalysisAnnotation(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("large annotation target"),
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

	if err := client.WriteAnnotation(ctx, contracts.Annotation{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		Kind:          "analysis",
		AnalyzerName:  "large.checked",
		AnalyzerVer:   "v1",
		Data: map[string]any{
			"status":  "complete",
			"payload": strings.Repeat("x", 5<<20),
		},
	}); err != nil {
		t.Fatal(err)
	}

	hasAnnotation, err := client.HasAnalysisAnnotation(ctx, digest, "large.checked", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if !hasAnnotation {
		t.Fatal("expected large analysis annotation through gRPC client")
	}
}

func TestFilestoreClientStorageVerifyAndPathOverGRPC(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader: strings.NewReader("grpc storage verify path"),
		Facets: []contracts.Facet{{Kind: "file", Version: "1"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "gmail", SourceName: "primary", ExternalID: "m1", ExternalVersion: "v1",
		}},
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

	// StorageBreakdown round-trips: positive logical totals and a manifest row.
	report, err := client.StorageBreakdown(ctx, filestore.StorageBreakdownRequest{
		Digest:         digest,
		RecursiveParts: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.RootDigest != digest || report.TotalLogicalBytes <= 0 ||
		len(report.Files) == 0 {
		t.Fatalf("unexpected storage breakdown over gRPC: %#v", report)
	}
	hasManifest := false
	for _, file := range report.Files {
		if file.Role == "manifest" {
			hasManifest = true
		}
	}
	if !hasManifest {
		t.Fatalf("expected a manifest row over gRPC: %#v", report.Files)
	}

	// Verify round-trips clean.
	verifyReport, err := client.Verify(ctx, filestore.VerifyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if verifyReport.Status != filestore.VerifyStatusOK {
		t.Fatalf("expected clean verify over gRPC: %#v", verifyReport)
	}

	// ResolvePath round-trips: the content chunk packs resolve as a directory.
	pathReport, err := client.ResolvePath(
		ctx,
		filestore.PathResolveRequest{Path: "chunk-packs"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if pathReport.Kind != "directory" {
		t.Fatalf("unexpected path resolve over gRPC: %#v", pathReport)
	}
}

func TestFilestoreServerNotifiesSchedulerOnObjectAndAnnotationChanges(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	notifier := &recordingObjectChangeNotifier{}
	endpoint, cleanup := serveTestFilestore(
		t,
		store,
		WithObjectChangeNotifier(notifier),
	)
	defer cleanup()
	client, err := NewFilestoreClient(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	digest, err := client.Put(ctx, PutRequest{
		Reader:    strings.NewReader("notify object"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file", Version: "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.WriteAnnotation(ctx, contracts.Annotation{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		Kind:          "analysis",
		AnalyzerName:  "notify.checked",
		AnalyzerVer:   "v1",
		Data:          map[string]any{"status": "complete"},
	}); err != nil {
		t.Fatal(err)
	}

	requests := notifier.waitForRequests(t, 2)
	if len(requests) != 2 {
		t.Fatalf("expected object and projection notifications, got %#v", requests)
	}
	if requests[0].ProjectionOnly || requests[0].Reason != "object_changed" {
		t.Fatalf("expected object change notification, got %#v", requests[0])
	}
	if !requests[1].ProjectionOnly || requests[1].Reason != "projection_refresh" {
		t.Fatalf("expected projection-only notification, got %#v", requests[1])
	}
}

func TestFilestorePutDoesNotBlockOnSchedulerNotification(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	notifier := newBlockingObjectChangeNotifier()
	defer notifier.release()
	endpoint, cleanup := serveTestFilestore(
		t,
		store,
		WithObjectChangeNotifier(notifier),
	)
	defer cleanup()
	client, err := NewFilestoreClient(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		_, putErr := client.Put(ctx, PutRequest{
			Reader: strings.NewReader("notify object"),
			Facets: []contracts.Facet{{Kind: "file", Version: "1"}},
		})
		done <- putErr
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Put blocked on scheduler notification")
	}
}

func TestObjectStreamReaderDoesNotDrainStreamBeforeReturning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &objectStreamReader{
		stream: &blockingObjectStream{
			ctx:    ctx,
			chunks: [][]byte{[]byte("0123456789")},
		},
		cancel: cancel,
	}

	buffer := make([]byte, 4)
	n, err := reader.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buffer) || string(buffer) != "0123" {
		t.Fatalf("unexpected partial read n=%d content=%q", n, string(buffer))
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected reader close to cancel stream context")
	}
}

func serveTestFilestore(
	t *testing.T,
	store filestore.Store,
	options ...FilestoreServerOption,
) (Endpoint, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(defaultServerOptions()...)
	pb.RegisterFilestoreServiceServer(server, NewFilestoreServer(store, options...))
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

type recordingObjectChangeNotifier struct {
	mu       sync.Mutex
	requests []contracts.ObjectChangeRequest
}

func (notifier *recordingObjectChangeNotifier) NotifyObjectsChanged(
	_ context.Context,
	request contracts.ObjectChangeRequest,
) (contracts.SchedulerScanResponse, error) {
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	notifier.requests = append(notifier.requests, request)

	return contracts.SchedulerScanResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
	}, nil
}

func (notifier *recordingObjectChangeNotifier) waitForRequests(
	t *testing.T,
	count int,
) []contracts.ObjectChangeRequest {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		notifier.mu.Lock()
		if len(notifier.requests) >= count {
			requests := append([]contracts.ObjectChangeRequest{}, notifier.requests...)
			notifier.mu.Unlock()

			return requests
		}
		notifier.mu.Unlock()

		select {
		case <-deadline:
			notifier.mu.Lock()
			requests := append([]contracts.ObjectChangeRequest{}, notifier.requests...)
			notifier.mu.Unlock()
			t.Fatalf("expected %d notifications, got %#v", count, requests)
		case <-ticker.C:
		}
	}
}

type blockingObjectChangeNotifier struct {
	releaseCh chan struct{}
	once      sync.Once
}

func newBlockingObjectChangeNotifier() *blockingObjectChangeNotifier {
	return &blockingObjectChangeNotifier{releaseCh: make(chan struct{})}
}

func (notifier *blockingObjectChangeNotifier) NotifyObjectsChanged(
	_ context.Context,
	_ contracts.ObjectChangeRequest,
) (contracts.SchedulerScanResponse, error) {
	<-notifier.releaseCh

	return contracts.SchedulerScanResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
	}, nil
}

func (notifier *blockingObjectChangeNotifier) release() {
	notifier.once.Do(func() {
		close(notifier.releaseCh)
	})
}

type blockingObjectStream struct {
	grpc.ClientStream
	ctx    context.Context
	chunks [][]byte
}

func (stream *blockingObjectStream) Recv() (*pb.ObjectChunk, error) {
	if len(stream.chunks) > 0 {
		chunk := stream.chunks[0]
		stream.chunks = stream.chunks[1:]

		return &pb.ObjectChunk{Data: chunk}, nil
	}

	<-stream.ctx.Done()

	return nil, stream.ctx.Err()
}
