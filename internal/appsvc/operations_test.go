// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
)

func TestMailSearchOperationPersistsProgressAndRefreshesTerminalRequest(t *testing.T) {
	ctx := context.Background()
	services, err := appsvc.New(appsvc.Options{
		Query:   operationQuery{},
		Objects: operationObjects{},
	})
	if err != nil {
		t.Fatal(err)
	}

	request := appsvc.SearchOptions{Query: "needle", Limit: 5}
	first, err := services.MailSearchOperation(ctx, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := services.MailSearchOperation(ctx, request, nil)
	if err != nil {
		t.Fatal(err)
	}

	if first.Operation.OperationID == "" {
		t.Fatal("operation id should be returned")
	}
	if first.Operation.OperationID == second.Operation.OperationID {
		t.Fatalf(
			"operation id = %q, want a new operation after terminal %q",
			second.Operation.OperationID,
			first.Operation.OperationID,
		)
	}
	if first.Operation.Status != contracts.OperationStatusComplete {
		t.Fatalf("operation status = %q, want complete", first.Operation.Status)
	}
	if len(first.Operation.Progress) == 0 {
		t.Fatal("operation progress should be recorded")
	}
}

func TestMailSearchOperationDeduplicatesRunningRequest(t *testing.T) {
	ctx := context.Background()
	query := &blockingOperationQuery{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	services, err := appsvc.New(appsvc.Options{
		Query:   query,
		Objects: operationObjects{},
	})
	if err != nil {
		t.Fatal(err)
	}

	request := appsvc.SearchOptions{Query: "needle", Limit: 5}
	firstDone := make(chan operationResult, 1)
	go func() {
		response, err := services.MailSearchOperation(ctx, request, nil)
		firstDone <- operationResult{response: response, err: err}
	}()

	select {
	case <-query.started:
	case <-time.After(time.Second):
		t.Fatal("first operation did not start")
	}

	secondObserved := make(chan struct{})
	var observed atomic.Bool
	secondDone := make(chan operationResult, 1)
	go func() {
		response, err := services.MailSearchOperation(
			ctx,
			request,
			func(contracts.OperationProgressEvent) {
				if observed.CompareAndSwap(false, true) {
					close(secondObserved)
				}
			},
		)
		secondDone <- operationResult{response: response, err: err}
	}()

	select {
	case <-secondObserved:
	case <-time.After(time.Second):
		t.Fatal("second operation did not attach to running operation")
	}
	close(query.release)

	first := <-firstDone
	second := <-secondDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	if second.err != nil {
		t.Fatal(second.err)
	}
	if first.response.Operation.OperationID != second.response.Operation.OperationID {
		t.Fatalf(
			"operation id = %q, want running operation %q",
			second.response.Operation.OperationID,
			first.response.Operation.OperationID,
		)
	}
	if query.calls.Load() != 1 {
		t.Fatalf("search calls = %d, want 1", query.calls.Load())
	}
}

type operationResult struct {
	response contracts.OperationResultResponse
	err      error
}

type operationQuery struct{}

func (operationQuery) Search(
	context.Context,
	contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	return contracts.SearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results: []contracts.SearchResult{{
			ObjectDigest: "digest-1",
			Title:        "needle",
		}},
		Total: 1,
	}, nil
}

func (operationQuery) Structure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (operationQuery) Relationships(
	context.Context,
	contracts.RelationshipRequest,
) (contracts.RelationshipResponse, error) {
	return contracts.RelationshipResponse{}, nil
}

func (operationQuery) Graph(
	context.Context,
	contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	return contracts.GraphResponse{}, nil
}

func (operationQuery) AnalysisStatus(
	context.Context,
	contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	return contracts.AnalysisStatusResponse{}, nil
}

func (operationQuery) SourceCursors(
	context.Context,
	contracts.SourceCursorRequest,
) (contracts.SourceCursorResponse, error) {
	return contracts.SourceCursorResponse{}, nil
}

func (operationQuery) RelatedObjects(
	context.Context,
	contracts.RelatedObjectsRequest,
) (contracts.RelatedObjectsResponse, error) {
	return contracts.RelatedObjectsResponse{}, nil
}

type blockingOperationQuery struct {
	calls   atomic.Int32
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (query *blockingOperationQuery) Search(
	ctx context.Context,
	request contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	query.calls.Add(1)
	query.once.Do(func() { close(query.started) })
	select {
	case <-ctx.Done():
		return contracts.SearchResponse{}, ctx.Err()
	case <-query.release:
	}

	return operationQuery{}.Search(ctx, request)
}

func (query *blockingOperationQuery) Structure(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Structure, error) {
	return operationQuery{}.Structure(ctx, digest)
}

func (query *blockingOperationQuery) Relationships(
	ctx context.Context,
	request contracts.RelationshipRequest,
) (contracts.RelationshipResponse, error) {
	return operationQuery{}.Relationships(ctx, request)
}

func (query *blockingOperationQuery) Graph(
	ctx context.Context,
	request contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	return operationQuery{}.Graph(ctx, request)
}

func (query *blockingOperationQuery) AnalysisStatus(
	ctx context.Context,
	request contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	return operationQuery{}.AnalysisStatus(ctx, request)
}

func (query *blockingOperationQuery) SourceCursors(
	ctx context.Context,
	request contracts.SourceCursorRequest,
) (contracts.SourceCursorResponse, error) {
	return operationQuery{}.SourceCursors(ctx, request)
}

func (query *blockingOperationQuery) RelatedObjects(
	ctx context.Context,
	request contracts.RelatedObjectsRequest,
) (contracts.RelatedObjectsResponse, error) {
	return operationQuery{}.RelatedObjects(ctx, request)
}

type operationObjects struct{}

func (operationObjects) ReadManifest(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Manifest, error) {
	return contracts.Manifest{}, nil
}

func (operationObjects) GetStructure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (operationObjects) Open(
	context.Context,
	contracts.ObjectDigest,
) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
