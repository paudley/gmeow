// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
)

func TestMailSearchOperationPersistsProgressAndDeduplicatesRequest(t *testing.T) {
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
	if first.Operation.OperationID != second.Operation.OperationID {
		t.Fatalf(
			"operation id = %q, want existing %q",
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
