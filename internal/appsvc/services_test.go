// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc_test

import (
	"context"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/source"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestMailSearchUsesRealQueryAndGmailAdapter(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	defer queryService.Close()
	indexedDigest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader:    strings.NewReader("hello bob indexed"),
		MediaType: "message/rfc822",
		Facets:    []contracts.Facet{{Kind: appsvc.MailMessageFacet}},
		Provenance: []contracts.Provenance{{
			SourceKind: "gmail",
			SourceName: "primary",
			ExternalID: "indexed",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testsupport.CleanupQueryObjects(t, indexedDigest) })

	manifest, err := filestoreService.Client.ReadManifest(ctx, indexedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := queryService.Client.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}

	sourceService, err := source.NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	gmail, err := source.NewGmailAdapter("primary", gmailExternalBackend{
		hits: []source.GmailSearchHit{
			{MessageID: "indexed"},
			{MessageID: "fresh", Version: "1"},
		},
		messages: map[string]source.GmailMessage{
			"fresh": {
				MessageID:    "fresh",
				Version:      "1",
				Subject:      "fresh bob",
				Body:         []byte("hello bob fresh"),
				BodyMediaTyp: "text/plain",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	services, err := appsvc.New(appsvc.Options{
		Query:   queryService.Client,
		Objects: filestoreService.Client,
		Sources: appsvc.NewStaticSourceRegistry(gmail),
		Ingest:  sourceService,
	})
	if err != nil {
		t.Fatal(err)
	}

	response, err := services.MailSearch(
		ctx,
		appsvc.SearchOptions{Query: "bob", Limit: 10},
	)
	if err != nil {
		t.Fatal(err)
	}

	if response.Total != 2 {
		t.Fatalf("expected indexed and hydrated Gmail results, got %#v", response)
	}
	if !hasPendingFreshResult(response.Results, "fresh") {
		t.Fatalf("expected hydrated live result with pending states: %#v", response.Results)
	}
}

func TestSummarySearchUsesLiveGmailHydration(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	defer queryService.Close()
	sourceService, err := source.NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	backend := &countingGmailBackend{
		gmailExternalBackend: gmailExternalBackend{
			hits: []source.GmailSearchHit{
				{MessageID: "fresh", Version: "1"},
			},
			messages: map[string]source.GmailMessage{
				"fresh": {
					MessageID: "fresh",
					Version:   "1",
					ThreadID:  "thread-1",
					Subject:   "fresh bob",
					Headers: map[string]string{
						"Date":       "Wed, 27 May 2026 09:15:00 -0600",
						"From":       "Alice <alice@example.test>",
						"To":         "Bob <bob@example.test>",
						"Subject":    "fresh bob",
						"Message-ID": "<fresh@example.test>",
					},
					Body:         []byte("hello bob fresh"),
					BodyMediaTyp: "text/plain",
				},
			},
		},
	}
	gmail, err := source.NewGmailAdapter("primary", backend)
	if err != nil {
		t.Fatal(err)
	}
	services, err := appsvc.New(appsvc.Options{
		Query:   queryService.Client,
		Objects: filestoreService.Client,
		Sources: appsvc.NewStaticSourceRegistry(gmail),
		Ingest:  sourceService,
	})
	if err != nil {
		t.Fatal(err)
	}

	response, err := services.SummarySearch(ctx, appsvc.SearchOptions{
		Query: "bob",
		Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	if backend.searches != 1 {
		t.Fatalf("expected summary_search to use Gmail live search, got %d", backend.searches)
	}
	if response.Returned != 1 ||
		response.Messages[0].MessageID != "<fresh@example.test>" ||
		response.Messages[0].Subject != "fresh bob" ||
		response.Messages[0].From != "alice@example.test" ||
		response.Messages[0].To != "bob@example.test" {
		t.Fatalf("summary search response = %#v", response)
	}
}

func TestSourceActionUsesRealGmailAdapterAndRejectsWrongFacet(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	defer queryService.Close()
	gmail, err := source.NewGmailAdapter("primary", gmailExternalBackend{})
	if err != nil {
		t.Fatal(err)
	}
	services, err := appsvc.New(appsvc.Options{
		Query:   queryService.Client,
		Objects: filestoreService.Client,
		Sources: appsvc.NewStaticSourceRegistry(gmail),
	})
	if err != nil {
		t.Fatal(err)
	}

	mailDigest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader:    strings.NewReader("mail"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: appsvc.MailMessageFacet}},
		Provenance: []contracts.Provenance{{
			SourceKind: "gmail",
			SourceName: "primary",
			ExternalID: "msg-1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	action, err := services.SourceAction(ctx, appsvc.SourceActionRequest{
		Digest: mailDigest,
		Action: "archive",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !action.Applied || action.Attributes["message_id"] != "msg-1" {
		t.Fatalf("unexpected source action result: %#v", action)
	}

	fileDigest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader:    strings.NewReader("file"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "gmail",
			SourceName: "primary",
			ExternalID: "msg-2",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := services.SourceAction(
		ctx,
		appsvc.SourceActionRequest{Digest: fileDigest, Action: "archive"},
	); err == nil {
		t.Fatal("expected non-mail object action rejection")
	}
}

func TestForceAnalysisUsesRealSchedulerService(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	defer queryService.Close()
	digest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader:    strings.NewReader("force me"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	schedulerService := testsupport.StartSchedulerGRPC(
		t,
		ctx,
		filestoreService.Store,
		[]contracts.AnalyzerSpec{
			{Name: "summary", Version: "1", WorkerKind: "go", Deterministic: true},
		},
	)
	defer schedulerService.Close()
	services, err := appsvc.New(appsvc.Options{
		Query:     queryService.Client,
		Objects:   filestoreService.Client,
		Scheduler: schedulerService.Client,
	})
	if err != nil {
		t.Fatal(err)
	}

	response, err := services.ForceAnalysis(ctx, appsvc.ForceAnalysisRequest{
		Digest:      digest,
		Analyzers:   []string{"summary"},
		RequestedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued != 1 || response.Scanned != 1 {
		t.Fatalf("unexpected force response: %#v", response)
	}
}

type gmailExternalBackend struct {
	hits     []source.GmailSearchHit
	messages map[string]source.GmailMessage
}

type countingGmailBackend struct {
	gmailExternalBackend
	searches int
}

func (backend *countingGmailBackend) Search(
	ctx context.Context,
	query string,
	limit int,
) ([]source.GmailSearchHit, error) {
	backend.searches++
	return backend.gmailExternalBackend.Search(ctx, query, limit)
}

func (backend gmailExternalBackend) Search(
	context.Context,
	string,
	int,
) ([]source.GmailSearchHit, error) {
	return append([]source.GmailSearchHit{}, backend.hits...), nil
}

func (backend gmailExternalBackend) ListMessages(
	context.Context,
	source.GmailListRequest,
) (source.GmailListPage, error) {
	return source.GmailListPage{
		Hits: append([]source.GmailSearchHit{}, backend.hits...),
	}, nil
}

func (backend gmailExternalBackend) ListHistory(
	context.Context,
	source.GmailHistoryRequest,
) (source.GmailHistoryPage, error) {
	return source.GmailHistoryPage{}, nil
}

func (backend gmailExternalBackend) GetMessage(
	_ context.Context,
	messageID string,
) (source.GmailMessage, error) {
	return backend.messages[messageID], nil
}

func (backend gmailExternalBackend) ModifyMessage(
	_ context.Context,
	messageID string,
	_ string,
	_ map[string]any,
) (map[string]any, error) {
	return map[string]any{"ok": true, "message_id": messageID}, nil
}

func hasPendingFreshResult(
	results []appsvc.ObjectSearchResult,
	externalID string,
) bool {
	for _, result := range results {
		if result.Attributes["external_id"] == externalID &&
			result.AnalysisPending &&
			result.ProjectionPending {
			return true
		}
	}

	return false
}
