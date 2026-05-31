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

const (
	canonicalParentDigest = contracts.ObjectDigest(
		"ffaf47c75c3539857dd3a70f9a21f8f128bd116884207a92338797dd3effc0ef",
	)
	canonicalHeaderDigest = contracts.ObjectDigest(
		"e37ab049a5bc51afb838f9f7acc0fbc987377bbb5f1d929a2700edab3989d499",
	)
	canonicalBodyDigest = contracts.ObjectDigest(
		"5fa990b9d8e0504fef77a691c6853a15b2a0d9e7b82c54742e5396c6ce2e6710",
	)
	canonicalHTMLDigest = contracts.ObjectDigest(
		"08720dee689f9216196df9b4ffe5baa6501630a6a1046583dc70180651e20e03",
	)
)

func TestCanonicalMessageBuildsTightMailView(t *testing.T) {
	ctx := context.Background()
	services, err := appsvc.New(appsvc.Options{
		Query:   canonicalQueryService{},
		Objects: canonicalObjectService{},
	})
	if err != nil {
		t.Fatal(err)
	}

	response, err := services.Retrieve(ctx, canonicalParentDigest, false)
	if err != nil {
		t.Fatal(err)
	}
	message := response.Message
	if message == nil {
		t.Fatal("expected canonical message")
	}
	if message.MessageID != "<paudley/coding-ethos/pull/217/review/4369413606@github.com>" {
		t.Fatalf("message id = %q", message.MessageID)
	}
	if message.SelectedHeaders.Subject == "" || message.SelectedHeaders.Date == "" {
		t.Fatalf("selected headers incomplete: %#v", message.SelectedHeaders)
	}
	if message.Summary == "" ||
		!strings.Contains(message.Summary, "critical Git policies") {
		t.Fatalf("summary not selected from body analysis: %q", message.Summary)
	}
	if len(message.Categories) != 1 || message.Categories[0] != "primary" {
		t.Fatalf("categories = %#v", message.Categories)
	}
	if strings.Contains(message.Body, "Reply to this email") {
		t.Fatalf("body retained footer noise: %q", message.Body)
	}
	if len(message.Attachments) != 0 {
		t.Fatalf(
			"HTML alternative should not be a canonical attachment: %#v",
			message.Attachments,
		)
	}
	if message.Graph.Source["kind"] != "pull_request_review" ||
		message.Graph.Source["repository"] != "paudley/coding-ethos" {
		t.Fatalf("semantic graph source = %#v", message.Graph.Source)
	}
}

func TestMessageSummaryAndSummarySearchUseCanonicalMessageView(t *testing.T) {
	ctx := context.Background()
	services, err := appsvc.New(appsvc.Options{
		Query:   canonicalQueryService{},
		Objects: canonicalObjectService{},
	})
	if err != nil {
		t.Fatal(err)
	}

	message, err := services.MessageSummary(ctx, appsvc.MessageSummaryRequest{
		MessageID: "<paudley/coding-ethos/pull/217/review/4369413606@github.com>",
	})
	if err != nil {
		t.Fatal(err)
	}
	if message.MessageID == "" ||
		!strings.Contains(message.Summary, "critical Git policies") {
		t.Fatalf("message summary = %#v", message)
	}

	response, err := services.SummarySearch(ctx, appsvc.SearchOptions{
		Query: "repo.kind profiles",
		Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Returned != 1 || response.Messages[0].From != "notifications@github.com" ||
		response.Messages[0].To != "coding-ethos@noreply.github.com" {
		t.Fatalf("summary search response = %#v", response)
	}
	if response.Messages[0].Date != "26/05/26" {
		t.Fatalf("summary search date = %q", response.Messages[0].Date)
	}
	if strings.Contains(response.Messages[0].Summary, "Reply to this email") {
		t.Fatalf("summary search returned body/footer noise: %#v", response.Messages[0])
	}
}

func TestMessageSummaryDoesNotRequireCompletedAnalysis(t *testing.T) {
	ctx := context.Background()
	services, err := appsvc.New(appsvc.Options{
		Query:   canonicalQueryWithoutAnalysisService{},
		Objects: canonicalObjectService{},
	})
	if err != nil {
		t.Fatal(err)
	}

	message, err := services.MessageSummary(ctx, appsvc.MessageSummaryRequest{
		MessageID: "<paudley/coding-ethos/pull/217/review/4369413606@github.com>",
	})
	if err != nil {
		t.Fatal(err)
	}
	if message.MessageID != "<paudley/coding-ethos/pull/217/review/4369413606@github.com>" {
		t.Fatalf("message id = %q", message.MessageID)
	}
	if message.Summary != "" || !strings.Contains(message.Body, "protected branch work") {
		t.Fatalf("expected pending analysis body without summary: %#v", message)
	}
}

type canonicalQueryService struct{}

func (canonicalQueryService) Search(
	context.Context,
	contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	return contracts.SearchResponse{
		Total: 1,
		Results: []contracts.SearchResult{
			{
				ObjectDigest: canonicalParentDigest,
				Title:        "Re: [paudley/coding-ethos] Remove git enforcement optionality (PR #217)",
				Facets:       []string{appsvc.MailMessageFacet},
				Score:        1,
			},
		},
	}, nil
}

func (canonicalQueryService) Structure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (canonicalQueryService) Relationships(
	context.Context,
	contracts.RelationshipRequest,
) (contracts.RelationshipResponse, error) {
	return contracts.RelationshipResponse{}, nil
}

func (canonicalQueryService) Graph(
	context.Context,
	contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	return contracts.GraphResponse{}, nil
}

func (canonicalQueryService) AnalysisStatus(
	context.Context,
	contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	return contracts.AnalysisStatusResponse{
		Statuses: []contracts.AnalysisStatus{
			{
				ObjectDigest: canonicalBodyDigest,
				AnalyzerName: "summary.model",
				Status:       "complete",
				Data: map[string]any{
					"summary": "This update enforces critical Git policies unconditionally while introducing new configuration sections for profiles and repository kinds.",
					"bullets": []any{
						"Support was added for profiles and repo.kind in repository settings.",
					},
				},
			},
			{
				ObjectDigest: canonicalParentDigest,
				AnalyzerName: "categories.sklearn",
				Status:       "complete",
				Data:         map[string]any{"category_ids": []any{"primary"}},
			},
		},
	}, nil
}

func (canonicalQueryService) SourceCursors(
	context.Context,
	contracts.SourceCursorRequest,
) (contracts.SourceCursorResponse, error) {
	return contracts.SourceCursorResponse{}, nil
}

func (canonicalQueryService) RelatedObjects(
	context.Context,
	contracts.RelatedObjectsRequest,
) (contracts.RelatedObjectsResponse, error) {
	return contracts.RelatedObjectsResponse{}, nil
}

type canonicalQueryWithoutAnalysisService struct {
	canonicalQueryService
}

func (canonicalQueryWithoutAnalysisService) AnalysisStatus(
	context.Context,
	contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	return contracts.AnalysisStatusResponse{}, nil
}

type canonicalObjectService struct{}

func (canonicalObjectService) ReadManifest(
	_ context.Context,
	digest contracts.ObjectDigest,
) (contracts.Manifest, error) {
	switch digest {
	case canonicalParentDigest:
		return contracts.Manifest{
			ObjectDigest: canonicalParentDigest,
			ObjectID:     "gmail:primary:19e67b267f02ded5",
			MediaType:    "application/vnd.gmeow.gmail-message+json",
			Facets: []contracts.Facet{{
				Kind: appsvc.MailMessageFacet,
				Metadata: map[string]any{
					"message_id":     "19e67b267f02ded5",
					"rfc_message_id": "<paudley/coding-ethos/pull/217/review/4369413606@github.com>",
					"thread_id":      "19e67b11bf0ccbf4",
					"subject":        "Re: [paudley/coding-ethos] Remove git enforcement optionality (PR #217)",
				},
			}},
			Compound: contracts.Compound{
				IsCompound: true,
				Parts: []contracts.CompoundPart{
					{Digest: canonicalHeaderDigest, Role: "rfc822_headers"},
					{Digest: canonicalBodyDigest, Role: "email_body", Order: 1},
					{Digest: canonicalHTMLDigest, Role: "attachment", Order: 4},
				},
			},
		}, nil
	case canonicalHTMLDigest:
		return contracts.Manifest{
			ObjectDigest: canonicalHTMLDigest,
			MediaType:    "text/html",
			Facets:       []contracts.Facet{{Kind: "file"}},
		}, nil
	default:
		return contracts.Manifest{ObjectDigest: digest}, nil
	}
}

func (canonicalObjectService) GetStructure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (canonicalObjectService) Open(
	_ context.Context,
	digest contracts.ObjectDigest,
) (io.ReadCloser, error) {
	switch digest {
	case canonicalHeaderDigest:
		return io.NopCloser(strings.NewReader(`[
			{"name":"Date","value":"Tue, 26 May 2026 21:30:05 -0700"},
			{"name":"From","value":"gemini-code-assist[bot] <notifications@github.com>"},
			{"name":"To","value":"paudley/coding-ethos <coding-ethos@noreply.github.com>"},
			{"name":"Subject","value":"Re: [paudley/coding-ethos] Remove git enforcement optionality (PR #217)"},
			{"name":"Message-ID","value":"<paudley/coding-ethos/pull/217/review/4369413606@github.com>"}
		]`)), nil
	case canonicalBodyDigest:
		return io.NopCloser(strings.NewReader(canonicalBodyText())), nil
	default:
		return io.NopCloser(strings.NewReader("")), nil
	}
}

func canonicalBodyText() string {
	return "@gemini-code-assist[bot] commented on this pull request.\n\n" +
		"## Code Review\n\n" +
		"This pull request removes the ability for consumer repositories to disable invariant Git policies " +
		"such as protected branch work, hook bypass prevention, and history rewrite prevention via their " +
		"`repo_config.yaml` configuration. It cleans up the corresponding `enabled` toggles from the " +
		"configuration files, updates the policy compiler to enforce these policies unconditionally, and " +
		"adds validation tests to reject any attempts to disable them. Additionally, it introduces support " +
		"for a `profiles` section and a `repo.kind` setting in the repository configuration. There are no " +
		"review comments to address, and I have no further feedback to provide.\n\n" +
		"-- \nReply to this email directly or view it on GitHub:\n" +
		"https://github.com/paudley/coding-ethos/pull/217#pullrequestreview-4369413606"
}
