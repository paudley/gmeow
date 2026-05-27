// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mcpiface

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	toon "github.com/toon-format/toon-go"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestMCPToolsReturnToonOnlyContent(t *testing.T) {
	ctx := context.Background()
	services, digest := testServicesWithDigest(t, "hello mcp mail")
	server, err := New(services)
	if err != nil {
		t.Fatal(err)
	}

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverErr := make(chan error, 1)
	go func() {
		_, err := server.server.Connect(ctx, serverTransport, nil)
		serverErr <- err
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}

	mail := callToolToon(
		t,
		ctx,
		session,
		"mail_search",
		map[string]any{"query": "mcp", "limit": 5},
		false,
	)
	operationID, ok := mail.object["operation_id"].(string)
	if !ok || operationID == "" {
		t.Fatalf("mail_search operation_id missing from TOON: %#v", mail.object)
	}

	tests := []struct {
		arguments map[string]any
		name      string
		wantTool  string
		wantKeys  []string
		wantText  []string
		isError   bool
	}{
		{
			name:      "object_search",
			arguments: map[string]any{"query": "hello", "limit": 5},
			wantTool:  "object_search",
			wantKeys:  []string{"query", "results", "returned", "total"},
		},
		{
			name: "object_retrieve",
			arguments: map[string]any{
				"digest":          string(digest),
				"include_content": true,
			},
			wantTool: "object_retrieve",
			wantKeys: []string{"operation_id", "name", "result", "status"},
			wantText: []string{"content: hello mcp mail"},
		},
		{
			name:      "get_structure",
			arguments: map[string]any{"digest": string(digest)},
			wantTool:  "get_structure",
			wantKeys:  []string{"digest", "facets"},
		},
		{
			name:      "get_provenance",
			arguments: map[string]any{"digest": string(digest)},
			wantTool:  "get_provenance",
			wantKeys:  []string{"provenance"},
		},
		{
			name:      "get_facets",
			arguments: map[string]any{"digest": string(digest)},
			wantTool:  "get_facets",
			wantKeys:  []string{"facets"},
			wantText:  []string{"mail_message"},
		},
		{
			name:      "compound_expand",
			arguments: map[string]any{"digest": string(digest)},
			wantTool:  "compound_expand",
			wantKeys:  []string{"is_compound"},
		},
		{
			name: "graph_explore",
			arguments: map[string]any{
				"node":           string(digest),
				"limit":          5,
				"schema_version": 1,
			},
			wantTool: "graph_explore",
			wantKeys: []string{"operation_id", "result", "status"},
		},
		{
			name: "analysis_status",
			arguments: map[string]any{
				"object_digests": []any{string(digest)},
				"limit":          5,
				"schema_version": 1,
			},
			wantTool: "analysis_status",
			wantKeys: []string{"operation_id", "result", "status"},
		},
		{
			name:      "force_analysis",
			arguments: map[string]any{"digest": string(digest)},
			wantTool:  "force_analysis",
			wantKeys:  []string{"error"},
			isError:   true,
		},
		{
			name:      "operation_status",
			arguments: map[string]any{"operation_id": operationID},
			wantTool:  "operation_status",
			wantKeys:  []string{"operation_id", "name", "progress", "status"},
		},
		{
			name:      "operation_result",
			arguments: map[string]any{"operation_id": operationID},
			wantTool:  "operation_result",
			wantKeys:  []string{"operation_id", "name", "result", "status"},
		},
		{
			name:      "operation_resume",
			arguments: map[string]any{"operation_id": operationID},
			wantTool:  "operation_resume",
			wantKeys:  []string{"operation_id", "name", "result", "status"},
		},
		{
			name:      "source_action",
			arguments: map[string]any{"digest": string(digest), "action": "archive"},
			wantTool:  "source_action",
			wantKeys:  []string{"error"},
			isError:   true,
		},
		{
			name:      "ops_status",
			arguments: map[string]any{},
			wantTool:  "ops_status",
			wantKeys:  []string{"counts", "scheduler"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			call := callToolToon(
				t,
				ctx,
				session,
				test.name,
				test.arguments,
				test.isError,
			)
			decoded := call.object
			if decoded["tool"] != test.wantTool {
				t.Fatalf("tool = %#v, want %q in %#v", decoded["tool"], test.wantTool, decoded)
			}
			for _, key := range test.wantKeys {
				if _, ok := decoded[key]; !ok {
					t.Fatalf("%s missing key %q in %#v", test.name, key, decoded)
				}
			}
			for _, snippet := range test.wantText {
				if !strings.Contains(call.text, snippet) {
					t.Fatalf("%s missing TOON snippet %q in:\n%s", test.name, snippet, call.text)
				}
			}
		})
	}
}

func TestMCPStreamableHTTPMailSearchUsesAppServices(t *testing.T) {
	ctx := context.Background()
	services := testServices(t, "hello streamable mcp mail")
	httpServer := httptest.NewServer(NewStreamableHandler(
		services,
		HTTPOptions{SessionTimeout: time.Minute},
	))
	defer httpServer.Close()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint: httpServer.URL + streamableEndpoint,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "mail_search",
		Arguments: map[string]any{"query": "streamable", "limit": 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("tool returned error: %#v", result.Content)
	}
	text := toonText(t, result)
	if !strings.Contains(text, "tool: mail_search") {
		t.Fatalf("expected TOON mail_search output, got:\n%s", text)
	}
}

func TestMCPMailSearchReturnsCanonicalMessageView(t *testing.T) {
	message := testCanonicalMessage()
	result, err := toonToolResult(toonSearch(
		"mail_search",
		appsvc.SearchOptions{Query: "repo.kind profiles invariant Git policies", Limit: 5},
		appsvc.ObjectSearchResponse{
			Total: 1,
			Results: []appsvc.ObjectSearchResult{{
				ObjectDigest: contracts.ObjectDigest(message.Digest),
				Message:      &message,
				Facets:       []string{appsvc.MailMessageFacet},
				Score:        1,
			}},
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	searchText := toonText(t, result)
	for _, snippet := range []string{
		"message:",
		"message_id: <paudley/coding-ethos/pull/217/review/4369413606@github.com>",
		"selected_headers:",
		"This update enforces critical Git policies unconditionally",
		"categories[1]: primary",
		"kind: pull_request_review",
		"repository: paudley/coding-ethos",
		"pull_request: 217",
		"body: \"@gemini-code-assist[bot] commented on this pull request.",
		"attachments[0]:",
	} {
		if !strings.Contains(searchText, snippet) {
			t.Fatalf("mail_search missing %q in:\n%s", snippet, searchText)
		}
	}
	for _, forbidden := range []string{
		"ner.spacy",
		"contains",
		"part_of",
		"Reply to this email",
		"text/html",
	} {
		if strings.Contains(searchText, forbidden) {
			t.Fatalf("mail_search included forbidden %q in:\n%s", forbidden, searchText)
		}
	}

	retrieve, err := toonToolResult(toonRetrieve(appsvc.RetrieveResponse{
		Manifest: contracts.Manifest{
			ObjectDigest: contracts.ObjectDigest(message.Digest),
			ObjectID:     message.ObjectID,
			MediaType:    "application/vnd.gmeow.gmail-message+json",
		},
		Message: &message,
	}))
	if err != nil {
		t.Fatal(err)
	}
	retrieveText := toonText(t, retrieve)
	if !strings.Contains(retrieveText, "message:") ||
		!strings.Contains(retrieveText, "selected_headers:") ||
		!strings.Contains(retrieveText, "repo.kind") {
		t.Fatalf("object_retrieve missing canonical message in:\n%s", retrieveText)
	}

	summary, err := toonToolResult(toonSummarySearch(appsvc.SummarySearchResponse{
		Query:    "repo.kind profiles",
		Total:    1,
		Returned: 1,
		Messages: []appsvc.MessageSummaryListItem{{
			MessageID: message.MessageID,
			Date:      "27/05/26",
			Subject:   message.SelectedHeaders.Subject,
			To:        "coding-ethos@noreply.github.com",
			From:      "notifications@github.com",
			Summary:   message.Summary,
		}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	summaryText := toonText(t, summary)
	for _, snippet := range []string{
		"tool: summary_search",
		"msgid_subject",
		"27/05/26",
		"coding-ethos@noreply.github.com / notifications@github.com",
		"This update enforces critical Git policies unconditionally",
	} {
		if !strings.Contains(summaryText, snippet) {
			t.Fatalf("summary_search missing %q in:\n%s", snippet, summaryText)
		}
	}
	if strings.Contains(summaryText, "bullets") || strings.Contains(summaryText, "body:") {
		t.Fatalf("summary_search should stay compact:\n%s", summaryText)
	}
}

func TestMCPStreamableHTTPObjectRetrieveIncludesContent(t *testing.T) {
	ctx := context.Background()
	services, digest := testServicesWithDigest(t, "hello streamable retrieve")
	httpServer := httptest.NewServer(NewStreamableHandler(
		services,
		HTTPOptions{SessionTimeout: time.Minute},
	))
	defer httpServer.Close()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint: httpServer.URL + streamableEndpoint,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "object_retrieve",
		Arguments: map[string]any{
			"digest":          string(digest),
			"include_content": true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("tool returned error: %#v", result.Content)
	}
	text := toonText(t, result)
	if !strings.Contains(text, "tool: object_retrieve") ||
		!strings.Contains(text, "hello streamable retrieve") {
		t.Fatalf("expected retrieved content in TOON output: %s", text)
	}
}

func TestToonSearchOutputUsesTableRows(t *testing.T) {
	result, err := toonToolResult(toonSearch(
		"object_search",
		appsvc.SearchOptions{Query: "needle"},
		appsvc.ObjectSearchResponse{
			Total: 2,
			Results: []appsvc.ObjectSearchResult{
				{
					ObjectDigest: "digest-1",
					Title:        "first",
					Snippet:      "alpha",
					Facets:       []string{"mail_message"},
					Score:        1.25,
				},
				{
					ObjectDigest:      "digest-2",
					Title:             "second",
					Snippet:           "beta",
					ProjectionPending: true,
				},
			},
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	text := toonText(t, result)
	for _, snippet := range []string{
		"tool: object_search",
		"query: needle",
		"results[2]{rank,digest,score,title,snippet,facets,pending}:",
		`1,digest-1,1.25,first,alpha,mail_message,""`,
		`2,digest-2,0,second,beta,"",projection`,
	} {
		if !strings.Contains(text, snippet) {
			t.Fatalf("missing %q in:\n%s", snippet, text)
		}
	}
}

func TestToonOpsStatusConvertsJSONDecodedMetrics(t *testing.T) {
	output := toonOpsStatus(appsvc.OpsStatusResponse{
		Metadata: map[string]any{
			"metrics": map[string]any{
				"latency": float64(1.5),
				"queued":  float64(2),
				"ignored": "not numeric",
			},
		},
	})

	if output.Metrics["latency"] != 1.5 {
		t.Fatalf("latency metric = %v, want 1.5", output.Metrics["latency"])
	}
	if output.Metrics["queued"] != 2 {
		t.Fatalf("queued metric = %v, want 2", output.Metrics["queued"])
	}
	if _, ok := output.Metrics["ignored"]; ok {
		t.Fatalf("non-numeric metric copied into output: %#v", output.Metrics)
	}
}

func TestToonNestedOperationResultUsesExplicitMapAdapters(t *testing.T) {
	output, ok := toonNestedOperationResult("mail_search", map[string]any{
		"total": float64(1),
		"results": []any{
			map[string]any{
				"object_digest":      "digest-1",
				"score":              float64(0.75),
				"title":              "subject",
				"snippet":            "preview",
				"facets":             []any{"mail_message", "thread"},
				"analysis_pending":   true,
				"projection_pending": false,
			},
		},
	}).(toonSearchOutput)
	if !ok {
		t.Fatalf("nested result = %T, want toonSearchOutput", output)
	}
	if output.Total != 1 || output.Returned != 1 {
		t.Fatalf("search totals = %#v", output)
	}
	if output.Results[0].Facets != "mail_message|thread" {
		t.Fatalf("facets = %q, want joined facets", output.Results[0].Facets)
	}
	if output.Results[0].Pending != "analysis" {
		t.Fatalf("pending = %q, want analysis", output.Results[0].Pending)
	}
}

func TestMCPStreamableHTTPHandlesConcurrentSessions(t *testing.T) {
	ctx := context.Background()
	services := testServices(t, "hello concurrent mcp mail")
	httpServer := httptest.NewServer(NewStreamableHandler(
		services,
		HTTPOptions{SessionTimeout: time.Minute},
	))
	defer httpServer.Close()

	var wait sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
			session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
				Endpoint:             httpServer.URL + streamableEndpoint,
				DisableStandaloneSSE: true,
			}, nil)
			if err != nil {
				errs <- err

				return
			}
			defer session.Close()
			_, err = session.CallTool(ctx, &mcp.CallToolParams{
				Name:      "mail_search",
				Arguments: map[string]any{"query": "concurrent", "limit": 5},
			})
			if err != nil {
				errs <- err
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMCPStreamableHTTPLifecycleAndHeaders(t *testing.T) {
	services := testServices(t, "hello lifecycle mcp mail")
	httpServer := httptest.NewServer(NewStreamableHandler(
		services,
		HTTPOptions{SessionTimeout: time.Minute},
	))
	defer httpServer.Close()

	response := postMCPMessage(
		t,
		httpServer.URL+streamableEndpoint,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}`,
	)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("unexpected initialize status %s: %s", response.Status, body)
	}
	if contentType := response.Header.Get(
		"Content-Type",
	); !strings.Contains(
		contentType,
		"text/event-stream",
	) {
		t.Fatalf("expected SSE response, got %q", contentType)
	}
	sessionID := response.Header.Get("Mcp-Session-Id")
	if sessionID == "" {
		t.Fatal("expected MCP session ID")
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "event: message") ||
		!strings.Contains(string(body), "id: ") ||
		!strings.Contains(string(body), `"protocolVersion"`) {
		t.Fatalf("unexpected SSE initialize body: %s", body)
	}

	request, err := http.NewRequest(
		http.MethodDelete,
		httpServer.URL+streamableEndpoint,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Mcp-Session-Id", sessionID)
	deleteResponse, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer deleteResponse.Body.Close()
	if deleteResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected DELETE status: %s", deleteResponse.Status)
	}

	request, err = http.NewRequest(http.MethodGet, httpServer.URL+streamableEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Mcp-Session-Id", sessionID)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("expected deleted session to be missing, got %s", response.Status)
	}
}

func TestMCPStreamableHTTPRejectsInvalidRequests(t *testing.T) {
	services := testServices(t, "hello invalid mcp mail")
	httpServer := httptest.NewServer(NewStreamableHandler(services, HTTPOptions{}))
	defer httpServer.Close()

	request, err := http.NewRequest(http.MethodGet, httpServer.URL+streamableEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected missing Accept to fail, got %s", response.Status)
	}

	request, err = http.NewRequest(
		http.MethodPost,
		httpServer.URL+streamableEndpoint,
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept", "application/json, text/event-stream")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("expected missing Content-Type to fail, got %s", response.Status)
	}
}

func TestMCPStreamableHTTPHealthAndMetrics(t *testing.T) {
	services := testServices(t, "hello health mcp mail")
	httpServer := httptest.NewServer(NewStreamableHandler(services, HTTPOptions{}))
	defer httpServer.Close()

	response, err := http.Get(httpServer.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected health status: %s", response.Status)
	}

	response, err = http.Get(httpServer.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected metrics status: %s", response.Status)
	}
	if contentType := response.Header.Get(
		"Content-Type",
	); !strings.Contains(
		contentType,
		"text/plain",
	) {
		t.Fatalf("unexpected metrics content type: %s", contentType)
	}
}

func postMCPMessage(t *testing.T, endpoint, message string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(message))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}

	return response
}

type toonCall struct {
	object map[string]any
	text   string
}

func callToolToon(
	t *testing.T,
	ctx context.Context,
	session *mcp.ClientSession,
	name string,
	arguments map[string]any,
	wantError bool,
) toonCall {
	t.Helper()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      name,
		Arguments: arguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError != wantError {
		t.Fatalf(
			"%s IsError = %t, want %t:\n%s",
			name,
			result.IsError,
			wantError,
			toonText(t, result),
		)
	}
	text := toonText(t, result)
	decoded, err := toon.DecodeString(text)
	if err != nil {
		t.Fatalf("decode TOON for %s: %v\n%s", name, err, text)
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded TOON for %s = %[2]T %[2]v, want object", name, decoded)
	}

	return toonCall{object: object, text: text}
}

func toonText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if result.StructuredContent != nil {
		t.Fatalf("structuredContent = %#v, want nil", result.StructuredContent)
	}
	if len(result.Content) != 1 {
		t.Fatalf("content blocks = %d, want 1: %#v", len(result.Content), result.Content)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content block = %T, want TextContent", result.Content[0])
	}
	if json.Valid([]byte(strings.TrimSpace(text.Text))) {
		t.Fatalf("content is JSON, want TOON: %s", text.Text)
	}

	return text.Text
}

func testServices(t *testing.T, text string) *appsvc.Services {
	t.Helper()
	services, _ := testServicesWithDigest(t, text)

	return services
}

func testServicesWithDigest(
	t *testing.T,
	text string,
) (*appsvc.Services, contracts.ObjectDigest) {
	t.Helper()
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	t.Cleanup(filestoreService.Close)
	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	t.Cleanup(queryService.Close)
	digest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader:    strings.NewReader(text),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: appsvc.MailMessageFacet}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testsupport.CleanupQueryObjects(t, digest) })
	manifest, err := filestoreService.Client.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := queryService.Client.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}

	services, err := appsvc.New(appsvc.Options{
		Query:   queryService.Client,
		Objects: filestoreService.Client,
	})
	if err != nil {
		t.Fatal(err)
	}

	return services, digest
}

func testCanonicalMessage() appsvc.CanonicalMessage {
	return appsvc.CanonicalMessage{
		MessageID: "<paudley/coding-ethos/pull/217/review/4369413606@github.com>",
		Digest:    "ffaf47c75c3539857dd3a70f9a21f8f128bd116884207a92338797dd3effc0ef",
		ObjectID:  "gmail:primary:19e67b267f02ded5",
		ThreadID:  "19e67b11bf0ccbf4",
		SelectedHeaders: appsvc.CanonicalSelectedHeaders{
			Date:    "Tue, 26 May 2026 21:30:05 -0700",
			From:    "gemini-code-assist[bot] <notifications@github.com>",
			To:      "paudley/coding-ethos <coding-ethos@noreply.github.com>",
			Subject: "Re: [paudley/coding-ethos] Remove git enforcement optionality (PR #217)",
		},
		Summary: "This update enforces critical Git policies unconditionally while introducing new configuration sections for profiles and repository kinds.",
		Bullets: []string{
			"Invariant Git policies such as protected branches can no longer be disabled via configuration.",
			"Support was added for profiles and repo.kind in repository settings.",
			"The review is complete and no further feedback is needed.",
		},
		Categories: []string{"primary"},
		Graph: appsvc.CanonicalMessageGraph{
			Source: map[string]any{
				"kind":         "pull_request_review",
				"service":      "github",
				"repository":   "paudley/coding-ethos",
				"pull_request": 217,
				"review_id":    "4369413606",
				"actor":        "gemini-code-assist[bot]",
			},
			Topics: []string{
				"git enforcement optionality",
				"invariant Git policies",
				"protected branch work",
				"hook bypass prevention",
				"history rewrite prevention",
				"repo_config.yaml",
				"profiles",
				"repo.kind",
			},
			Actions: []string{
				"removes ability to disable invariant Git policies",
				"removes enabled toggles from configuration",
				"enforces policies unconditionally",
				"adds validation tests for disable attempts",
				"adds profiles configuration support",
				"adds repo.kind configuration support",
			},
			Links: []appsvc.CanonicalMessageLink{
				{
					Rel: "canonical",
					URL: "https://github.com/paudley/coding-ethos/pull/217#pullrequestreview-4369413606",
				},
			},
		},
		Body:        "@" + "gemini-code-assist[bot] commented on this pull request. Code Review This pull request removes the ability for consumer repositories to disable invariant Git policies such as protected branch work, hook bypass prevention, and history rewrite prevention via their repo_config.yaml configuration. It cleans up the corresponding enabled toggles from the configuration files, updates the policy compiler to enforce these policies unconditionally, and adds validation tests to reject any attempts to disable them. Additionally, it introduces support for a profiles section and a repo.kind setting in the repository configuration. There are no review comments to address, and I have no further feedback to provide.",
		Attachments: []appsvc.CanonicalMessageAttachment{},
	}
}
