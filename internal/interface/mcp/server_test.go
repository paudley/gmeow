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
