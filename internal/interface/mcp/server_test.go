// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mcpiface

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestMCPMailSearchUsesAppServices(t *testing.T) {
	ctx := context.Background()
	services := testServices(t, "hello mcp mail")
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

	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "mail_search",
		Arguments: map[string]any{"query": "mcp", "limit": 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("tool returned error: %#v", result.Content)
	}
	if result.StructuredContent == nil {
		t.Fatalf("expected structured content: %#v", result)
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
	if result.StructuredContent == nil {
		t.Fatalf("expected structured content: %#v", result)
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

func testServices(t *testing.T, text string) *appsvc.Services {
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

	return services
}
