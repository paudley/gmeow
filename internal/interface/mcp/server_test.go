// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mcpiface

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/query/memory"
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

func testServices(t *testing.T, text string) *appsvc.Services {
	t.Helper()
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	index := memory.New(store)
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(text),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: appsvc.MailMessageFacet}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}

	services, err := appsvc.New(appsvc.Options{Query: index, Objects: store})
	if err != nil {
		t.Fatal(err)
	}

	return services
}
