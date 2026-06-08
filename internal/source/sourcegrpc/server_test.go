// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package sourcegrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/source"
	"blackcat.ca/gmeow/internal/source/sourcegrpc"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestSourceGRPCSearchAndHydrateWritesThroughFilestore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	sourceService, err := source.NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := source.NewGmailAdapter("primary", grpcGmailBackend{
		hits: []source.GmailSearchHit{{MessageID: "fresh", Version: "h1"}},
		messages: map[string]source.GmailMessage{
			"fresh": {
				MessageID:    "fresh",
				Version:      "h1",
				Subject:      "fresh subject",
				Body:         []byte("fresh body"),
				BodyMediaTyp: "text/plain",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := sourcegrpc.NewServer(adapter, sourceService)
	if err != nil {
		t.Fatal(err)
	}

	endpoint := testsupport.UnixEndpoint(t, "source.sock")
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- rpc.Serve(ctx, endpoint, func(grpcServer *grpc.Server) {
			pb.RegisterSourceServiceServer(grpcServer, server)
		})
	}()
	t.Cleanup(func() {
		cancel()
		err := <-serveErr
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("source grpc serve: %v", err)
		}
	})
	time.Sleep(50 * time.Millisecond)

	client, err := sourcegrpc.NewClient(
		ctx,
		endpoint,
		"gmail",
		"primary",
		[]string{source.CapabilityLiveSearch, source.CapabilityBackfill},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	hits, err := client.SearchAndHydrate(
		ctx,
		nil,
		source.LiveSearchRequest{Query: "fresh", Limit: 10},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || !hits[0].Hydrated || hits[0].ObjectDigest == "" {
		t.Fatalf("expected hydrated source hit, got %#v", hits)
	}
	_, found, err := filestoreService.Client.LookupSourceObject(
		ctx,
		contracts.SourceObjectRef{
			SourceKind:      "gmail",
			SourceName:      "primary",
			ExternalID:      "fresh",
			ExternalVersion: "h1",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected source grpc hydration to write FILESTORE source identity")
	}
}

type grpcGmailBackend struct {
	hits     []source.GmailSearchHit
	messages map[string]source.GmailMessage
}

func (backend grpcGmailBackend) Search(
	context.Context,
	string,
	int,
) ([]source.GmailSearchHit, error) {
	return append([]source.GmailSearchHit{}, backend.hits...), nil
}

func (backend grpcGmailBackend) ListMessages(
	context.Context,
	source.GmailListRequest,
) (source.GmailListPage, error) {
	return source.GmailListPage{
		Hits: append([]source.GmailSearchHit{}, backend.hits...),
	}, nil
}

func (backend grpcGmailBackend) ListHistory(
	context.Context,
	source.GmailHistoryRequest,
) (source.GmailHistoryPage, error) {
	return source.GmailHistoryPage{}, nil
}

func (backend grpcGmailBackend) GetMessage(
	_ context.Context,
	messageID string,
) (source.GmailMessage, error) {
	return backend.messages[messageID], nil
}

func (backend grpcGmailBackend) ModifyMessage(
	context.Context,
	string,
	string,
	map[string]any,
) (map[string]any, error) {
	return map[string]any{"ok": true}, nil
}
