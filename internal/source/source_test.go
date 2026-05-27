// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/analysis"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestIngestSourceLookupHitDoesNotReadOrRewritePayload(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	existing, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader: strings.NewReader("known payload"),
		Facets: []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind:      "gmail",
			SourceName:      "primary",
			ExternalID:      "message-1",
			ExternalVersion: "v1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}

	digest, wrote, err := service.Ingest(ctx, IngestObject{
		Reader:      panicReader{},
		SourceKind:  "gmail",
		SourceName:  "primary",
		ExternalID:  "message-1",
		ExternalVer: "v1",
		Facets:      []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if digest != existing || wrote {
		t.Fatalf("expected lookup digest without write, digest=%s wrote=%t", digest, wrote)
	}
}

func TestIngestSourceClaimSerializesHydration(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "v1",
	}
	claim, acquired, err := filestoreService.Client.TryAcquireSourceIngest(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("expected test setup to acquire source ingest claim")
	}
	defer filestoreService.Client.ReleaseSourceIngest(ctx, claim)
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = service.Ingest(ctx, IngestObject{
		Reader:      strings.NewReader("hello"),
		SourceKind:  "gmail",
		SourceName:  "primary",
		ExternalID:  "message-1",
		ExternalVer: "v1",
		Facets:      []contracts.Facet{{Kind: "file"}},
	})
	if err == nil {
		t.Fatal("expected concurrent source ingest claim to block hydration")
	}
	if !errors.Is(err, ErrSourceIngestInProgress) {
		t.Fatalf("expected source ingest claim error, got %v", err)
	}
}

func TestBackfillSkipsConcurrentSourceIngestClaim(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}

	report, err := service.RunBackfill(ctx, duplicatePullAdapter{}, BackfillRequest{
		Cursor:      map[string]any{},
		PageSize:    2,
		Concurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Processed != 2 || report.Created != 1 || report.Skipped != 1 ||
		report.Failed != 0 {
		t.Fatalf("unexpected duplicate ingest report: %#v", report)
	}
}

func TestGmailMessageCreatesCompoundWithoutRawRFC822Duplicate(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}

	adapter, err := NewGmailAdapter("primary", gmailExternalBackend{})
	if err != nil {
		t.Fatal(err)
	}

	message := GmailMessage{
		MessageID: "m1",
		Version:   "h1",
		ThreadID:  "t1",
		Subject:   "Subject",
		Headers: map[string]string{
			"subject":    "Subject",
			"message-id": "<m1@example.test>",
		},
		Metadata: map[string]any{
			"label_ids": []string{"INBOX", "UNREAD"},
		},
		Body: []byte("hello body"),
		Attachments: []GmailAttachment{{
			ID:        "a1",
			FileName:  "same.txt",
			MediaType: "text/plain",
			Content:   []byte("shared attachment"),
		}},
	}
	digest, wrote, err := adapter.IngestMessage(ctx, service, message)
	if err != nil {
		t.Fatal(err)
	}
	if !wrote {
		t.Fatal("expected first Gmail hydrate to write")
	}

	structure, err := filestoreService.Client.GetStructure(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}

	forbidden := map[string]bool{"raw_rfc822": true, "body/raw/attachment": true}
	for role := range structure.PartsByRole {
		if forbidden[role] {
			t.Fatalf("gmail compound has forbidden duplicate raw role %q", role)
		}
	}

	for _, role := range []string{
		"rfc822_headers",
		"email_body",
		"gmail_data",
		"mime_structure",
		"attachment",
	} {
		if len(structure.PartsByRole[role]) == 0 {
			t.Fatalf("expected Gmail compound role %q in %#v", role, structure.PartsByRole)
		}
	}

	manifest, err := filestoreService.Client.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	metadata := mailMessageMetadataForTest(t, manifest)
	if metadata["rfc_message_id"] != "<m1@example.test>" {
		t.Fatalf("expected RFC Message-ID projection, got %#v", metadata)
	}
	if labels := stringSliceValue(metadata["label_ids"]); len(labels) != 2 ||
		labels[0] != "INBOX" || labels[1] != "UNREAD" {
		t.Fatalf("expected Gmail label IDs in mail facet metadata, got %#v", metadata)
	}
	if !hasRelationship(
		manifest.Relationships,
		"contains",
		digest,
		structure.PartsByRole["attachment"][0].Digest,
		"attachment",
	) {
		t.Fatalf(
			"compound manifest missing contains relationship: %#v",
			manifest.Relationships,
		)
	}
	if !hasRelationship(
		manifest.Relationships,
		"part_of",
		structure.PartsByRole["attachment"][0].Digest,
		digest,
		"attachment",
	) {
		t.Fatalf(
			"compound manifest missing part_of relationship: %#v",
			manifest.Relationships,
		)
	}
}

func mailMessageMetadataForTest(
	t *testing.T,
	manifest contracts.Manifest,
) map[string]any {
	t.Helper()
	for _, facet := range manifest.Facets {
		if facet.Kind == "mail_message" {
			return facet.Metadata
		}
	}

	t.Fatalf("manifest missing mail_message facet: %#v", manifest.Facets)
	return nil
}

func TestDriveAdapterFailsUnsupportedOperationsWithActionableError(t *testing.T) {
	adapter, err := NewDriveAdapter("work")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = adapter.Pull(context.Background(), nil, PullRequest{})
	if !errors.Is(err, ErrUnsupportedOperation) ||
		!strings.Contains(err.Error(), "design-only") {
		t.Fatalf("expected design-only unsupported error, got %v", err)
	}
}

func TestGmailSearchAndHydrateReusesExistingDigestWithoutFetchingPayload(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	existing, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader: strings.NewReader("known gmail payload"),
		Facets: []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind:      "gmail",
			SourceName:      "primary",
			ExternalID:      "m1",
			ExternalVersion: "v1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}

	adapter, err := NewGmailAdapter("primary", &searchOnlyGmailBackend{})
	if err != nil {
		t.Fatal(err)
	}

	results, err := adapter.SearchAndHydrate(
		ctx,
		service,
		LiveSearchRequest{Query: "bob", Limit: 1},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 || results[0].ObjectDigest != existing || results[0].Hydrated {
		t.Fatalf("expected existing FILESTORE digest without hydration: %#v", results)
	}
}

func TestGmailBackfillPagesIntoFilestoreAndRerunDedupes(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewGmailAdapter("primary", &backfillGmailBackend{})
	if err != nil {
		t.Fatal(err)
	}

	report, err := service.RunBackfill(ctx, adapter, BackfillRequest{
		Cursor:   map[string]any{"mode": "full", "query": "newer_than:30d"},
		PageSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Completed || report.Processed != 2 || report.Created != 2 {
		t.Fatalf("unexpected first backfill report: %#v", report)
	}
	cursor, found, err := service.ReadCursor(ctx, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if !found || cursor.Cursor["completed"] != true {
		t.Fatalf("expected persisted completed cursor, found=%t cursor=%#v", found, cursor)
	}

	digest, found, err := service.LookupSourceObject(ctx, contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "m1",
		ExternalVersion: "h1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected backfilled Gmail source object")
	}
	structure, err := filestoreService.Client.GetStructure(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(structure.PartsByRole["email_body"]) == 0 {
		t.Fatalf("backfill must create full Gmail compound parts: %#v", structure)
	}

	rerun, err := service.RunBackfill(ctx, adapter, BackfillRequest{
		Cursor:   map[string]any{"mode": "full", "query": "newer_than:30d"},
		PageSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rerun.Created != 0 || rerun.Skipped != 2 {
		t.Fatalf("expected idempotent rerun, got %#v", rerun)
	}
}

func TestBackfillCursorNamespaceWritesMergeLatestCursor(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewGmailAdapter("primary", &backfillGmailBackend{})
	if err != nil {
		t.Fatal(err)
	}
	stale := contracts.SourceCursor{
		SourceKind: "gmail",
		SourceName: "primary",
		Cursor:     map[string]any{},
	}

	err = service.writeBackfillCursor(
		ctx,
		adapter,
		stale,
		contracts.SourceCursor{
			SourceKind: "gmail",
			SourceName: "primary",
			Cursor:     map[string]any{"page_token": "backfill-page"},
		},
		"backfill",
	)
	if err != nil {
		t.Fatal(err)
	}
	err = service.writeBackfillCursor(
		ctx,
		adapter,
		stale,
		contracts.SourceCursor{
			SourceKind: "gmail",
			SourceName: "primary",
			Cursor:     map[string]any{"page_token": "inbox-page"},
		},
		"inbox_refresh",
	)
	if err != nil {
		t.Fatal(err)
	}

	cursor, found, err := service.ReadCursor(ctx, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if !found ||
		mapCursorValue(cursor.Cursor, "backfill")["page_token"] != "backfill-page" ||
		mapCursorValue(cursor.Cursor, "inbox_refresh")["page_token"] != "inbox-page" {
		t.Fatalf("expected merged cursor namespaces, found=%t cursor=%#v", found, cursor)
	}
}

func TestGmailHistoryExpiredCursorFailsClosed(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewGmailAdapter("primary", &expiredHistoryGmailBackend{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.RunBackfill(ctx, adapter, BackfillRequest{
		Cursor:   map[string]any{"mode": "history", "history_anchor": "10"},
		PageSize: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "history cursor expired") {
		t.Fatalf("expected expired history cursor error, got %v", err)
	}
	cursor, found, err := service.ReadCursor(ctx, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if !found || cursor.Cursor["history_expired"] != true {
		t.Fatalf("expected persisted expired cursor, found=%t cursor=%#v", found, cursor)
	}
}

func TestGmailActionRequiresMatchingProvenanceAndMailFacet(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}

	backend := &actionGmailBackend{}
	adapter, err := NewGmailAdapter("primary", backend)
	if err != nil {
		t.Fatal(err)
	}

	digest, _, err := adapter.IngestMessage(ctx, service, GmailMessage{
		MessageID: "m-action",
		Version:   "v1",
		Subject:   "Action",
		Headers:   map[string]string{"subject": "Action"},
		Body:      []byte("body"),
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := service.ApplyAction(ctx, adapter, ActionRequest{
		ObjectDigest: digest,
		Action:       "archive",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied || backend.messageID != "m-action" {
		t.Fatalf(
			"expected provenance-gated Gmail action, result=%#v message=%q",
			result,
			backend.messageID,
		)
	}

	fileDigest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader:     strings.NewReader("not mail"),
		MediaType:  "text/plain",
		SourceHint: "not-mail",
		Facets:     []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{
			{SourceKind: "gmail", SourceName: "primary", ExternalID: "file-1"},
		},
		ContentRoles: []string{"source"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ApplyAction(ctx, adapter, ActionRequest{
		ObjectDigest: fileDigest,
		Action:       "archive",
	})
	if !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("expected facet-gated action rejection, got %v", err)
	}
}

func TestPushIngestProjectsAndSchedulesAnalysis(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}

	ingestor, err := NewPushIngestor("ringme-fixture", "ringme")
	if err != nil {
		t.Fatal(err)
	}
	digest, wrote, err := ingestor.Ingest(ctx, service, PushRecord{
		Payload:     []byte("call note with invoice"),
		MediaType:   "text/plain",
		ExternalID:  "call-1",
		ExternalVer: "v1",
		DisplayName: "Call 1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !wrote {
		t.Fatal("expected push ingest to write new FILESTORE object")
	}

	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	defer queryService.Close()
	if err := queryService.Client.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	search, err := queryService.Client.Search(
		ctx,
		contracts.SearchRequest{
			Facets: []string{"phone"},
			Provenance: contracts.ProvenanceFilter{
				SourceNames: []string{"ringme-fixture"},
				ExternalIDs: []string{"call-1"},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(search.Results) != 1 || search.Results[0].ObjectDigest != digest {
		t.Fatalf("expected projected push object to be searchable, got %#v", search.Results)
	}

	schedulerService := testsupport.StartSchedulerGRPC(
		t,
		ctx,
		filestoreService.Store,
		[]contracts.AnalyzerSpec{analysis.TextExtractAnalyzer{}.Spec()},
	)
	defer schedulerService.Close()
	response, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{
		PriorityClass: contracts.PriorityFreshIngest,
		RequestedBy:   "source-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued == 0 || status.Pending == 0 {
		t.Fatalf(
			"expected push ingest to schedule analysis, response=%#v status=%#v",
			response,
			status,
		)
	}
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) {
	panic("payload should not be read on source lookup hit")
}

type gmailExternalBackend struct{}

func (gmailExternalBackend) Search(
	context.Context,
	string,
	int,
) ([]GmailSearchHit, error) {
	return nil, nil
}

func (gmailExternalBackend) ListMessages(
	context.Context,
	GmailListRequest,
) (GmailListPage, error) {
	return GmailListPage{}, nil
}

func (gmailExternalBackend) ListHistory(
	context.Context,
	GmailHistoryRequest,
) (GmailHistoryPage, error) {
	return GmailHistoryPage{}, nil
}

func (gmailExternalBackend) GetMessage(
	context.Context,
	string,
) (GmailMessage, error) {
	return GmailMessage{}, errors.New("not used")
}

func (gmailExternalBackend) ModifyMessage(
	context.Context,
	string,
	string,
	map[string]any,
) (map[string]any, error) {
	return map[string]any{}, nil
}

type searchOnlyGmailBackend struct{}

func (*searchOnlyGmailBackend) Search(
	context.Context,
	string,
	int,
) ([]GmailSearchHit, error) {
	return []GmailSearchHit{{MessageID: "m1", Version: "v1"}}, nil
}

func (*searchOnlyGmailBackend) ListMessages(
	context.Context,
	GmailListRequest,
) (GmailListPage, error) {
	return GmailListPage{}, errors.New("not used")
}

func (*searchOnlyGmailBackend) ListHistory(
	context.Context,
	GmailHistoryRequest,
) (GmailHistoryPage, error) {
	return GmailHistoryPage{}, errors.New("not used")
}

func (*searchOnlyGmailBackend) GetMessage(
	context.Context,
	string,
) (GmailMessage, error) {
	return GmailMessage{}, errors.New(
		"payload should not be fetched for existing FILESTORE hit",
	)
}

func (*searchOnlyGmailBackend) ModifyMessage(
	context.Context,
	string,
	string,
	map[string]any,
) (map[string]any, error) {
	return nil, errors.New("not used")
}

type actionGmailBackend struct {
	messageID string
}

func (*actionGmailBackend) Search(
	context.Context,
	string,
	int,
) ([]GmailSearchHit, error) {
	return nil, nil
}

func (*actionGmailBackend) ListMessages(
	context.Context,
	GmailListRequest,
) (GmailListPage, error) {
	return GmailListPage{}, errors.New("not used")
}

func (*actionGmailBackend) ListHistory(
	context.Context,
	GmailHistoryRequest,
) (GmailHistoryPage, error) {
	return GmailHistoryPage{}, errors.New("not used")
}

func (*actionGmailBackend) GetMessage(
	context.Context,
	string,
) (GmailMessage, error) {
	return GmailMessage{}, errors.New("not used")
}

func (backend *actionGmailBackend) ModifyMessage(
	_ context.Context,
	messageID string,
	action string,
	_ map[string]any,
) (map[string]any, error) {
	backend.messageID = messageID

	return map[string]any{"action": action}, nil
}

type backfillGmailBackend struct{}

func (*backfillGmailBackend) Search(
	context.Context,
	string,
	int,
) ([]GmailSearchHit, error) {
	return nil, errors.New("not used")
}

func (*backfillGmailBackend) ListMessages(
	_ context.Context,
	request GmailListRequest,
) (GmailListPage, error) {
	switch request.PageToken {
	case "":
		return GmailListPage{
			Hits:          []GmailSearchHit{{MessageID: "m1", Version: "h1"}},
			NextPageToken: "page-2",
		}, nil
	case "page-2":
		return GmailListPage{
			Hits: []GmailSearchHit{{MessageID: "m2", Version: "h2"}},
		}, nil
	default:
		return GmailListPage{}, nil
	}
}

func (*backfillGmailBackend) ListHistory(
	context.Context,
	GmailHistoryRequest,
) (GmailHistoryPage, error) {
	return GmailHistoryPage{}, errors.New("not used")
}

func (*backfillGmailBackend) GetMessage(
	_ context.Context,
	messageID string,
) (GmailMessage, error) {
	version := "h1"
	if messageID == "m2" {
		version = "h2"
	}

	return GmailMessage{
		MessageID: messageID,
		Version:   version,
		ThreadID:  "thread-" + messageID,
		Subject:   "Subject " + messageID,
		Headers: map[string]string{
			"subject": "Subject " + messageID,
		},
		Body: []byte("body " + messageID),
	}, nil
}

func (*backfillGmailBackend) ModifyMessage(
	context.Context,
	string,
	string,
	map[string]any,
) (map[string]any, error) {
	return nil, errors.New("not used")
}

type expiredHistoryGmailBackend struct {
	backfillGmailBackend
}

func (*expiredHistoryGmailBackend) ListHistory(
	context.Context,
	GmailHistoryRequest,
) (GmailHistoryPage, error) {
	return GmailHistoryPage{Expired: true}, nil
}

type duplicatePullAdapter struct{}

func (duplicatePullAdapter) Name() string {
	return "primary"
}

func (duplicatePullAdapter) Kind() string {
	return "gmail"
}

func (duplicatePullAdapter) Capabilities() []string {
	return []string{CapabilityBackfill}
}

func (duplicatePullAdapter) Pull(
	_ context.Context,
	_ IngestService,
	request PullRequest,
) ([]IngestObject, contracts.SourceCursor, error) {
	cursor := cloneCursor(request.Cursor)
	cursor["completed"] = true

	return []IngestObject{
			{
				Reader:      strings.NewReader("hello"),
				SourceKind:  "gmail",
				SourceName:  "primary",
				ExternalID:  "message-1",
				ExternalVer: "v1",
				Facets:      []contracts.Facet{{Kind: "file"}},
			},
			{
				Reader:      strings.NewReader("hello"),
				SourceKind:  "gmail",
				SourceName:  "primary",
				ExternalID:  "message-1",
				ExternalVer: "v1",
				Facets:      []contracts.Facet{{Kind: "file"}},
			},
		},
		contracts.SourceCursor{
			SourceKind: "gmail",
			SourceName: "primary",
			Cursor:     cursor,
		}, nil
}

func hasRelationship(
	relationships []contracts.Relationship,
	kind string,
	from contracts.ObjectDigest,
	to contracts.ObjectDigest,
	role string,
) bool {
	for _, relationship := range relationships {
		if relationship.Type == kind &&
			relationship.From == from &&
			relationship.To == to &&
			relationship.Role == role {
			return true
		}
	}

	return false
}

var _ io.Reader = panicReader{}
