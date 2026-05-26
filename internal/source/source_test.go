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

func TestDriveAdapterFailsUnsupportedOperationsWithActionableError(t *testing.T) {
	adapter, err := NewDriveAdapter("work")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = adapter.Pull(context.Background(), PullRequest{})
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
