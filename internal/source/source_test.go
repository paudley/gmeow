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
	"blackcat.ca/gmeow/internal/filestore"
	querymemory "blackcat.ca/gmeow/internal/query/memory"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/scheduler"
)

func TestIngestSourceLookupHitDoesNotReadOrRewritePayload(t *testing.T) {
	ctx := context.Background()
	existing := contracts.ObjectDigest(
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	store := &recordingStore{lookupDigest: existing, lookupFound: true}
	service, err := NewService(store)
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

	if store.putCalls != 0 {
		t.Fatalf("lookup hit transferred payload %d times", store.putCalls)
	}

	if len(store.attached) != 1 {
		t.Fatalf("expected provenance attach on lookup hit, got %#v", store.attached)
	}
}

func TestIngestSourceClaimSerializesHydration(t *testing.T) {
	ctx := context.Background()
	store := &recordingStore{claimAcquired: false}
	service, err := NewService(store)
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

	if store.putCalls != 0 {
		t.Fatalf("blocked claim transferred payload %d times", store.putCalls)
	}
}

func TestGmailMessageCreatesCompoundWithoutRawRFC822Duplicate(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	service, err := NewService(filestoreClient{store: store})
	if err != nil {
		t.Fatal(err)
	}

	adapter, err := NewGmailAdapter("primary", memoryGmailBackend{})
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

	structure, err := store.GetStructure(ctx, digest)
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

	manifest, err := store.ReadManifest(ctx, digest)
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
	existing := contracts.ObjectDigest(
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	store := &recordingStore{lookupDigest: existing, lookupFound: true}
	service, err := NewService(store)
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
	store := filestore.NewFilesystemStore(t.TempDir())
	service, err := NewService(filestoreClient{store: store})
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

	fileDigest, err := store.Put(ctx, filestore.PutRequest{
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
	store := filestore.NewFilesystemStore(t.TempDir())
	service, err := NewService(filestoreClient{store: store})
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

	index := querymemory.New(store)
	if err := index.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	search, err := index.Search(
		ctx,
		contracts.SearchRequest{Query: "Call 1", Facets: []string{"phone"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(search.Results) != 1 || search.Results[0].ObjectDigest != digest {
		t.Fatalf("expected projected push object to be searchable, got %#v", search.Results)
	}

	broker := scheduler.NewMemoryBroker()
	schedulerService, err := scheduler.NewService(
		store,
		broker,
		[]contracts.AnalyzerSpec{analysis.TextExtractAnalyzer{}.Spec()},
		scheduler.Config{},
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := schedulerService.Scan(ctx, contracts.SchedulerScanRequest{
		PriorityClass: contracts.PriorityFreshIngest,
		RequestedBy:   "source-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued == 0 || len(broker.Jobs()) == 0 {
		t.Fatalf(
			"expected push ingest to schedule analysis, response=%#v jobs=%#v",
			response,
			broker.Jobs(),
		)
	}
}

type recordingStore struct {
	lookupDigest  contracts.ObjectDigest
	claim         contracts.SourceIngestClaim
	lookupFound   bool
	claimAcquired bool
	putCalls      int
	attached      []contracts.Provenance
}

func (store *recordingStore) LookupSourceObject(
	context.Context,
	contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	return store.lookupDigest, store.lookupFound, nil
}

func (store *recordingStore) TryAcquireSourceIngest(
	_ context.Context,
	ref contracts.SourceObjectRef,
) (contracts.SourceIngestClaim, bool, error) {
	if !store.claimAcquired {
		return contracts.SourceIngestClaim{}, false, nil
	}

	store.claim = contracts.SourceIngestClaim{SourceObject: ref, ClaimID: "claim-1"}

	return store.claim, true, nil
}

func (store *recordingStore) ReleaseSourceIngest(
	context.Context,
	contracts.SourceIngestClaim,
) error {
	return nil
}

func (store *recordingStore) Put(
	context.Context,
	rpc.PutRequest,
) (contracts.ObjectDigest, error) {
	store.putCalls++

	return contracts.ObjectDigest(
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	), nil
}

func (store *recordingStore) ReadManifest(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Manifest, error) {
	return contracts.Manifest{}, errors.New("not implemented")
}

func (store *recordingStore) AttachProvenance(
	_ context.Context,
	_ contracts.ObjectDigest,
	provenance []contracts.Provenance,
) error {
	store.attached = append(store.attached, provenance...)

	return nil
}

func (store *recordingStore) PutCompound(
	context.Context,
	rpc.CompoundPutRequest,
) (contracts.ObjectDigest, error) {
	return contracts.ObjectDigest(
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	), nil
}

func (store *recordingStore) WriteSourceCursor(
	context.Context,
	contracts.SourceCursor,
) error {
	return nil
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) {
	panic("payload should not be read on source lookup hit")
}

type filestoreClient struct {
	store filestore.Store
}

func (client filestoreClient) LookupSourceObject(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	return client.store.LookupSourceObject(ctx, ref)
}

func (client filestoreClient) TryAcquireSourceIngest(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.SourceIngestClaim, bool, error) {
	return client.store.TryAcquireSourceIngest(ctx, ref)
}

func (client filestoreClient) ReleaseSourceIngest(
	ctx context.Context,
	claim contracts.SourceIngestClaim,
) error {
	return client.store.ReleaseSourceIngest(ctx, claim)
}

func (client filestoreClient) Put(
	ctx context.Context,
	request rpc.PutRequest,
) (contracts.ObjectDigest, error) {
	return client.store.Put(ctx, filestore.PutRequest{
		Reader:        request.Reader,
		MediaType:     request.MediaType,
		SourceHint:    request.SourceHint,
		ContentRoles:  request.ContentRoles,
		Facets:        request.Facets,
		Provenance:    request.Provenance,
		Relationships: request.Relationships,
	})
}

func (client filestoreClient) ReadManifest(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Manifest, error) {
	return client.store.ReadManifest(ctx, digest)
}

func (client filestoreClient) AttachProvenance(
	ctx context.Context,
	digest contracts.ObjectDigest,
	provenance []contracts.Provenance,
) error {
	return client.store.AttachProvenance(ctx, digest, provenance)
}

func (client filestoreClient) PutCompound(
	ctx context.Context,
	request rpc.CompoundPutRequest,
) (contracts.ObjectDigest, error) {
	return client.store.PutCompound(ctx, filestore.CompoundPutRequest{
		ObjectID:      request.ObjectID,
		MediaType:     request.MediaType,
		SourceHint:    request.SourceHint,
		ContentRoles:  request.ContentRoles,
		Facets:        request.Facets,
		Provenance:    request.Provenance,
		Relationships: request.Relationships,
		Parts:         request.Parts,
	})
}

func (client filestoreClient) WriteSourceCursor(
	ctx context.Context,
	cursor contracts.SourceCursor,
) error {
	return client.store.WriteSourceCursor(ctx, cursor)
}

type memoryGmailBackend struct{}

func (memoryGmailBackend) Search(
	context.Context,
	string,
	int,
) ([]GmailSearchHit, error) {
	return nil, nil
}

func (memoryGmailBackend) GetMessage(
	context.Context,
	string,
) (GmailMessage, error) {
	return GmailMessage{}, errors.New("not used")
}

func (memoryGmailBackend) ModifyMessage(
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
