// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"
	"io"
	"os"
	"slices"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestUpdateJMAPEmailStateWritesRecoveryOverlayBeforeQuery(t *testing.T) {
	digest := contracts.ObjectDigest("digest-1")
	recorder := &jmapWriteRecorder{}
	query := &orderedJMAPQuery{
		recorder: recorder,
		states: map[contracts.ObjectDigest]contracts.JMAPEmailState{
			digest: {
				ObjectDigest: digest,
				MailboxIDs:   []string{"all", "inbox"},
				Keywords:     []string{"$seen"},
			},
		},
	}
	objects := &orderedObjectStore{recorder: recorder}
	services, err := New(Options{Query: query, Objects: objects})
	if err != nil {
		t.Fatal(err)
	}

	state, err := services.UpdateJMAPEmailState(context.Background(), JMAPEmailMutation{
		ObjectDigest: digest,
		MailboxIDs: map[string]bool{
			"inbox":       false,
			"mbox-apollo": true,
		},
		Keywords: map[string]bool{
			"$seen":    true,
			"$flagged": true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(recorder.events, []string{"overlay", "query"}) {
		t.Fatalf("write order = %#v, want overlay before query", recorder.events)
	}
	if !slices.Equal(state.MailboxIDs, []string{"all", "mbox-apollo"}) ||
		!slices.Equal(state.Keywords, []string{"$flagged", "$seen"}) {
		t.Fatalf("unexpected state: %#v", state)
	}
	overlay := objects.overlay["jmap"].(map[string]any)
	if !slices.Equal(overlay["mailbox_ids"].([]string), []string{"all", "mbox-apollo"}) ||
		!slices.Equal(overlay["keywords"].([]string), []string{"$flagged", "$seen"}) {
		t.Fatalf("unexpected overlay: %#v", objects.overlay)
	}
}

func TestUpdateJMAPMailboxesWritesRecoveryCursorBeforeQuery(t *testing.T) {
	recorder := &jmapWriteRecorder{}
	query := &orderedJMAPQuery{
		recorder: recorder,
		mailboxes: []contracts.JMAPMailbox{{
			MailboxID: "all",
			Name:      "All Mail",
			IsSystem:  true,
		}},
	}
	objects := &orderedObjectStore{recorder: recorder}
	services, err := New(Options{Query: query, Objects: objects})
	if err != nil {
		t.Fatal(err)
	}

	result, err := services.UpdateJMAPMailboxes(context.Background(), JMAPMailboxMutation{
		Create: map[string]JMAPMailboxCreate{
			"client-1": {Name: "Apollo"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(recorder.events, []string{"source_cursor", "query"}) {
		t.Fatalf("write order = %#v, want source cursor before query", recorder.events)
	}
	if result.Created["client-1"].Name != "Apollo" {
		t.Fatalf("unexpected result: %#v", result)
	}
	if objects.cursor.SourceKind != contracts.JMAPMailboxCatalogSourceKind ||
		objects.cursor.SourceName != contracts.JMAPMailboxCatalogSourceName {
		t.Fatalf("unexpected source cursor: %#v", objects.cursor)
	}
}

type jmapWriteRecorder struct {
	events []string
}

type orderedJMAPQuery struct {
	recorder  *jmapWriteRecorder
	states    map[contracts.ObjectDigest]contracts.JMAPEmailState
	mailboxes []contracts.JMAPMailbox
}

func (query *orderedJMAPQuery) Search(
	context.Context,
	contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	return contracts.SearchResponse{}, nil
}

func (query *orderedJMAPQuery) Structure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (query *orderedJMAPQuery) Relationships(
	context.Context,
	contracts.RelationshipRequest,
) (contracts.RelationshipResponse, error) {
	return contracts.RelationshipResponse{}, nil
}

func (query *orderedJMAPQuery) Graph(
	context.Context,
	contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	return contracts.GraphResponse{}, nil
}

func (query *orderedJMAPQuery) AnalysisStatus(
	context.Context,
	contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	return contracts.AnalysisStatusResponse{}, nil
}

func (query *orderedJMAPQuery) RelatedObjects(
	context.Context,
	contracts.RelatedObjectsRequest,
) (contracts.RelatedObjectsResponse, error) {
	return contracts.RelatedObjectsResponse{}, nil
}

func (query *orderedJMAPQuery) SourceCursors(
	context.Context,
	contracts.SourceCursorRequest,
) (contracts.SourceCursorResponse, error) {
	return contracts.SourceCursorResponse{}, nil
}

func (query *orderedJMAPQuery) JMAPMailboxes(
	context.Context,
) ([]contracts.JMAPMailbox, error) {
	return query.mailboxes, nil
}

func (query *orderedJMAPQuery) JMAPEmailStates(
	context.Context,
	[]contracts.ObjectDigest,
) (map[contracts.ObjectDigest]contracts.JMAPEmailState, error) {
	return query.states, nil
}

func (query *orderedJMAPQuery) JMAPEmailQuery(
	context.Context,
	contracts.JMAPEmailQueryRequest,
) (contracts.JMAPEmailQueryResponse, error) {
	return contracts.JMAPEmailQueryResponse{}, nil
}

func (query *orderedJMAPQuery) JMAPThreads(
	context.Context,
	[]string,
) (map[string]contracts.JMAPThread, error) {
	return map[string]contracts.JMAPThread{}, nil
}

func (query *orderedJMAPQuery) JMAPBlobLookup(
	context.Context,
	contracts.JMAPBlobLookupRequest,
) (contracts.JMAPBlobLookupResponse, error) {
	return contracts.JMAPBlobLookupResponse{}, nil
}

func (query *orderedJMAPQuery) UpdateJMAPMailboxCatalog(
	context.Context,
	contracts.JMAPMailboxCatalogUpdate,
) ([]contracts.JMAPMailbox, error) {
	query.recorder.events = append(query.recorder.events, "query")
	return query.mailboxes, nil
}

func (query *orderedJMAPQuery) JMAPMailboxEmailCounts(
	context.Context,
	contracts.JMAPMailboxEmailCountRequest,
) (contracts.JMAPMailboxEmailCountResponse, error) {
	return contracts.JMAPMailboxEmailCountResponse{Counts: map[string]int{}}, nil
}

func (query *orderedJMAPQuery) UpdateJMAPEmailState(
	_ context.Context,
	update contracts.JMAPEmailStateUpdate,
) (contracts.JMAPEmailState, error) {
	query.recorder.events = append(query.recorder.events, "query")
	return contracts.JMAPEmailState{
		ObjectDigest: update.ObjectDigest,
		MailboxIDs:   append([]string{}, update.MailboxIDs...),
		Keywords:     append([]string{}, update.Keywords...),
	}, nil
}

type orderedObjectStore struct {
	recorder *jmapWriteRecorder
	overlay  map[string]any
	cursor   contracts.SourceCursor
}

func (objects *orderedObjectStore) ReadManifest(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Manifest, error) {
	return contracts.Manifest{}, os.ErrNotExist
}

func (objects *orderedObjectStore) GetStructure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (objects *orderedObjectStore) Open(
	context.Context,
	contracts.ObjectDigest,
) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}

func (objects *orderedObjectStore) WriteOverlays(
	_ context.Context,
	_ contracts.ObjectDigest,
	overlays map[string]any,
) error {
	objects.recorder.events = append(objects.recorder.events, "overlay")
	objects.overlay = overlays
	return nil
}

func (objects *orderedObjectStore) WriteSourceCursor(
	_ context.Context,
	cursor contracts.SourceCursor,
) error {
	objects.recorder.events = append(objects.recorder.events, "source_cursor")
	objects.cursor = cursor
	return nil
}
