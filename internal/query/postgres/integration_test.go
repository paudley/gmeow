// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/contactentity"
	"blackcat.ca/gmeow/internal/filestore"
)

const (
	testPostgresDSNEnv     = "GMEOW_TEST_POSTGRES_DSN"
	testQueryMigrationsEnv = "GMEOW_TEST_QUERY_MIGRATIONS_DIR"
)

func TestPostgresProjectsFilestoreWithSourceCursorRebuild(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })
	schema := createQueryTestSchema(t, ctx, dsn)
	t.Cleanup(func() { dropQueryTestSchema(t, dsn, schema) })

	sourceName := "integration-" + randomHex(t, 8)
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello apollo"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: sourceName,
			ExternalID: "apollo-1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteSourceCursor(ctx, contracts.SourceCursor{
		SourceKind: contracts.JMAPMailboxCatalogSourceKind,
		SourceName: contracts.JMAPMailboxCatalogSourceName,
		Cursor: map[string]any{
			contracts.JMAPMailboxCatalogCursorKey: []contracts.JMAPMailbox{{
				MailboxID: "mbox-apollo",
				Name:      "Apollo",
				SortOrder: 80,
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	config := Config{
		ConnString:     dsnWithSearchPath(dsn, schema),
		MigrationsDir:  migrationsDir,
		MigrationTable: schema + ".goose_db_version",
	}
	if err := Migrate(ctx, config); err != nil {
		t.Fatal(err)
	}
	index, err := New(ctx, config, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(index.Close)

	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}
	results, err := index.Search(ctx, contracts.SearchRequest{
		Query:  "apollo",
		Facets: []string{"file"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results.Total != 1 || results.Results[0].ObjectDigest != digest {
		t.Fatalf("unexpected search response: %#v", results)
	}

	report, err := index.RebuildReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Projected != 1 || report.Failed != 0 {
		t.Fatalf("unexpected rebuild report: %#v", report)
	}
	mailboxes, err := index.JMAPMailboxes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMailbox(mailboxes, "mbox-apollo") {
		t.Fatalf("rebuild did not project JMAP mailbox catalog: %#v", mailboxes)
	}
}

func TestJMAPMutableMailStateRecoversFromFilestoreRebuild(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })

	store := filestore.NewFilesystemStore(t.TempDir())
	messageDate := "Wed, 27 May 2026 09:15:00 -0600"
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello apollo"),
		MediaType: "message/rfc822",
		Facets: []contracts.Facet{{
			Kind: contracts.MailMessageFacetKind,
			Metadata: map[string]any{
				"rfc_message_id": "<apollo@example.test>",
				"thread_id":      "thread-apollo",
				"subject":        "Apollo update",
				"date":           messageDate,
				"label_ids":      []string{"INBOX", "UNREAD"},
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "jmap",
			ExternalID: "apollo-1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	firstIndex := newMigratedTestIndex(t, ctx, dsn, migrationsDir, store)
	projectStoredObject(t, ctx, firstIndex, store, digest)
	services, err := appsvc.New(appsvc.Options{
		Query:   firstIndex,
		Objects: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	mailboxResult, err := services.UpdateJMAPMailboxes(ctx, appsvc.JMAPMailboxMutation{
		Create: map[string]appsvc.JMAPMailboxCreate{
			"client-1": {Name: "Apollo"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	customMailbox := mailboxResult.Created["client-1"].MailboxID
	if customMailbox == "" {
		t.Fatalf("missing custom mailbox in result: %#v", mailboxResult)
	}
	if _, err := services.UpdateJMAPEmailState(ctx, appsvc.JMAPEmailMutation{
		ObjectDigest: digest,
		MailboxIDs: map[string]bool{
			"inbox":       false,
			customMailbox: true,
		},
		Keywords: map[string]bool{
			"$seen":    true,
			"$flagged": true,
		},
	}); err != nil {
		t.Fatal(err)
	}

	secondIndex := newMigratedTestIndex(t, ctx, dsn, migrationsDir, store)
	if err := secondIndex.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	mailboxes, err := secondIndex.JMAPMailboxes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMailbox(mailboxes, customMailbox) {
		t.Fatalf("rebuild did not recover custom mailbox %q: %#v", customMailbox, mailboxes)
	}
	states, err := secondIndex.JMAPEmailStates(ctx, []contracts.ObjectDigest{digest})
	if err != nil {
		t.Fatal(err)
	}
	state, ok := states[digest]
	if !ok {
		t.Fatalf("rebuild did not recover JMAP email state for %s", digest)
	}
	if !sameStrings(state.MailboxIDs, []string{"all", customMailbox}) ||
		!sameStrings(state.Keywords, []string{"$flagged", "$seen"}) {
		t.Fatalf("unexpected recovered JMAP state: %#v", state)
	}

	query, err := secondIndex.JMAPEmailQuery(ctx, contracts.JMAPEmailQueryRequest{
		After: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sameDigests(query.IDs, []contracts.ObjectDigest{digest}) {
		t.Fatalf("date-filtered JMAP query did not return recovered message: %#v", query)
	}
	exactAfter, ok := parseJMAPTime(messageDate)
	if !ok {
		t.Fatalf("test message date did not parse: %q", messageDate)
	}
	query, err = secondIndex.JMAPEmailQuery(ctx, contracts.JMAPEmailQueryRequest{
		After: exactAfter,
		Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sameDigests(query.IDs, []contracts.ObjectDigest{digest}) {
		t.Fatalf("inclusive after filter did not return boundary message: %#v", query)
	}
}

func TestMailMessageMetadataProjectsIntoQuery(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })

	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello apollo"),
		MediaType: "message/rfc822",
		Facets: []contracts.Facet{{
			Kind: contracts.MailMessageFacetKind,
			Metadata: map[string]any{
				"rfc_message_id":      "<apollo@example.test>",
				"gmail_message_id":    "gmail-apollo",
				"thread_id":           "thread-apollo",
				"subject":             "Apollo update",
				"date":                "Wed, 27 May 2026 09:15:00 -0600",
				"received_at":         "2023-11-14T22:13:20Z",
				"label_ids":           []string{"INBOX", "STARRED"},
				"history_id":          uint64(12345),
				"internal_date":       int64(1700000000000),
				"size_estimate":       int64(2048),
				"archive_mailbox":     "cur",
				"archive_source_path": "/mail/archive/cur/message.eml",
				"classification_label_values": []map[string]any{{
					"label_id": "smart-label",
					"fields": []map[string]any{{
						"field_id":  "category",
						"selection": "important",
					}},
				}},
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "mail-message",
			ExternalID: "apollo",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	index := newMigratedTestIndex(t, ctx, dsn, migrationsDir, store)
	projectStoredObject(t, ctx, index, store, digest)

	var metadataJSON []byte
	if err := index.pool.QueryRow(ctx, `
		SELECT metadata_json
		FROM query_object_facets
		WHERE object_digest = $1 AND kind = $2`,
		digest,
		contracts.MailMessageFacetKind,
	).Scan(&metadataJSON); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"rfc_message_id",
		"gmail_message_id",
		"thread_id",
		"subject",
		"date",
		"received_at",
		"history_id",
		"internal_date",
		"size_estimate",
		"archive_mailbox",
		"archive_source_path",
		"classification_label_values",
	} {
		if _, ok := metadata[key]; !ok {
			t.Fatalf("projected mail-message metadata missing %q: %#v", key, metadata)
		}
	}

	states, err := index.JMAPEmailStates(ctx, []contracts.ObjectDigest{digest})
	if err != nil {
		t.Fatal(err)
	}
	state, ok := states[digest]
	if !ok ||
		!sameStrings(state.MailboxIDs, []string{"all", "inbox"}) ||
		!sameStrings(state.Keywords, []string{"$flagged", "$seen"}) {
		t.Fatalf("unexpected projected mail-message labels: %#v", states)
	}
	if state.ThreadID != "thread-apollo" ||
		!state.ReceivedAt.Equal(time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)) {
		t.Fatalf("unexpected projected mail-message state: %#v", state)
	}
}

func TestMailArchiveMissingGmailReport(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })
	schema := createQueryTestSchema(t, ctx, dsn)
	t.Cleanup(func() { dropQueryTestSchema(t, dsn, schema) })

	store := filestore.NewFilesystemStore(t.TempDir())
	archiveOnly := putMailIdentityProjectionFixture(
		t,
		ctx,
		store,
		"<archive-only@example.test>",
		contracts.MailArchiveSourceKind,
		"deep",
		false,
	)
	membershipOnly := putMailMembershipProjectionFixture(
		t,
		ctx,
		store,
		"<archive-membership@example.test>",
		"deep",
	)
	_ = putMailIdentityProjectionFixture(
		t,
		ctx,
		store,
		"<in-gmail@example.test>",
		contracts.MailArchiveSourceKind,
		"deep",
		false,
	)
	_ = putMailIdentityProjectionFixture(
		t,
		ctx,
		store,
		"<in-gmail@example.test>",
		"gmail",
		"primary",
		false,
	)
	generated := putMailIdentityProjectionFixture(
		t,
		ctx,
		store,
		"<gmeow-generated-test@gmeow.local>",
		contracts.MailArchiveSourceKind,
		"deep",
		true,
	)

	config := Config{
		ConnString:     dsnWithSearchPath(dsn, schema),
		MigrationsDir:  migrationsDir,
		MigrationTable: schema + ".goose_db_version",
	}
	if err := Migrate(ctx, config); err != nil {
		t.Fatal(err)
	}
	index, err := New(ctx, config, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(index.Close)
	if err := index.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	report, err := index.MailArchiveMissingGmail(ctx, contracts.MailIdentityReportRequest{
		SourceNames: []string{"deep"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Total != 2 ||
		!containsDigest(report.Items, archiveOnly) ||
		!containsDigest(report.Items, membershipOnly) {
		t.Fatalf("unexpected missing Gmail report: %#v", report)
	}
	archiveItem, ok := itemForDigest(report.Items, archiveOnly)
	if !ok ||
		archiveItem.VersionCount != 1 ||
		archiveItem.MaxScale != contracts.VersionScaleMinor {
		t.Fatalf("missing version metadata in report: %#v", report.Items)
	}

	report, err = index.MailArchiveMissingGmail(ctx, contracts.MailIdentityReportRequest{
		SourceNames:      []string{"deep"},
		IncludeGenerated: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Total != 3 || !containsDigest(report.Items, generated) {
		t.Fatalf("expected generated archive identity when requested: %#v", report)
	}

	resolved, err := index.ResolveMailIdentity(ctx, contracts.MailIdentityResolveRequest{
		MessageID: "<archive-only@example.test>",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Total != 1 ||
		len(resolved.Digests) != 1 ||
		resolved.Digests[0] != archiveOnly {
		t.Fatalf("unexpected mail identity resolution: %#v", resolved)
	}
}

func TestRDFBundleProjectsContactFactsAndCorrections(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })

	store := filestore.NewFilesystemStore(t.TempDir())
	profile := `@prefix bcid: <https://patrickaudley.com/lod#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .

<https://patrickaudley.com/#paudley> a foaf:Person, schema:Person ;
    foaf:name "Patrick Colm Audley"@en ;
    schema:email <mailto:paudley@blackcat.ca> ;
    bcid:historicalEmail <mailto:paudley@gt.ca> ;
    schema:knowsAbout <https://patrickaudley.com/#concept-linked-data> .
`
	profileDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(profile),
		MediaType: "text/turtle",
		Facets: []contracts.Facet{
			contactentity.Facet(contactentity.MetadataInput{
				RootSubject: "https://patrickaudley.com/#paudley",
				Format:      "text/turtle",
				SourceKind:  "fixture",
				IdentityHints: []string{
					"mailto:paudley@blackcat.ca",
				},
			}),
			{
				Kind: contracts.RDFSourceBundleFacetKind,
				Metadata: contactentity.Metadata(contactentity.MetadataInput{
					RootSubject: "https://patrickaudley.com/#paudley",
					Format:      "text/turtle",
				}),
			},
		},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "rdf-profile",
			ExternalID: "profile",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	correction := `@prefix bcid: <https://patrickaudley.com/lod#> .
@prefix time: <http://www.w3.org/2006/time#> .

<< <https://patrickaudley.com/#paudley> bcid:historicalEmail <mailto:paudley@gt.ca> >>
    time:hasEnd "2004-06-30" .
`
	correctionDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(correction),
		MediaType: "text/turtle",
		Facets: []contracts.Facet{{
			Kind: contracts.RDFClaimBundleFacetKind,
			Metadata: contactentity.Metadata(contactentity.MetadataInput{
				TargetSubject: "https://patrickaudley.com/#paudley",
				Format:        "text/turtle",
				ClaimKind:     "correction",
			}),
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "rdf-claim",
			ExternalID: "profile-correction",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	index := newMigratedTestIndex(t, ctx, dsn, migrationsDir, store)
	if err := index.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	aggregate, err := index.ContactAggregate(ctx, contracts.ContactAggregateRequest{
		ContactID: "https://patrickaudley.com/#paudley",
	})
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.DisplayName != "Patrick Colm Audley" ||
		aggregate.PrimaryEmail != "paudley@blackcat.ca" {
		t.Fatalf("unexpected contact aggregate: %#v", aggregate)
	}
	historical := contactFactValueFor(aggregate.Facts, "email", "paudley@gt.ca")
	if historical.ValidUntil != "2004-06-30" || !historical.Historical {
		t.Fatalf("historical email correction not applied: %#v", aggregate.Facts)
	}
	resolved, err := index.ResolveContactIdentity(
		ctx,
		contracts.ContactIdentityResolveRequest{
			Identity: "mailto:paudley@gt.ca",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !sameStrings(resolved.ContactIDs, []string{"https://patrickaudley.com/#paudley"}) {
		t.Fatalf("unexpected contact identity resolution: %#v", resolved)
	}
	search, err := index.ContactSearch(ctx, contracts.ContactSearchRequest{
		Query: "blackcat",
		Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if search.Total != 1 ||
		search.Results[0].ContactID != "https://patrickaudley.com/#paudley" {
		t.Fatalf("contact email was not searchable through rollup: %#v", search)
	}
	emptyPage, err := index.ContactSearch(ctx, contracts.ContactSearchRequest{
		Query:  "blackcat",
		Limit:  5,
		Offset: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if emptyPage.Total != 1 || len(emptyPage.Results) != 0 {
		t.Fatalf("empty page lost contact search total: %#v", emptyPage)
	}

	structure, err := index.Structure(ctx, profileDigest)
	if err != nil {
		t.Fatal(err)
	}
	if len(structure.PartsByRole) != 0 {
		t.Fatalf(
			"RDF profile should not expand into FILESTORE compound parts: %#v",
			structure,
		)
	}
	if _, err := index.Structure(ctx, correctionDigest); err != nil {
		t.Fatal(err)
	}
}

func TestContactMessagesProjectMailParticipantObservations(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })

	store := filestore.NewFilesystemStore(t.TempDir())
	contactID := "https://patrickaudley.com/#paudley"
	profile := `@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .

<https://patrickaudley.com/#paudley> a foaf:Person ;
    foaf:name "Patrick Audley" ;
    schema:email <mailto:paudley@blackcat.ca> .
`
	if _, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(profile),
		MediaType: "text/turtle",
		Facets: []contracts.Facet{
			contactentity.Facet(contactentity.MetadataInput{
				RootSubject: contactID,
				Format:      "text/turtle",
			}),
			{
				Kind: contracts.RDFSourceBundleFacetKind,
				Metadata: contactentity.Metadata(contactentity.MetadataInput{
					RootSubject: contactID,
					Format:      "text/turtle",
				}),
			},
		},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "rdf-profile",
			ExternalID: "contact-messages-profile",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	older := putMailParticipantProjectionFixture(
		t,
		ctx,
		store,
		"<older@example.test>",
		"Wed, 27 May 2026 09:15:00 -0600",
		"Patrick <paudley@blackcat.ca>",
		"Apollo <apollo@example.test>",
	)
	newer := putMailParticipantProjectionFixture(
		t,
		ctx,
		store,
		"<newer@example.test>",
		"Thu, 28 May 2026 10:30:00 -0600",
		"Apollo <apollo@example.test>",
		"Patrick <paudley@blackcat.ca>",
	)

	index := newMigratedTestIndex(t, ctx, dsn, migrationsDir, store)
	if err := index.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	messages, err := index.ContactMessages(ctx, contracts.ContactMessageRequest{
		ContactID: contactID,
		Limit:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if messages.Total != 2 ||
		len(messages.Results) != 1 ||
		messages.Results[0].MessageDigest != newer ||
		messages.Results[0].Role != "to" ||
		messages.Results[0].Token != "paudley@blackcat.ca" {
		t.Fatalf("unexpected contact messages page: %#v", messages)
	}

	fromMessages, err := index.ContactMessages(ctx, contracts.ContactMessageRequest{
		ContactID: contactID,
		Role:      "from",
		Limit:     5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if fromMessages.Total != 1 ||
		len(fromMessages.Results) != 1 ||
		fromMessages.Results[0].MessageDigest != older {
		t.Fatalf("unexpected role-filtered contact messages: %#v", fromMessages)
	}

	aggregate, err := index.ContactAggregate(ctx, contracts.ContactAggregateRequest{
		ContactID: contactID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.MessageCount != 2 ||
		aggregate.ParticipantCount != 2 ||
		aggregate.FirstSeenAt.IsZero() ||
		aggregate.LastSeenAt.IsZero() {
		t.Fatalf("mail observations did not refresh contact rollup: %#v", aggregate)
	}

	manifest, err := store.ReadManifest(ctx, older)
	if err != nil {
		t.Fatal(err)
	}
	for index := range manifest.Facets {
		if manifest.Facets[index].FacetKind() != contracts.MailMessageFacetKind {
			continue
		}
		manifest.Facets[index].Metadata["from"] = "Other <other@example.test>"
		manifest.Facets[index].Metadata["to"] = "Apollo <apollo@example.test>"
	}
	if err := index.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}

	messages, err = index.ContactMessages(ctx, contracts.ContactMessageRequest{
		ContactID: contactID,
		Limit:     5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if messages.Total != 1 ||
		len(messages.Results) != 1 ||
		messages.Results[0].MessageDigest != newer {
		t.Fatalf("stale participant rows survived re-projection: %#v", messages)
	}
}

func TestContactIntelligenceQueriesProjectedFacts(t *testing.T) {
	ctx := context.Background()
	dsn := queryIntegrationDSN(t)
	migrationsDir := queryIntegrationMigrationsDir(t)
	lock := acquireQueryIntegrationLock(t, ctx, dsn)
	t.Cleanup(func() { releaseQueryIntegrationLock(t, lock) })

	store := filestore.NewFilesystemStore(t.TempDir())
	contactID := "https://patrickaudley.com/#paudley"
	profile := `@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .
@prefix bcid: <https://patrickaudley.com/lod#> .

<https://patrickaudley.com/#paudley> a foaf:Person ;
    foaf:name "Patrick Audley" ;
    foaf:knows <https://example.test/#apollo> ;
    schema:affiliation "Blackcat Informatics" ;
    schema:email <mailto:paudley@blackcat.ca> ;
    bcid:historicalEmail <mailto:paudley@gt.ca> .
`
	if _, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(profile),
		MediaType: "text/turtle",
		Facets: []contracts.Facet{
			contactentity.Facet(contactentity.MetadataInput{
				RootSubject: contactID,
				Format:      "text/turtle",
			}),
			{
				Kind: contracts.RDFSourceBundleFacetKind,
				Metadata: contactentity.Metadata(contactentity.MetadataInput{
					RootSubject: contactID,
					Format:      "text/turtle",
				}),
			},
		},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "rdf-profile",
			ExternalID: "contact-intelligence-profile",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	putMailParticipantProjectionFixture(
		t,
		ctx,
		store,
		"<contact-intelligence@example.test>",
		"Thu, 28 May 2026 10:30:00 -0600",
		"Apollo <apollo@example.test>",
		"Patrick <paudley@blackcat.ca>",
	)

	index := newMigratedTestIndex(t, ctx, dsn, migrationsDir, store)
	if err := index.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}

	facts, err := index.ContactFacts(ctx, contracts.ContactFactRequest{
		ContactIDs: []string{contactID},
		FactKinds:  []string{"email"},
		Current:    true,
		Limit:      10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if facts.Total != 1 ||
		len(facts.Facts) != 1 ||
		facts.Facts[0].Value != "paudley@blackcat.ca" {
		t.Fatalf("unexpected current email facts: %#v", facts)
	}

	historical, err := index.ContactFacts(ctx, contracts.ContactFactRequest{
		ContactIDs: []string{contactID},
		FactKinds:  []string{"email"},
		At:         "2001-01-01",
		Limit:      10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if historical.Total != 2 {
		t.Fatalf(
			"expected interval lookup to include open current and historical facts: %#v",
			historical,
		)
	}

	identities, err := index.ContactIdentityDetails(
		ctx,
		contracts.ContactIdentityDetailRequest{
			Identities: []string{"mailto:paudley@blackcat.ca"},
			Limit:      10,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if identities.Total != 1 ||
		identities.Results[0].ContactID != contactID ||
		identities.Results[0].MatchedToken != "paudley@blackcat.ca" {
		t.Fatalf("unexpected identity details: %#v", identities)
	}

	neighborhood, err := index.ContactNeighborhood(
		ctx,
		contracts.ContactNeighborhoodRequest{
			ContactID: contactID,
			Limit:     10,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !contactNeighborhoodHas(
		neighborhood.Results,
		"affiliation",
		"Blackcat Informatics",
	) ||
		!contactNeighborhoodHas(
			neighborhood.Results,
			"relationship",
			"https://example.test/#apollo",
		) {
		t.Fatalf("unexpected contact neighborhood: %#v", neighborhood)
	}

	inputs, err := index.ContactAnalysisInputs(ctx, contracts.ContactAnalysisInputRequest{
		ContactIDs: []string{contactID},
		Limit:      10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inputs.Total != 1 ||
		len(inputs.Results) != 1 ||
		!strings.Contains(inputs.Results[0].InputText, "Contact: Patrick Audley") ||
		!strings.Contains(inputs.Results[0].InputText, "email: paudley@blackcat.ca") ||
		!strings.Contains(
			inputs.Results[0].InputText,
			"Counts: 5 facts, 1 messages, 1 participants",
		) {
		t.Fatalf("unexpected contact analysis input: %#v", inputs)
	}

	filteredInputs, err := index.ContactAnalysisInputs(
		ctx,
		contracts.ContactAnalysisInputRequest{
			ContactIDs: []string{contactID},
			FactKinds:  []string{"affiliation"},
			Limit:      10,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(filteredInputs.Results) != 1 {
		t.Fatalf("expected one filtered analysis input: %#v", filteredInputs)
	}
	if !strings.Contains(
		filteredInputs.Results[0].InputText,
		"affiliation: Blackcat Informatics",
	) ||
		strings.Contains(filteredInputs.Results[0].InputText, "email: paudley@blackcat.ca") {
		t.Fatalf("fact kind filter was not applied to analysis input: %#v", filteredInputs)
	}
}

func putMailIdentityProjectionFixture(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
	messageID string,
	sourceKind string,
	sourceName string,
	generated bool,
) contracts.ObjectDigest {
	t.Helper()
	digest, err := store.PutCompound(ctx, filestore.CompoundPutRequest{
		ObjectID:     "fixture:" + sourceKind + ":" + sourceName + ":" + messageID,
		MediaType:    "application/vnd.gmeow.fixture+json",
		ContentRoles: []string{contracts.MailMessageContentRole},
		Facets: []contracts.Facet{{
			Kind: contracts.MailMessageFacetKind,
			Metadata: map[string]any{
				"rfc_message_id":       messageID,
				"generated_message_id": generated,
				"max_scale":            contracts.VersionScaleMinor,
				"version_count":        1,
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: sourceKind,
			SourceName: sourceName,
			ExternalID: messageID,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func putMailParticipantProjectionFixture(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
	messageID string,
	date string,
	from string,
	to string,
) contracts.ObjectDigest {
	t.Helper()
	digest, err := store.PutCompound(ctx, filestore.CompoundPutRequest{
		ObjectID:     "fixture:mail-participant:" + messageID,
		MediaType:    "application/vnd.gmeow.fixture+json",
		ContentRoles: []string{contracts.MailMessageContentRole},
		Facets: []contracts.Facet{{
			Kind: contracts.MailMessageFacetKind,
			Metadata: map[string]any{
				"rfc_message_id": messageID,
				"date":           date,
				"from":           from,
				"to":             to,
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: "fixture",
			SourceName: "mail-participant",
			ExternalID: messageID,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func putMailMembershipProjectionFixture(
	t *testing.T,
	ctx context.Context,
	store *filestore.FilesystemStore,
	messageID string,
	sourceName string,
) contracts.ObjectDigest {
	t.Helper()
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:       strings.NewReader(`{"membership":true}`),
		MediaType:    "application/vnd.gmeow.mail-archive-membership+json",
		SourceHint:   "membership",
		ContentRoles: []string{contracts.MailArchiveMembershipRole},
		Facets: []contracts.Facet{{
			Kind: contracts.MailArchiveMembershipFacetKind,
			Metadata: map[string]any{
				"rfc_message_id":       messageID,
				"generated_message_id": false,
				"max_scale":            contracts.VersionScaleTrivial,
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: contracts.MailArchiveSourceKind,
			SourceName: sourceName,
			ExternalID: messageID + ":membership",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func containsDigest(
	items []contracts.MailIdentityReportItem,
	digest contracts.ObjectDigest,
) bool {
	for _, item := range items {
		if item.CanonicalDigest == digest {
			return true
		}
	}
	return false
}

func itemForDigest(
	items []contracts.MailIdentityReportItem,
	digest contracts.ObjectDigest,
) (contracts.MailIdentityReportItem, bool) {
	for _, item := range items {
		if item.CanonicalDigest == digest {
			return item, true
		}
	}

	return contracts.MailIdentityReportItem{}, false
}

func contactFactValueFor(
	facts []contracts.ContactFact,
	kind string,
	value string,
) contracts.ContactFact {
	for _, fact := range facts {
		if fact.FactKind == kind && fact.Value == value {
			return fact
		}
	}

	return contracts.ContactFact{}
}

func contactNeighborhoodHas(
	results []contracts.ContactNeighborhoodResult,
	kind string,
	value string,
) bool {
	for _, result := range results {
		if result.FactKind == kind && result.Value == value {
			return true
		}
	}

	return false
}

func queryIntegrationDSN(t *testing.T) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv(testPostgresDSNEnv))
	if dsn == "" {
		t.Skipf("%s is required for postgres integration tests", testPostgresDSNEnv)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse postgres integration DSN: %v", err)
	}
	database := strings.TrimPrefix(parsed.Path, "/")
	user := parsed.User.Username()
	if !strings.Contains(database, "test") || !strings.Contains(user, "test") {
		t.Fatalf(
			"refusing to run postgres integration test against non-test target database=%q user=%q",
			database,
			user,
		)
	}

	return dsn
}

func queryIntegrationMigrationsDir(t *testing.T) string {
	t.Helper()
	dir := strings.TrimSpace(os.Getenv(testQueryMigrationsEnv))
	if dir == "" {
		t.Skipf("%s is required for postgres integration tests", testQueryMigrationsEnv)
	}
	if !filepath.IsAbs(dir) {
		t.Fatalf("%s must be an absolute path outside the repository", testQueryMigrationsEnv)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("read working directory: %v", err)
	}
	rel, err := filepath.Rel(cwd, dir)
	if err == nil && rel != ".." &&
		!strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("%s must not point inside the repository: %s", testQueryMigrationsEnv, dir)
	}
	if strings.Contains(
		dir,
		string(filepath.Separator)+"data"+string(filepath.Separator),
	) {
		t.Fatalf("%s must not point inside a data directory: %s", testQueryMigrationsEnv, dir)
	}

	return dir
}

func createQueryTestSchema(t *testing.T, ctx context.Context, dsn string) string {
	t.Helper()
	schema := fmt.Sprintf("gmeow_test_%d", time.Now().UnixNano())
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres for test schema: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+quoteIdent(schema)); err != nil {
		t.Fatalf("create postgres test schema %s: %v", schema, err)
	}

	return schema
}

func dropQueryTestSchema(t *testing.T, dsn, schema string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres to drop test schema %s: %v", schema, err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(
		ctx,
		`DROP SCHEMA IF EXISTS `+quoteIdent(schema)+` CASCADE`,
	); err != nil {
		t.Fatalf("drop postgres test schema %s: %v", schema, err)
	}
}

func dsnWithSearchPath(dsn, schema string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	query := parsed.Query()
	query.Set("options", "-c search_path="+schema+",public")
	parsed.RawQuery = query.Encode()

	return parsed.String()
}

func newMigratedTestIndex(
	t *testing.T,
	ctx context.Context,
	dsn string,
	migrationsDir string,
	store *filestore.FilesystemStore,
) *Index {
	t.Helper()
	schema := createQueryTestSchema(t, ctx, dsn)
	t.Cleanup(func() { dropQueryTestSchema(t, dsn, schema) })
	config := Config{
		ConnString:     dsnWithSearchPath(dsn, schema),
		MigrationsDir:  migrationsDir,
		MigrationTable: schema + ".goose_db_version",
	}
	if err := Migrate(ctx, config); err != nil {
		t.Fatal(err)
	}
	index, err := New(ctx, config, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(index.Close)

	return index
}

func projectStoredObject(
	t *testing.T,
	ctx context.Context,
	index *Index,
	store *filestore.FilesystemStore,
	digest contracts.ObjectDigest,
) {
	t.Helper()
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}
}

func acquireQueryIntegrationLock(
	t *testing.T,
	ctx context.Context,
	dsn string,
) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres for integration lock: %v", err)
	}
	if _, err := conn.Exec(
		ctx,
		"SELECT pg_advisory_lock($1)",
		int64(0x676d656f775154),
	); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("acquire postgres integration lock: %v", err)
	}

	return conn
}

func releaseQueryIntegrationLock(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	if conn == nil {
		return
	}
	ctx := context.Background()
	if _, err := conn.Exec(
		ctx,
		"SELECT pg_advisory_unlock($1)",
		int64(0x676d656f775154),
	); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("release postgres integration lock: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close postgres integration lock: %v", err)
	}
}

func quoteIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func randomHex(t *testing.T, bytes int) string {
	t.Helper()
	buffer := make([]byte, bytes)
	if _, err := rand.Read(buffer); err != nil {
		t.Fatalf("read random bytes: %v", err)
	}
	return hex.EncodeToString(buffer)
}

func hasMailbox(mailboxes []contracts.JMAPMailbox, mailboxID string) bool {
	for _, mailbox := range mailboxes {
		if mailbox.MailboxID == mailboxID {
			return true
		}
	}

	return false
}

func sameStrings(left, right []string) bool {
	leftCopy := slices.Clone(left)
	rightCopy := slices.Clone(right)
	slices.Sort(leftCopy)
	slices.Sort(rightCopy)

	return slices.Equal(leftCopy, rightCopy)
}

func sameDigests(left, right []contracts.ObjectDigest) bool {
	leftCopy := slices.Clone(left)
	rightCopy := slices.Clone(right)
	slices.Sort(leftCopy)
	slices.Sort(rightCopy)

	return slices.Equal(leftCopy, rightCopy)
}
