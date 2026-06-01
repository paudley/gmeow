// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/mailmessage"
	"blackcat.ca/gmeow/internal/testsupport"
)

// TestArchiveImportParallelDedupsDuplicateMessageIDs locks the inter-message
// parallelism invariant: 10 distinct Message-IDs with 5 identical copies each
// must collapse to exactly 10 canonical objects regardless of ingest concurrency
// — the per-Message-ID lock prevents two workers both ingesting a duplicate as a
// new canonical, so the parallel result matches the serial one.
func TestArchiveImportParallelDedupsDuplicateMessageIDs(t *testing.T) {
	ctx := context.Background()

	buildCorpus := func(root string) {
		for id := range 10 {
			body := fmt.Sprintf(
				"Message-ID: <dup-%d@example.test>\r\nFrom: s@example.test\r\n"+
					"To: r@example.test\r\nSubject: subject %d\r\n\r\nbody %d\r\n",
				id, id, id,
			)
			for copyN := range 5 {
				writeTestFile(
					t,
					filepath.Join(root, fmt.Sprintf("m%d-%d.eml", id, copyN)),
					body,
				)
			}
		}
	}

	runImport := func(concurrency int) ArchiveImportReport {
		filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
		defer filestoreService.Close()
		root := t.TempDir()
		buildCorpus(root)
		importer, err := NewArchiveImporter(filestoreService.Client)
		if err != nil {
			t.Fatal(err)
		}
		importer.SetConcurrency(concurrency)
		report, err := importer.Import(ctx, ArchiveImportRequest{
			SourceName: "archive",
			Roots:      []string{root},
		})
		if err != nil {
			t.Fatal(err)
		}

		return report
	}

	serial := runImport(1)
	parallel := runImport(8)

	if serial.Imported != 10 {
		t.Fatalf(
			"serial: expected 10 canonical imports, got %d (%#v)",
			serial.Imported,
			serial,
		)
	}
	if parallel.Imported != serial.Imported {
		t.Fatalf(
			"parallel created a different canonical count: serial=%d parallel=%d",
			serial.Imported, parallel.Imported,
		)
	}
	if parallel.Scanned != 50 {
		t.Fatalf("expected 50 scanned, got %d (%#v)", parallel.Scanned, parallel)
	}
	if dupes := parallel.MessageIDDuplicates + parallel.ExactDuplicates; dupes != 40 {
		t.Fatalf("expected 40 duplicates, got %d (%#v)", dupes, parallel)
	}
}

// TestArchiveImportRecordsRunForListing confirms a completed import writes a
// listable run record (status, counts, source) into the state directory.
func TestArchiveImportRecordsRunForListing(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	stateDir := t.TempDir()
	writeTestFile(
		t,
		filepath.Join(root, "m.eml"),
		"Message-ID: <run@example.test>\r\nSubject: s\r\n\r\nbody\r\n",
	)

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	report, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "arc",
		Roots:      []string{root},
		StateDir:   stateDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	records, err := ListImportRunRecords(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 run record, got %d", len(records))
	}
	record := records[0]
	if record.RunID != report.RunID {
		t.Fatalf("run id mismatch: record=%s report=%s", record.RunID, report.RunID)
	}
	if record.Status != ImportRunStatusCompleted {
		t.Fatalf("expected completed status, got %q", record.Status)
	}
	if record.SourceName != "arc" || record.Imported != 1 {
		t.Fatalf("unexpected record: %#v", record)
	}
}

func TestArchiveImportMaildirReadOnlyAndGeneratedMessageID(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	messagePath := filepath.Join(root, "inbox", "cur", "1:2,S")
	writeTestFile(t, filepath.Join(root, "inbox", "tmp", ".keep"), "")
	writeTestFile(t, filepath.Join(root, "inbox", "new", ".keep"), "")
	writeTestFile(
		t,
		messagePath,
		"From: a@example.test\nTo: b@example.test\nSubject: No id\n\nbody one\n",
	)
	before := statModTime(t, messagePath)

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	report, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{root},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Imported != 1 || report.GeneratedMessageIDs != 1 {
		t.Fatalf("unexpected report: %#v", report)
	}
	after := statModTime(t, messagePath)
	if !after.Equal(before) {
		t.Fatalf("maildir source file was modified: before=%s after=%s", before, after)
	}
	parsed, err := parseArchiveFile(messagePath, root, ArchiveImportFormatMaildir, 0)
	if err != nil {
		t.Fatal(err)
	}
	ref := contracts.SourceObjectRef{
		SourceKind: contracts.MailIdentitySourceKind,
		SourceName: contracts.MailIdentitySourceName,
		ExternalID: parsed.MessageID,
	}
	digest, found, err := filestoreService.Client.LookupSourceObject(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected archive source lookup")
	}
	manifest, err := filestoreService.Store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	metadata := mailMessageMetadataForArchiveTest(t, manifest)
	messageID, _ := metadata["rfc_message_id"].(string)
	if !strings.HasPrefix(messageID, "<gmeow-generated-") {
		t.Fatalf("expected generated message id, got %#v", metadata)
	}
}

func TestArchiveImportExactReimportDoesNotMutateFilestore(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	messagePath := filepath.Join(root, "cur", "1:2,S")
	writeTestFile(t, filepath.Join(root, "tmp", ".keep"), "")
	writeTestFile(t, filepath.Join(root, "new", ".keep"), "")
	writeTestFile(
		t,
		messagePath,
		"Message-ID: <exact@example.test>\nFrom: a@example.test\nTo: b@example.test\nSubject: Exact\n\nsame body\n",
	)

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	firstReport, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{root},
	})
	if err != nil {
		t.Fatal(err)
	}
	if firstReport.Imported != 1 {
		t.Fatalf("expected first import to write one canonical message, got %#v", firstReport)
	}

	canonical, found, err := filestoreService.Client.LookupSourceObject(
		ctx,
		contracts.SourceObjectRef{
			SourceKind: contracts.MailIdentitySourceKind,
			SourceName: contracts.MailIdentitySourceName,
			ExternalID: "<exact@example.test>",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected canonical message lookup")
	}
	before := archiveObjectManifests(t, ctx, filestoreService, canonical)

	secondReport, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{root},
	})
	if err != nil {
		t.Fatal(err)
	}
	if secondReport.Imported != 0 ||
		secondReport.ExactDuplicates != 0 ||
		secondReport.MessageIDDuplicates != 1 {
		t.Fatalf("expected exact reimport to be a duplicate no-op, got %#v", secondReport)
	}

	after := archiveObjectManifests(t, ctx, filestoreService, canonical)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf(
			"exact reimport mutated filestore manifests\nbefore=%#v\nafter=%#v",
			before,
			after,
		)
	}
}

func TestArchiveImportProgressIncludesPrecountTotal(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	writeTestFile(
		t,
		filepath.Join(root, "one.eml"),
		"Message-ID: <one@example.test>\nSubject: one\n\none\n",
	)
	writeTestFile(
		t,
		filepath.Join(root, "two.eml"),
		"Message-ID: <two@example.test>\nSubject: two\n\ntwo\n",
	)

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	var final ArchiveImportProgress
	report, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{root},
		Progress: func(progress ArchiveImportProgress) {
			final = progress
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Imported != 2 {
		t.Fatalf("expected two imports, got %#v", report)
	}
	if final.Total != 2 || final.Ingested != 2 {
		t.Fatalf("expected final progress 2/2, got %#v", final)
	}
}

func TestArchiveImportPrecountSingleFileMirrorsImportRoot(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "message")
	writeTestFile(t, path, "X-Test: one\n\nbody\n")

	count, err := countArchiveRootMessages(
		context.Background(),
		path,
		ArchiveImportFormatAuto,
	)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected single non-directory root to count as one message, got %d", count)
	}
}

func TestArchiveImportPrecountKeepsPartialMboxCountOnScannerError(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "Inbox")
	writeTestFile(t, path, strings.Join([]string{
		"From sender@example.test Sat Jan 01 00:00:00 2000",
		"Message-ID: <one@example.test>",
		"Subject: one",
		"",
		"first body",
		"From sender@example.test Sat Jan 01 00:00:01 2000",
		strings.Repeat("x", 33*1024*1024),
	}, "\n"))

	count, err := countArchiveRootMessages(
		context.Background(),
		path,
		ArchiveImportFormatMbox,
	)
	if err == nil {
		t.Fatal("expected scanner error")
	}
	if count != 1 {
		t.Fatalf("expected partial mbox count to keep first message, got %d", count)
	}
}

func TestArchiveImportPrecountSkipsBadRoots(t *testing.T) {
	root := t.TempDir()
	writeTestFile(
		t,
		filepath.Join(root, "one.eml"),
		"Message-ID: <one@example.test>\nSubject: one\n\none\n",
	)

	count, err := countArchiveImportMessages(context.Background(), ArchiveImportRequest{
		Roots: []string{filepath.Join(root, "missing"), root},
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected count from valid root only, got %d", count)
	}
}

func TestArchiveImportMboxNNMLAndCollisionVariant(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "Inbox"), strings.Join([]string{
		"From sender@example.test Sat Jan 01 00:00:00 2000",
		"Message-ID: <same@example.test>",
		"From: sender@example.test",
		"To: recv@example.test",
		"Subject: first",
		"",
		"short body",
		"From sender@example.test Sat Jan 01 00:00:01 2000",
		"Message-ID: <same@example.test>",
		"From: sender@example.test",
		"To: recv@example.test",
		"Subject: first changed",
		"",
		"longer body wins canonical slot",
		"",
	}, "\n"))
	writeTestFile(
		t,
		filepath.Join(root, "nnml", "1"),
		"Message-ID: <nnml@example.test>\nSubject: nnml\n\nnnml body",
	)

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	report, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{root},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Imported != 2 || report.Collisions != 1 {
		t.Fatalf("unexpected report: %#v", report)
	}

	digest, found, err := filestoreService.Client.LookupSourceObject(
		ctx,
		contracts.SourceObjectRef{
			SourceKind: contracts.MailIdentitySourceKind,
			SourceName: contracts.MailIdentitySourceName,
			ExternalID: "<same@example.test>",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected Message-ID identity lookup")
	}
	if digest == "" {
		t.Fatal("empty canonical digest")
	}
	parsedMessages, err := parseMboxFile(filepath.Join(root, "Inbox"), root)
	if err != nil {
		t.Fatal(err)
	}
	variantDigest, found, err := filestoreService.Client.LookupSourceObject(
		ctx,
		contracts.SourceObjectRef{
			SourceKind:      contracts.MailArchiveSourceKind,
			SourceName:      "archive",
			ExternalID:      "Inbox#1:variant",
			ExternalVersion: parsedMessages[1].ExternalVersion,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected variant source lookup")
	}
	structure, err := filestoreService.Store.GetStructure(ctx, variantDigest)
	if err != nil {
		t.Fatal(err)
	}
	if len(structure.PartsByRole[contracts.MailPatchDiffRole]) < 2 {
		t.Fatalf(
			"expected variant patch diffs in canonical compound: %#v",
			structure.PartsByRole,
		)
	}
	manifest, err := filestoreService.Store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	metadata := mailMessageMetadataForArchiveTest(t, manifest)
	if metadata["version_count"] != float64(2) && metadata["version_count"] != 2 {
		t.Fatalf("expected version_count=2, got %#v", metadata)
	}
}

func TestArchiveImportVariantDoesNotOverwriteCanonicalParts(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	first := filepath.Join(root, "first.eml")
	second := filepath.Join(root, "second.eml")
	writeTestFile(
		t,
		first,
		"Message-ID: <variant@example.test>\nFrom: a@example.test\nTo: b@example.test\nSubject: canonical\n\ncanonical body is much longer\n",
	)
	writeTestFile(
		t,
		second,
		"Message-ID: <variant@example.test>\nFrom: a@example.test\nTo: b@example.test\nSubject: variant\n\nshort\n",
	)

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{first},
	}); err != nil {
		t.Fatal(err)
	}
	report, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Collisions != 1 || report.Promoted != 0 {
		t.Fatalf("expected non-promoted variant collision, got %#v", report)
	}
	canonical, found, err := filestoreService.Client.LookupSourceObject(
		ctx,
		contracts.SourceObjectRef{
			SourceKind: contracts.MailIdentitySourceKind,
			SourceName: contracts.MailIdentitySourceName,
			ExternalID: "<variant@example.test>",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected canonical message lookup")
	}
	structure, err := filestoreService.Store.GetStructure(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	bodyParts := structure.PartsByRole[contracts.MailBodyRole]
	if len(bodyParts) == 0 {
		t.Fatalf("canonical body missing: %#v", structure.PartsByRole)
	}
	reader, err := filestoreService.Store.Open(ctx, bodyParts[0].Digest)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	if closeErr := reader.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "canonical body is much longer\n" {
		t.Fatalf("canonical body was overwritten: %q", body)
	}
	manifest, err := filestoreService.Store.ReadManifest(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	metadata := mailMessageMetadataForArchiveTest(t, manifest)
	if metadata["subject"] != "canonical" {
		t.Fatalf("canonical metadata was overwritten: %#v", metadata)
	}
}

func archiveObjectManifests(
	t *testing.T,
	ctx context.Context,
	filestoreService *testsupport.FilestoreService,
	canonical contracts.ObjectDigest,
) map[contracts.ObjectDigest]contracts.Manifest {
	t.Helper()

	structure, err := filestoreService.Store.GetStructure(ctx, canonical)
	if err != nil {
		t.Fatal(err)
	}
	digests := map[contracts.ObjectDigest]bool{canonical: true}
	for _, parts := range structure.PartsByRole {
		for _, part := range parts {
			digests[part.Digest] = true
		}
	}

	manifests := make(map[contracts.ObjectDigest]contracts.Manifest, len(digests))
	for digest := range digests {
		manifest, err := filestoreService.Store.ReadManifest(ctx, digest)
		if err != nil {
			t.Fatal(err)
		}
		manifests[digest] = manifest
	}

	return manifests
}

func TestArchiveImportLowNoiseSkipsBodyLineMatchesWithoutProvenance(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	first := filepath.Join(root, "one.eml")
	second := filepath.Join(root, "backup", "two.eml")
	writeTestFile(
		t,
		first,
		"Message-ID: <dup@example.test>\nFrom: a@example.test\nTo: b@example.test\nSubject: Same\n\nline one\nline two\n",
	)
	writeTestFile(
		t,
		second,
		"Message-ID: <dup@example.test>\nReceived: backup host\nFrom: a@example.test\nTo: b@example.test\nSubject: Same\n\nline one\nline two\n",
	)

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{first},
	}); err != nil {
		t.Fatal(err)
	}
	report, err := importer.Import(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{second},
		LowNoise:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.LowNoiseSkipped != 1 || report.TrivialSkipped != 1 {
		t.Fatalf("expected low-noise trivial skip, got %#v", report)
	}
	parsed, err := parseArchiveFile(second, second, ArchiveImportFormatEMLDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	aliasDigest, found, err := filestoreService.Client.LookupSourceObject(
		ctx,
		contracts.SourceObjectRef{
			SourceKind:      contracts.MailArchiveSourceKind,
			SourceName:      "archive",
			ExternalID:      parsed.ExternalID,
			ExternalVersion: parsed.ExternalVersion,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("low-noise duplicate should be discoverable through archive alias")
	}
	canonicalDigest, found, err := filestoreService.Client.LookupSourceObject(
		ctx,
		contracts.SourceObjectRef{
			SourceKind: contracts.MailIdentitySourceKind,
			SourceName: contracts.MailIdentitySourceName,
			ExternalID: "<dup@example.test>",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found || aliasDigest != canonicalDigest {
		t.Fatalf(
			"low-noise duplicate alias should preserve canonical lookup, alias=%s canonical=%s found=%t",
			aliasDigest,
			canonicalDigest,
			found,
		)
	}
	membershipDigest, found, err := filestoreService.Client.LookupSourceObject(
		ctx,
		contracts.SourceObjectRef{
			SourceKind:      contracts.MailArchiveSourceKind,
			SourceName:      "archive",
			ExternalID:      parsed.ExternalID + ":membership",
			ExternalVersion: parsed.ExternalVersion,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("low-noise duplicate should write compact membership provenance")
	}
	membership, err := filestoreService.Store.ReadManifest(ctx, membershipDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !hasFacetKind(membership, contracts.MailArchiveMembershipFacetKind) {
		t.Fatalf("membership object missing facet: %#v", membership.Facets)
	}
}

func TestMboxStreamingOffsetsParseOneMessageAtATime(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "Inbox")
	writeTestFile(t, path, strings.Join([]string{
		"From sender@example.test Sat Jan 01 00:00:00 2000",
		"Message-ID: <one@example.test>",
		"Subject: one",
		"",
		"first body",
		"From sender@example.test Sat Jan 01 00:00:01 2000",
		"Message-ID: <two@example.test>",
		"Subject: two",
		"",
		"second body",
		"",
	}, "\n"))

	offsets := []int64{}
	err := forEachMboxMessageOffset(path, func(offset, _ int64) error {
		offsets = append(offsets, offset)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 1 {
		t.Fatalf("unexpected offsets: %#v", offsets)
	}
	message, err := parseMboxMessageAt(path, root, 1)
	if err != nil {
		t.Fatal(err)
	}
	if message.MessageID != "<two@example.test>" ||
		!strings.Contains(string(message.Body), "second body") {
		t.Fatalf("parsed wrong mbox message: %#v body=%q", message, message.Body)
	}
}

func TestMboxStreamingReturnsMessageParseError(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "Inbox")
	writeTestFile(t, path, strings.Join([]string{
		"From sender@example.test Sat Jan 01 00:00:00 2000",
		"this is not a valid header",
		"",
		"bad body",
		"",
	}, "\n"))
	called := false
	err := forEachMboxMessage(path, root, func(archiveMessage) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("expected mbox parse error")
	}
	if called {
		t.Fatal("callback should not run for an unparseable message")
	}
}

func TestExtractMailBodyAndAttachmentsRecursesNestedMultipart(t *testing.T) {
	raw := strings.Join([]string{
		"Message-ID: <nested@example.test>",
		"Content-Type: multipart/mixed; boundary=mixed",
		"",
		"--mixed",
		"Content-Type: multipart/alternative; boundary=alt",
		"",
		"--alt",
		"Content-Type: text/plain",
		"",
		"nested plain body",
		"--alt--",
		"--mixed",
		"Content-Type: text/plain; name=note.txt",
		"Content-Disposition: attachment; filename=note.txt",
		"",
		"attachment body",
		"--mixed--",
		"",
	}, "\n")
	message, err := parseArchiveMessage(
		[]byte(raw),
		"nested.eml",
		".",
		ArchiveImportFormatEMLDir,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(message.Body) != "nested plain body" {
		t.Fatalf("unexpected body: %q", message.Body)
	}
	if len(message.Attachments) != 1 ||
		message.Attachments[0].FileName != "note.txt" ||
		string(message.Attachments[0].Content) != "attachment body" {
		t.Fatalf("unexpected attachments: %#v", message.Attachments)
	}
}

func TestArchiveMailMetadataUsesGenericMailMessageFields(t *testing.T) {
	message := archiveMessage{
		Message: mailmessage.Message{
			MessageID:    "<archive@example.test>",
			Subject:      "Archive update",
			Date:         "Wed, 27 May 2026 09:15:00 -0600",
			From:         "sender@example.test",
			To:           "recipient@example.test",
			Fingerprint:  "fingerprint",
			BodyLineHash: "body-line",
		},
		Format:     ArchiveImportFormatEMLDir,
		Mailbox:    "cur",
		SourcePath: "mail/cur/1.eml",
	}

	metadata := archiveMailMetadata(
		message,
		false,
		contracts.VersionScaleMinor,
		1,
		message.Fingerprint,
	)

	if metadata["date"] != "Wed, 27 May 2026 09:15:00 -0600" ||
		metadata["subject"] != "Archive update" ||
		metadata["archive_format"] != ArchiveImportFormatEMLDir ||
		metadata["archive_mailbox"] != "cur" ||
		metadata["archive_source_path"] != "mail/cur/1.eml" {
		t.Fatalf("unexpected archive mail-message metadata: %#v", metadata)
	}
	if _, ok := metadata["received_at"]; ok {
		t.Fatalf("archive import should not fabricate received_at: %#v", metadata)
	}
}

func TestQueuedArchiveImportUsesLocalStateAndMessageJobs(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	writeTestFile(
		t,
		filepath.Join(root, "one.eml"),
		"Message-ID: <queued@example.test>\nSubject: queued\n\nqueued body",
	)

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	queue := &memoryArchiveImportQueue{}
	report, err := (ArchiveImportQueuedRun{
		Importer: importer,
		Source:   queue,
	}).Run(ctx, ArchiveImportRequest{
		SourceName: "archive",
		Roots:      []string{root},
		StateDir:   t.TempDir(),
		Resume:     true,
		Publisher:  queue,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Enqueued != 1 || report.Processed != 1 || report.Imported != 1 {
		t.Fatalf("unexpected queued import report: %#v", report)
	}
	if queue.published != 1 {
		t.Fatalf("expected one published job, got %d", queue.published)
	}
}

func TestQueuedArchiveImportDrainsAtQueueHighWater(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	root := t.TempDir()
	for _, name := range []string{"one", "two", "three"} {
		writeTestFile(
			t,
			filepath.Join(root, name+".eml"),
			"Message-ID: <"+name+"@example.test>\nSubject: "+name+"\n\n"+name+" body",
		)
	}

	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	queue := &memoryArchiveImportQueue{}
	report, err := (ArchiveImportQueuedRun{
		Importer: importer,
		Source:   queue,
	}).Run(ctx, ArchiveImportRequest{
		SourceName:     "archive",
		Roots:          []string{root},
		StateDir:       t.TempDir(),
		Publisher:      queue,
		QueueHighWater: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Enqueued != 3 || report.Processed != 3 || report.Imported != 3 {
		t.Fatalf("unexpected queued import report: %#v", report)
	}
}

func TestQueuedArchiveImportReleasesForeignRunJobs(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	importer, err := NewArchiveImporter(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	queue := &memoryArchiveImportQueue{
		jobs: []contracts.SourceImportJob{{
			RunID: "other-run",
		}},
	}
	report := ArchiveImportReport{RunID: "this-run"}
	state := archiveImportRunState{RunID: "this-run"}
	err = (ArchiveImportQueuedRun{
		Importer: importer,
		Source:   queue,
	}).drainOne(ctx, ArchiveImportRequest{Publisher: queue}, "archive", &state, &report)
	if !errors.Is(err, errSourceImportOtherRunJob) {
		t.Fatalf("expected foreign run sentinel, got %v", err)
	}
	if queue.released != 1 || queue.retried != 0 || report.Processed != 0 {
		t.Fatalf(
			"foreign job should be released without retry/process: released=%d retried=%d report=%#v",
			queue.released,
			queue.retried,
			report,
		)
	}
	if len(queue.jobs) != 1 || queue.jobs[0].RunID != "other-run" {
		t.Fatalf("foreign job was not left available: %#v", queue.jobs)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatal(err)
	}
}

func statModTime(t *testing.T, path string) time.Time {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.ModTime()
}

func mailMessageMetadataForArchiveTest(
	t *testing.T,
	manifest contracts.Manifest,
) map[string]any {
	t.Helper()
	for _, facet := range manifest.Facets {
		if facet.FacetKind() == contracts.MailMessageFacetKind {
			return facet.Metadata
		}
	}
	t.Fatalf("manifest missing mail message facet: %#v", manifest.Facets)
	return nil
}

func hasFacetKind(manifest contracts.Manifest, kind string) bool {
	for _, facet := range manifest.Facets {
		if facet.FacetKind() == kind {
			return true
		}
	}

	return false
}

type memoryArchiveImportQueue struct {
	jobs      []contracts.SourceImportJob
	published int
	released  int
	retried   int
}

func (queue *memoryArchiveImportQueue) PublishSourceImportJob(
	_ context.Context,
	job contracts.SourceImportJob,
) error {
	queue.jobs = append(queue.jobs, job)
	queue.published++

	return nil
}

func (queue *memoryArchiveImportQueue) ProcessSourceImportFailures(
	context.Context,
	int,
) (int, error) {
	return 0, nil
}

func (queue *memoryArchiveImportQueue) SourceImportStatus(
	context.Context,
) (contracts.SourceImportQueueStatus, error) {
	return contracts.SourceImportQueueStatus{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Pending:       len(queue.jobs),
	}, nil
}

func (queue *memoryArchiveImportQueue) Receive(
	context.Context,
) (ArchiveImportJobReceipt, error) {
	if len(queue.jobs) == 0 {
		return nil, os.ErrNotExist
	}
	job := queue.jobs[0]
	queue.jobs = queue.jobs[1:]

	return &memoryArchiveImportReceipt{queue: queue, job: job}, nil
}

type memoryArchiveImportReceipt struct {
	queue *memoryArchiveImportQueue
	job   contracts.SourceImportJob
}

func (receipt *memoryArchiveImportReceipt) Job() contracts.SourceImportJob {
	return receipt.job
}

func (*memoryArchiveImportReceipt) Ack(context.Context) error {
	return nil
}

func (receipt *memoryArchiveImportReceipt) Release(context.Context) error {
	receipt.queue.released++
	receipt.queue.jobs = append(
		[]contracts.SourceImportJob{receipt.job},
		receipt.queue.jobs...)

	return nil
}

func (receipt *memoryArchiveImportReceipt) Retry(context.Context, error) error {
	receipt.queue.retried++

	return nil
}
