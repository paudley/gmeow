// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package imap

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestIMAPExposesOnlyMailFacet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	services := testServices(t)
	server, err := New("127.0.0.1:0", "user", "pass", services)
	if err != nil {
		t.Fatal(err)
	}

	errc := make(chan error, 1)
	go func() {
		errc <- server.Start(ctx)
	}()
	address := waitAddress(t, server)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	mustReadContains(t, reader, "OK")

	sendIMAP(t, conn, `a1 LOGIN "user" "pass"`)
	mustReadUntilTag(t, reader, "a1", "OK")
	sendIMAP(t, conn, "a2 SELECT INBOX")
	selectLines := mustReadUntilTag(t, reader, "a2", "OK")
	if !strings.Contains(selectLines, "EXISTS") {
		t.Fatalf("expected mailbox count, got:\n%s", selectLines)
	}
	sendIMAP(t, conn, "a3 SEARCH gmeow-imap-mail-fixture")
	searchLines := mustReadUntilTag(t, reader, "a3", "OK")
	if !strings.Contains(searchLines, "* SEARCH 1") {
		t.Fatalf("expected mail fixture in search response:\n%s", searchLines)
	}
	sendIMAP(t, conn, "a4 SEARCH gmeow-imap-file-fixture")
	fileSearchLines := mustReadUntilTag(t, reader, "a4", "OK")
	if strings.Contains(fileSearchLines, "1") {
		t.Fatalf("file facet leaked into IMAP search response:\n%s", fileSearchLines)
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-errc
}

func testServices(t *testing.T) *appsvc.Services {
	t.Helper()
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	t.Cleanup(filestoreService.Close)
	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	t.Cleanup(queryService.Close)
	for _, fixture := range []struct {
		text  string
		facet string
	}{
		{text: "gmeow-imap-mail-fixture", facet: appsvc.MailMessageFacet},
		{text: "gmeow-imap-file-fixture", facet: "file"},
	} {
		digest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
			Reader:    strings.NewReader(fixture.text),
			MediaType: "text/plain",
			Facets:    []contracts.Facet{{Kind: fixture.facet}},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { testsupport.CleanupQueryObjects(t, digest) })
		manifest, err := filestoreService.Client.ReadManifest(ctx, digest)
		if err != nil {
			t.Fatal(err)
		}
		annotation := contracts.Annotation{
			SchemaVersion: contracts.SchemaVersionPhase00,
			Kind:          "analysis",
			ObjectDigest:  digest,
			AnalyzerName:  "text.extract",
			AnalyzerVer:   "test",
			Data:          map[string]any{"text": fixture.text},
		}
		if err := queryService.Client.Project(ctx, manifest, []contracts.Annotation{annotation}); err != nil {
			t.Fatal(err)
		}
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

func waitAddress(t *testing.T, server *Server) string {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		address := server.Addr()
		if !strings.HasSuffix(address, ":0") {
			return address
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("IMAP server did not start listening")

	return ""
}

func sendIMAP(t *testing.T, conn net.Conn, command string) {
	t.Helper()
	if _, err := conn.Write([]byte(command + "\r\n")); err != nil {
		t.Fatal(err)
	}
}

func mustReadContains(t *testing.T, reader *bufio.Reader, needle string) {
	t.Helper()
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, needle) {
		t.Fatalf("expected %q in %q", needle, line)
	}
}

func mustReadUntilTag(t *testing.T, reader *bufio.Reader, tag, status string) string {
	t.Helper()
	var builder strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		builder.WriteString(line)
		if strings.HasPrefix(line, tag+" ") {
			if !strings.Contains(line, status) {
				t.Fatalf("expected tag %s status %s in:\n%s", tag, status, builder.String())
			}

			return builder.String()
		}
	}
}
