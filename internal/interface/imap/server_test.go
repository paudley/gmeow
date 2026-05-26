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
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/query/memory"
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
	if !strings.Contains(selectLines, "* 1 EXISTS") {
		t.Fatalf("expected only one mail object, got:\n%s", selectLines)
	}
	sendIMAP(t, conn, "a3 SEARCH ALL")
	searchLines := mustReadUntilTag(t, reader, "a3", "OK")
	if !strings.Contains(searchLines, "* SEARCH 1") {
		t.Fatalf("unexpected search response:\n%s", searchLines)
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
	store := filestore.NewFilesystemStore(t.TempDir())
	index := memory.New(store)
	for _, fixture := range []struct {
		text  string
		facet string
	}{
		{text: "hello mail", facet: appsvc.MailMessageFacet},
		{text: "hello file", facet: "file"},
	} {
		digest, err := store.Put(ctx, filestore.PutRequest{
			Reader:    strings.NewReader(fixture.text),
			MediaType: "text/plain",
			Facets:    []contracts.Facet{{Kind: fixture.facet}},
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
	}

	services, err := appsvc.New(appsvc.Options{Query: index, Objects: store})
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

func mustReadUntilTag(t *testing.T, reader *bufio.Reader, tag string, status string) string {
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
