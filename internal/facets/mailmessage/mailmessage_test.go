// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mailmessage

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestParsePreservesHTMLOnlyBody(t *testing.T) {
	raw := strings.Join([]string{
		"Message-ID: <html@example.test>",
		"Content-Type: text/html; charset=utf-8",
		"",
		"<p>Hello</p>",
	}, "\r\n")

	message, err := Parse([]byte(raw), "test")
	if err != nil {
		t.Fatal(err)
	}
	if message.BodyMediaType != "text/html" {
		t.Fatalf("BodyMediaType = %q, want text/html", message.BodyMediaType)
	}
	if string(message.Body) != "<p>Hello</p>" {
		t.Fatalf("Body = %q", message.Body)
	}
}

func TestParsePrefersPlainTextOverHTMLAlternative(t *testing.T) {
	raw := strings.Join([]string{
		"Message-ID: <alternative@example.test>",
		"Content-Type: multipart/alternative; boundary=alt",
		"",
		"--alt",
		"Content-Type: text/html",
		"",
		"<p>Hello</p>",
		"--alt",
		"Content-Type: text/plain",
		"",
		"Hello",
		"--alt--",
		"",
	}, "\r\n")

	message, err := Parse([]byte(raw), "test")
	if err != nil {
		t.Fatal(err)
	}
	if message.BodyMediaType != "text/plain" {
		t.Fatalf("BodyMediaType = %q, want text/plain", message.BodyMediaType)
	}
	if string(message.Body) != "Hello" {
		t.Fatalf("Body = %q", message.Body)
	}
}

func TestParseDecodesTransferEncodedBodyAndAttachments(t *testing.T) {
	attachment := base64.StdEncoding.EncodeToString([]byte("attachment content"))
	raw := strings.Join([]string{
		"Message-ID: <encoded@example.test>",
		"Content-Type: multipart/mixed; boundary=mixed",
		"",
		"--mixed",
		"Content-Type: text/plain",
		"Content-Transfer-Encoding: quoted-printable",
		"",
		"Hello=2C encoded",
		"--mixed",
		"Content-Type: text/plain; name=note.txt",
		"Content-Disposition: attachment; filename=note.txt",
		"Content-Transfer-Encoding: base64",
		"",
		attachment,
		"--mixed--",
		"",
	}, "\r\n")

	message, err := Parse([]byte(raw), "test")
	if err != nil {
		t.Fatal(err)
	}
	if string(message.Body) != "Hello, encoded" {
		t.Fatalf("Body = %q", message.Body)
	}
	if len(message.Attachments) != 1 ||
		string(message.Attachments[0].Content) != "attachment content" {
		t.Fatalf("Attachments = %#v", message.Attachments)
	}
}
