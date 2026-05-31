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

func TestMetadataIncludesParticipantHeaders(t *testing.T) {
	message := Message{
		Headers: map[string]string{
			"sender":   "sender@example.test",
			"reply-to": "reply@example.test",
			"cc":       "Carbon <cc@example.test>",
			"bcc":      "Blind <bcc@example.test>",
		},
		MessageID: "<participants@example.test>",
		From:      "From <from@example.test>",
		To:        "To <to@example.test>",
	}

	metadata := Metadata(message, false, "", 1, "")
	for _, key := range []string{"from", "sender", "reply_to", "to", "cc", "bcc"} {
		if metadata[key] == "" {
			t.Fatalf("metadata missing participant header %q: %#v", key, metadata)
		}
	}
}

func TestParticipantsFromMetadataParsesRolesAndDisplayNames(t *testing.T) {
	participants := ParticipantsFromMetadata(map[string]any{
		"from":     "Alice Example <alice@example.test>",
		"reply_to": "Replies <reply@example.test>",
		"to":       "Bob <bob@example.test>, carol@example.test",
		"cc":       "Carbon <cc@example.test>",
		"bcc":      "not an address",
	})

	expected := map[string]string{
		"from:alice@example.test":     "Alice Example",
		"reply_to:reply@example.test": "Replies",
		"to:bob@example.test":         "Bob",
		"to:carol@example.test":       "",
		"cc:cc@example.test":          "Carbon",
	}
	if len(participants) != len(expected) {
		t.Fatalf("ParticipantsFromMetadata returned %#v", participants)
	}

	for _, participant := range participants {
		key := participant.Role + ":" + participant.Address
		displayName, ok := expected[key]
		if !ok {
			t.Fatalf("unexpected participant %#v", participant)
		}
		if participant.DisplayName != displayName {
			t.Fatalf(
				"display name for %s = %q, want %q",
				key,
				participant.DisplayName,
				displayName,
			)
		}
		if participant.RawValue == "" {
			t.Fatalf("participant missing raw value: %#v", participant)
		}
	}
}

func TestParticipantsFromMetadataKeepsValidAddressesFromMalformedList(t *testing.T) {
	participants := ParticipantsFromMetadata(map[string]any{
		"to": `"Valid, Name" <valid@example.test>, not an address, Other <other@example.test>`,
	})

	expected := []string{"valid@example.test", "other@example.test"}
	if len(participants) != len(expected) {
		t.Fatalf("ParticipantsFromMetadata returned %#v", participants)
	}

	for index, address := range expected {
		if participants[index].Address != address {
			t.Fatalf(
				"participant %d address = %q, want %q: %#v",
				index,
				participants[index].Address,
				address,
				participants,
			)
		}
	}
}
