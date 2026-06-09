// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	gmail "google.golang.org/api/gmail/v1"
)

func TestGmailMessageMetadataIncludesClassificationLabels(t *testing.T) {
	metadata := gmailMessageMetadata(&gmail.Message{
		HistoryId:    42,
		InternalDate: 1700000000000,
		LabelIds:     []string{"INBOX", "IMPORTANT"},
		SizeEstimate: 1234,
		ClassificationLabelValues: []*gmail.ClassificationLabelValue{{
			LabelId: "class-confidential",
			Fields: []*gmail.ClassificationLabelFieldValue{{
				FieldId:   "sensitivity",
				Selection: "restricted",
			}},
		}},
		Snippet: "derived body text",
	})

	if metadata["snippet"] != nil {
		t.Fatalf("snippet is derived message text and should not be stored: %#v", metadata)
	}
	if metadata["history_id"] != uint64(42) ||
		metadata["internal_date"] != int64(1700000000000) ||
		metadata["received_at"] != "2023-11-14T22:13:20Z" ||
		metadata["size_estimate"] != int64(1234) {
		t.Fatalf("expected Gmail service metadata, got %#v", metadata)
	}

	labels, ok := metadata["classification_label_values"].([]map[string]any)
	if !ok || len(labels) != 1 {
		t.Fatalf("expected classification label values, got %#v", metadata)
	}
	if labels[0]["label_id"] != "class-confidential" {
		t.Fatalf("expected classification label ID, got %#v", labels[0])
	}

	fields, ok := labels[0]["fields"].([]map[string]any)
	if !ok || len(fields) != 1 {
		t.Fatalf("expected classification label fields, got %#v", labels[0])
	}
	if fields[0]["field_id"] != "sensitivity" ||
		fields[0]["selection"] != "restricted" {
		t.Fatalf("expected classification label field values, got %#v", fields[0])
	}
}

func TestConvertRawGmailMessageRejectsMalformedHeader(t *testing.T) {
	_, err := convertRawGmailMessage(&gmail.Message{
		Id:        "bad-header",
		HistoryId: 1,
		Raw: gmailTestData(strings.Join([]string{
			"Message-ID: <bad-header@example.test>",
			"Subject: Bad",
			"5BM=7Ba?=)YK)s[|/0GA?hOme#&b)(>OKI@'.7|<8Ao`{P4XL4?}",
			"",
			"body",
		}, "\r\n")),
	})
	if err == nil || !strings.Contains(err.Error(), "malformed header line") {
		t.Fatalf("expected malformed header parse error, got %v", err)
	}
}

func TestConvertRawGmailMessageRejectsTruncatedMultipart(t *testing.T) {
	_, err := convertRawGmailMessage(&gmail.Message{
		Id:        "truncated",
		HistoryId: 1,
		Raw: gmailTestData(strings.Join([]string{
			"Message-ID: <truncated@example.test>",
			"Subject: Truncated",
			"Content-Type: multipart/mixed; boundary=\"broken\"",
			"",
			"--broken",
			"Content-Type: text/plain",
			"Content-Length: 20",
			"",
			"short",
		}, "\r\n")),
	})
	if err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("expected truncated multipart parse error, got %v", err)
	}
}

func TestConvertStructuredGmailMessageMapsHeadersBodyAndAttachments(t *testing.T) {
	ctx := context.Background()
	message := &gmail.Message{
		Id:           "structured",
		HistoryId:    42,
		ThreadId:     "thread-structured",
		InternalDate: 1700000000000,
		LabelIds:     []string{"INBOX"},
		Payload: &gmail.MessagePart{
			MimeType: "multipart/mixed",
			Headers: []*gmail.MessagePartHeader{
				{Name: "Message-ID", Value: "<structured@example.test>"},
				{Name: "Subject", Value: "Structured"},
				{Name: "From", Value: "Sender <sender@example.test>"},
				{Name: "To", Value: "Receiver <receiver@example.test>"},
			},
			Parts: []*gmail.MessagePart{
				{
					PartId:   "0",
					MimeType: "text/html",
					Body: &gmail.MessagePartBody{
						Data: gmailTestData("<p>html body</p>"),
						Size: int64(len("<p>html body</p>")),
					},
				},
				{
					PartId:   "1",
					MimeType: "text/plain",
					Body: &gmail.MessagePartBody{
						Data: gmailTestData("plain body"),
						Size: int64(len("plain body")),
					},
				},
				{
					PartId:   "2",
					MimeType: "text/plain",
					Filename: "note.txt",
					Body: &gmail.MessagePartBody{
						AttachmentId: "attachment-1",
						Size:         int64(len("attached text")),
					},
				},
			},
		},
	}
	fetcher := func(
		_ context.Context,
		messageID string,
		attachmentID string,
	) ([]byte, error) {
		if messageID != "structured" || attachmentID != "attachment-1" {
			t.Fatalf("unexpected attachment fetch %s/%s", messageID, attachmentID)
		}

		return []byte("attached text"), nil
	}

	converted, err := convertStructuredGmailMessage(ctx, message, fetcher)
	if err != nil {
		t.Fatal(err)
	}

	if converted.MessageID != "structured" ||
		converted.Version != "42" ||
		converted.ThreadID != "thread-structured" ||
		converted.Subject != "Structured" {
		t.Fatalf("unexpected structured message identity: %#v", converted)
	}
	if converted.Headers["message-id"] != "<structured@example.test>" ||
		converted.Headers["from"] != "Sender <sender@example.test>" {
		t.Fatalf("expected structured headers, got %#v", converted.Headers)
	}
	if converted.BodyMediaTyp != "text/plain" || string(converted.Body) != "plain body" {
		t.Fatalf(
			"expected plain body preference, type=%q body=%q",
			converted.BodyMediaTyp,
			converted.Body,
		)
	}
	if len(converted.Attachments) != 1 ||
		converted.Attachments[0].ID != "attachment-1" ||
		converted.Attachments[0].FileName != "note.txt" ||
		string(converted.Attachments[0].Content) != "attached text" {
		t.Fatalf("expected structured attachment, got %#v", converted.Attachments)
	}
}

func TestConvertStructuredGmailMessageFailsClosedOnUndecodablePart(t *testing.T) {
	_, err := convertStructuredGmailMessage(
		context.Background(),
		&gmail.Message{
			Id:        "bad-part",
			HistoryId: 1,
			Payload: &gmail.MessagePart{
				MimeType: "text/plain",
				Body: &gmail.MessagePartBody{
					Data: "not valid base64",
					Size: 1,
				},
			},
		},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "decode gmail structured part") {
		t.Fatalf("expected structured decode error, got %v", err)
	}
}

func TestConvertStructuredGmailMessageFailsClosedOnMissingAttachmentFetcher(
	t *testing.T,
) {
	_, err := convertStructuredGmailMessage(
		context.Background(),
		&gmail.Message{
			Id:        "missing-fetcher",
			HistoryId: 1,
			Payload: &gmail.MessagePart{
				MimeType: "text/plain",
				Filename: "note.txt",
				Body: &gmail.MessagePartBody{
					AttachmentId: "attachment-1",
					Size:         1,
				},
			},
		},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "requires attachment fetcher") {
		t.Fatalf("expected missing attachment fetcher error, got %v", err)
	}
}

func TestConvertStructuredGmailMessagePropagatesAttachmentFetchError(t *testing.T) {
	fetchErr := errors.New("attachment unavailable")
	_, err := convertStructuredGmailMessage(
		context.Background(),
		&gmail.Message{
			Id:        "fetch-error",
			HistoryId: 1,
			Payload: &gmail.MessagePart{
				MimeType: "text/plain",
				Filename: "note.txt",
				Body: &gmail.MessagePartBody{
					AttachmentId: "attachment-1",
					Size:         1,
				},
			},
		},
		func(context.Context, string, string) ([]byte, error) {
			return nil, fetchErr
		},
	)
	if !errors.Is(err, fetchErr) {
		t.Fatalf("expected attachment fetch error, got %v", err)
	}
}

func gmailTestData(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
