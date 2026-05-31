// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
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
