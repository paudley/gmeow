// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"slices"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestDefaultJMAPMailboxesFromGmailLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   []string
		expected []string
	}{
		{
			name:     "inbox",
			labels:   []string{"INBOX", "UNREAD"},
			expected: []string{jmapMailboxAll, jmapMailboxInbox},
		},
		{
			name:     "archived",
			labels:   []string{"IMPORTANT"},
			expected: []string{jmapMailboxAll, jmapMailboxArchive},
		},
		{
			name:     "trash wins",
			labels:   []string{"INBOX", "TRASH"},
			expected: []string{jmapMailboxAll, jmapMailboxTrash},
		},
		{
			name:     "spam wins before inbox",
			labels:   []string{"INBOX", "SPAM"},
			expected: []string{jmapMailboxAll, jmapMailboxSpam},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := defaultJMAPMailboxes(test.labels)
			if !slices.Equal(actual, test.expected) {
				t.Fatalf("expected %#v, got %#v", test.expected, actual)
			}
		})
	}
}

func TestDefaultJMAPKeywordsFromGmailLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   []string
		expected []string
	}{
		{
			name:     "read",
			labels:   []string{"INBOX"},
			expected: []string{jmapKeywordSeen},
		},
		{
			name:     "unread",
			labels:   []string{"INBOX", "UNREAD"},
			expected: []string{},
		},
		{
			name:     "starred",
			labels:   []string{"STARRED"},
			expected: []string{jmapKeywordSeen, jmapKeywordFlagged},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := defaultJMAPKeywords(test.labels)
			if !slices.Equal(actual, test.expected) {
				t.Fatalf("expected %#v, got %#v", test.expected, actual)
			}
		})
	}
}

func TestLabelIDsFromMetadataAcceptsJSONShape(t *testing.T) {
	labels := labelIDsFromMetadata(map[string]any{
		"label_ids": []any{"INBOX", "UNREAD", 5},
	})
	expected := []string{"INBOX", "UNREAD"}
	if !slices.Equal(labels, expected) {
		t.Fatalf("expected %#v, got %#v", expected, labels)
	}
}

func TestParseJMAPTimeUsesMailDateParser(t *testing.T) {
	parsed, ok := parseJMAPTime("Wed, 27 May 2026 09:15:00 -0600 (MDT)")
	if !ok {
		t.Fatal("expected mail date with timezone comment to parse")
	}
	expected := time.Date(2026, 5, 27, 15, 15, 0, 0, time.UTC)
	if !parsed.Equal(expected) {
		t.Fatalf("parsed = %s, want %s", parsed, expected)
	}
}

func TestJMAPReceivedAtUsesOnlyMailMessageTimeFields(t *testing.T) {
	manifest := contracts.Manifest{}
	receivedAt := jmapReceivedAt(manifest, map[string]any{
		"internal_date": int64(1700000000000),
		"date":          "Wed, 27 May 2026 09:15:00 -0600",
	})
	expected := time.Date(2026, 5, 27, 15, 15, 0, 0, time.UTC)
	if receivedAt == nil || !receivedAt.Equal(expected) {
		t.Fatalf("receivedAt = %v, want %s", receivedAt, expected)
	}

	receivedAt = jmapReceivedAt(manifest, map[string]any{
		"received_at": "2023-11-14T22:13:20Z",
		"date":        "Wed, 27 May 2026 09:15:00 -0600",
	})
	expected = time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)
	if receivedAt == nil || !receivedAt.Equal(expected) {
		t.Fatalf("receivedAt = %v, want %s", receivedAt, expected)
	}
}

func TestJMAPOverlayStatePrefersProjectionAnnotation(t *testing.T) {
	digest := contracts.ObjectDigest(
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	state, ok := jmapOverlayState(
		contracts.Manifest{
			ObjectDigest: digest,
			Overlays: map[string]any{
				"jmap": map[string]any{
					"mailbox_ids": []any{"all", "archive"},
					"keywords":    []any{"$seen"},
				},
			},
		},
		[]contracts.Annotation{{
			Kind: "overlays",
			Data: map[string]any{
				"jmap": map[string]any{
					"mailbox_ids": []any{"all", "inbox"},
					"keywords":    []any{"$flagged"},
				},
			},
		}},
	)
	if !ok ||
		!slices.Equal(state.MailboxIDs, []string{"all", "inbox"}) ||
		!slices.Equal(state.Keywords, []string{"$flagged"}) {
		t.Fatalf("unexpected JMAP overlay state ok=%t state=%#v", ok, state)
	}
}
