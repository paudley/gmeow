// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"slices"
	"testing"

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
