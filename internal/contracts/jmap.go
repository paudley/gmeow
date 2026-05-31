// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import "time"

const (
	JMAPMailboxCatalogSourceKind = "jmap"
	JMAPMailboxCatalogSourceName = "mailboxes"
	JMAPMailboxCatalogCursorKey  = "mailboxes"
)

type JMAPMailbox struct {
	CreatedAt   time.Time `json:"created_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
	MailboxID   string    `json:"mailbox_id"`
	Name        string    `json:"name"`
	Role        string    `json:"role,omitempty"`
	ParentID    string    `json:"parent_id,omitempty"`
	SortOrder   int       `json:"sort_order,omitempty"`
	IsSystem    bool      `json:"is_system,omitempty"`
	IsDestroyed bool      `json:"is_destroyed,omitempty"`
}

type JMAPMailboxCatalogUpdate struct {
	Mailboxes []JMAPMailbox `json:"mailboxes"`
}

type JMAPMailboxEmailCountRequest struct {
	MailboxIDs []string `json:"mailbox_ids"`
}

type JMAPMailboxEmailCountResponse struct {
	Counts map[string]int `json:"counts"`
}

type JMAPEmailState struct {
	ReceivedAt    time.Time    `json:"received_at,omitempty"`
	ObjectDigest  ObjectDigest `json:"object_digest"`
	ThreadID      string       `json:"thread_id,omitempty"`
	MailboxIDs    []string     `json:"mailbox_ids,omitempty"`
	Keywords      []string     `json:"keywords,omitempty"`
	StateSequence int64        `json:"state_sequence,omitempty"`
}

type JMAPEmailStateUpdate struct {
	ObjectDigest ObjectDigest `json:"object_digest"`
	MailboxIDs   []string     `json:"mailbox_ids"`
	Keywords     []string     `json:"keywords"`
}

type JMAPEmailQueryRequest struct {
	After      time.Time `json:"after,omitempty"`
	Before     time.Time `json:"before,omitempty"`
	Text       string    `json:"text,omitempty"`
	InMailbox  string    `json:"in_mailbox,omitempty"`
	HasKeyword string    `json:"has_keyword,omitempty"`
	NotKeyword string    `json:"not_keyword,omitempty"`
	Limit      int       `json:"limit,omitempty"`
	Offset     int       `json:"offset,omitempty"`
}

type JMAPEmailQueryResponse struct {
	IDs    []ObjectDigest `json:"ids"`
	Total  int            `json:"total"`
	Offset int            `json:"offset"`
	Limit  int            `json:"limit"`
}

type JMAPThread struct {
	ID       string         `json:"id"`
	EmailIDs []ObjectDigest `json:"email_ids"`
}

type JMAPBlobLookupRequest struct {
	TypeNames []string       `json:"type_names"`
	BlobIDs   []ObjectDigest `json:"blob_ids"`
}

type JMAPBlobLookupResponse struct {
	Blobs map[ObjectDigest]JMAPBlobReferences `json:"blobs"`
}

type JMAPBlobReferences struct {
	EmailIDs   []ObjectDigest `json:"email_ids,omitempty"`
	ThreadIDs  []string       `json:"thread_ids,omitempty"`
	MailboxIDs []string       `json:"mailbox_ids,omitempty"`
}
