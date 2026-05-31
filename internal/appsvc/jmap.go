// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"
	"errors"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type JMAPEmailQueryResponse struct {
	IDs    []contracts.ObjectDigest `json:"ids"`
	Total  int                      `json:"total"`
	Offset int                      `json:"offset"`
	Limit  int                      `json:"limit"`
}

type JMAPEmailQueryRequest struct {
	After      time.Time `json:"after,omitempty"`
	Before     time.Time `json:"before,omitempty"`
	Text       string    `json:"text,omitempty"`
	InMailbox  string    `json:"in_mailbox,omitempty"`
	HasKeyword string    `json:"has_keyword,omitempty"`
	NotKeyword string    `json:"not_keyword,omitempty"`
	Offset     int       `json:"offset"`
	Limit      int       `json:"limit"`
}

type JMAPQuota struct {
	ID           string   `json:"id"`
	ResourceType string   `json:"resource_type"`
	Scope        string   `json:"scope"`
	Name         string   `json:"name"`
	Types        []string `json:"types"`
	Used         uint64   `json:"used"`
}

type JMAPEmailMutation struct {
	ObjectDigest     contracts.ObjectDigest `json:"object_digest"`
	MailboxIDs       map[string]bool        `json:"mailbox_ids,omitempty"`
	Keywords         map[string]bool        `json:"keywords,omitempty"`
	ReplaceMailboxes bool                   `json:"replace_mailboxes,omitempty"`
	ReplaceKeywords  bool                   `json:"replace_keywords,omitempty"`
}

type JMAPMailboxMutation struct {
	Create  map[string]JMAPMailboxCreate `json:"create,omitempty"`
	Update  map[string]JMAPMailboxPatch  `json:"update,omitempty"`
	Destroy []string                     `json:"destroy,omitempty"`
}

type JMAPMailboxCreate struct {
	Name      string `json:"name"`
	ParentID  string `json:"parent_id,omitempty"`
	SortOrder int    `json:"sort_order,omitempty"`
}

type JMAPMailboxPatch struct {
	Name      *string `json:"name,omitempty"`
	ParentID  *string `json:"parent_id,omitempty"`
	SortOrder *int    `json:"sort_order,omitempty"`
}

type JMAPMailboxMutationResult struct {
	Created      map[string]contracts.JMAPMailbox `json:"created,omitempty"`
	Updated      []string                         `json:"updated,omitempty"`
	Destroyed    []string                         `json:"destroyed,omitempty"`
	NotCreated   map[string]string                `json:"not_created,omitempty"`
	NotUpdated   map[string]string                `json:"not_updated,omitempty"`
	NotDestroyed map[string]string                `json:"not_destroyed,omitempty"`
}

type JMAPBlob struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Size int64  `json:"size"`
}

func (services *Services) JMAPQuotas(ctx context.Context) ([]JMAPQuota, error) {
	reader, ok := services.query.(interface {
		ObjectBreakdown(context.Context) (contracts.ObjectBreakdown, error)
	})
	if !ok {
		return nil, errors.New("JMAP quota reader is not configured")
	}

	breakdown, err := reader.ObjectBreakdown(ctx)
	if err != nil {
		return nil, err
	}

	messageCount := uint64(0)
	for _, row := range breakdown.ByFacet {
		if row.Label == MailMessageFacet && row.Count > 0 {
			messageCount = uint64(row.Count)
		}
	}

	return []JMAPQuota{
		{
			ID:           "filestore-bytes",
			ResourceType: "octets",
			Scope:        "account",
			Name:         "Projected FILESTORE bytes",
			Types:        []string{"Email", "Blob"},
			Used:         nonNegativeUint64(breakdown.TotalSizeBytes),
		},
		{
			ID:           "messages",
			ResourceType: "count",
			Scope:        "account",
			Name:         "Projected messages",
			Types:        []string{"Email"},
			Used:         messageCount,
		},
		{
			ID:           "objects",
			ResourceType: "count",
			Scope:        "account",
			Name:         "Projected objects",
			Types:        []string{"Blob"},
			Used:         nonNegativeUint64(breakdown.TotalObjects),
		},
	}, nil
}

func nonNegativeUint64(value int64) uint64 {
	if value <= 0 {
		return 0
	}

	return uint64(value)
}
