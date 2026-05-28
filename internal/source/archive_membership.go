// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"bytes"
	"context"
	"encoding/json"

	"blackcat.ca/gmeow/internal/contracts"
)

func (importer *ArchiveImporter) writeArchiveMembershipRecord(
	ctx context.Context,
	sourceName string,
	message archiveMessage,
	canonical contracts.ObjectDigest,
) error {
	payload, err := json.Marshal(map[string]any{
		"message_id":            message.MessageID,
		"canonical_digest":      canonical,
		"body_line_fingerprint": message.BodyLineHash,
		"source_path":           message.SourcePath,
		"mailbox":               message.Mailbox,
		"format":                message.Format,
		"external_id":           message.ExternalID,
		"external_version":      message.ExternalVersion,
	})
	if err != nil {
		return err
	}
	_, _, err = importer.service.Ingest(ctx, IngestObject{
		ObservedAt:   message.ObservedAt,
		Reader:       bytes.NewReader(payload),
		MediaType:    "application/vnd.gmeow.mail-archive-membership+json",
		SourceKind:   contracts.MailArchiveSourceKind,
		SourceName:   sourceName,
		ExternalID:   message.ExternalID + ":membership",
		ExternalVer:  message.ExternalVersion,
		SourceHint:   "archive membership",
		ContentRoles: []string{contracts.MailArchiveMembershipRole},
		Facets: []contracts.Facet{{
			Kind: contracts.MailArchiveMembershipFacetKind,
			Metadata: map[string]any{
				"rfc_message_id":        message.MessageID,
				"canonical_digest":      string(canonical),
				"body_line_fingerprint": message.BodyLineHash,
				"archive_format":        message.Format,
				"archive_mailbox":       message.Mailbox,
				"archive_source_path":   message.SourcePath,
				"generated_message_id":  message.GeneratedMessage,
				"version_count":         0,
				"max_scale":             contracts.VersionScaleTrivial,
			},
		}},
	})

	return err
}
