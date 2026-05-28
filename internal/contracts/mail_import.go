// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

const (
	MailArchiveSourceKind      = "mail_archive"
	MailIdentitySourceKind     = "mail_identity"
	MailIdentitySourceName     = "rfc_message_id"
	MailGeneratedMessageIDHost = "gmeow.local"
	MailMessageFacetKind       = "mail_message"
	MailVariantFacetKind       = "mail_message_variant"
	MailPatchDiffRole          = "patch_diff"
	MailArchiveMetadataRole    = "archive_data"
	MailMessageIdentityRole    = "mail_identity"
	MailMessageVariantRole     = "mail_variant"
	MailMessageContentRole     = "mail_message"
	MailMessageContainerRole   = "container"
	MailHeadersRole            = "rfc822_headers"
	MailBodyRole               = "email_body"
	MailAttachmentRole         = "attachment"
	MailMIMEStructureRole      = "mime_structure"
	MailVersionSetDomain       = "mail_message"
	MailBodyLineFingerprint    = "body_line"
	MailSemanticFingerprint    = "semantic"
)

type MailIdentityReportRequest struct {
	SourceNames      []string `json:"source_names,omitempty"`
	IncludeGenerated bool     `json:"include_generated,omitempty"`
	CollisionsOnly   bool     `json:"collisions_only,omitempty"`
	Limit            int      `json:"limit,omitempty"`
	Offset           int      `json:"offset,omitempty"`
}

type MailIdentityReportItem struct {
	MessageID       string         `json:"message_id"`
	CanonicalDigest ObjectDigest   `json:"canonical_digest"`
	SourceNames     []string       `json:"source_names,omitempty"`
	ArchiveDigests  []ObjectDigest `json:"archive_digests,omitempty"`
	VariantDigests  []ObjectDigest `json:"variant_digests,omitempty"`
	Generated       bool           `json:"generated"`
	Collision       bool           `json:"collision"`
	MaxScale        string         `json:"max_scale,omitempty"`
	VersionCount    int            `json:"version_count,omitempty"`
}

type MailIdentityReportResponse struct {
	Items  []MailIdentityReportItem `json:"items"`
	Total  int                      `json:"total"`
	Limit  int                      `json:"limit"`
	Offset int                      `json:"offset"`
}
