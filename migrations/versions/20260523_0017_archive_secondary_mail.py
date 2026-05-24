# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""archive secondary mail foundations

Revision ID: 20260523_0017
Revises: 20260523_0016
Create Date: 2026-05-23
"""

from __future__ import annotations

from alembic import op


revision = "20260523_0017"
down_revision = "20260523_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE content_objects ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ")
    op.execute("ALTER TABLE content_objects ADD COLUMN IF NOT EXISTS verification_status TEXT NOT NULL DEFAULT 'unverified'")
    op.execute("ALTER TABLE content_objects ADD COLUMN IF NOT EXISTS verification_error TEXT")
    op.execute("ALTER TABLE content_objects ADD COLUMN IF NOT EXISTS write_generation BIGINT NOT NULL DEFAULT 1")
    op.execute("ALTER TABLE content_objects ADD COLUMN IF NOT EXISTS metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb")
    op.execute("CREATE INDEX IF NOT EXISTS content_objects_verification_idx ON content_objects(verification_status, verified_at)")

    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS archive_state TEXT NOT NULL DEFAULT 'incomplete'")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS archive_checked_at TIMESTAMPTZ")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS archive_error TEXT")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS tombstoned_at TIMESTAMPTZ")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS purged_at TIMESTAMPTZ")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS deletion_source TEXT")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS deletion_policy TEXT")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS deleted_label_ids JSONB NOT NULL DEFAULT '[]'::jsonb")
    op.execute("CREATE INDEX IF NOT EXISTS messages_archive_state_idx ON messages(archive_state, archive_checked_at)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS retention_policies (
          id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          match_kind TEXT NOT NULL,
          match_value TEXT NOT NULL,
          action TEXT NOT NULL,
          enabled BOOLEAN NOT NULL DEFAULT true,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS retention_policies_enabled_idx ON retention_policies(enabled, match_kind, match_value)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS attachment_metadata_versions (
          id BIGSERIAL PRIMARY KEY,
          digest TEXT NOT NULL,
          metadata_digest TEXT NOT NULL REFERENCES content_objects(digest),
          source TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(digest, metadata_digest)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS attachment_metadata_versions_digest_idx ON attachment_metadata_versions(digest, created_at DESC)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS imap_mailboxes (
          id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          label_id TEXT,
          uidvalidity BIGINT NOT NULL DEFAULT (extract(epoch from now())::bigint),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS imap_message_uids (
          mailbox_id BIGINT NOT NULL REFERENCES imap_mailboxes(id) ON DELETE CASCADE,
          message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
          uid BIGSERIAL NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY(mailbox_id, message_id),
          UNIQUE(mailbox_id, uid)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS imap_message_uids_message_idx ON imap_message_uids(message_id)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS archive_exports (
          id BIGSERIAL PRIMARY KEY,
          export_path TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'running',
          manifest_digest TEXT,
          object_count BIGINT NOT NULL DEFAULT 0,
          message_count BIGINT NOT NULL DEFAULT 0,
          error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          finished_at TIMESTAMPTZ
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS archive_exports_created_idx ON archive_exports(created_at DESC)")

    op.execute(
        """
        INSERT INTO retention_policies(name, match_kind, match_value, action)
        VALUES
          ('spam_purge', 'label', 'SPAM', 'purge'),
          ('trash_purge', 'label', 'TRASH', 'purge')
        ON CONFLICT(name) DO NOTHING
        """
    )
    op.execute(
        """
        UPDATE messages
        SET archive_state = CASE
          WHEN raw_json_digest IS NOT NULL AND raw_rfc822_digest IS NOT NULL THEN 'complete'
          ELSE 'incomplete'
        END,
        archive_checked_at = COALESCE(archive_checked_at, now())
        """
    )
    _comments()


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS archive_exports")
    op.execute("DROP TABLE IF EXISTS imap_message_uids")
    op.execute("DROP TABLE IF EXISTS imap_mailboxes")
    op.execute("DROP TABLE IF EXISTS attachment_metadata_versions")
    op.execute("DROP TABLE IF EXISTS retention_policies")
    op.execute("DROP INDEX IF EXISTS messages_archive_state_idx")
    for column in ["deleted_label_ids", "deletion_policy", "deletion_source", "purged_at", "tombstoned_at", "archive_error", "archive_checked_at", "archive_state"]:
        op.execute(f"ALTER TABLE messages DROP COLUMN IF EXISTS {column}")
    op.execute("DROP INDEX IF EXISTS content_objects_verification_idx")
    for column in ["metadata_json", "write_generation", "verification_error", "verification_status", "verified_at"]:
        op.execute(f"ALTER TABLE content_objects DROP COLUMN IF EXISTS {column}")


def _comments() -> None:
    comments = {
        "COLUMN content_objects.verified_at": "Timestamp when object bytes were last verified against their BLAKE3 digest and compression metadata.",
        "COLUMN content_objects.verification_status": "Object verification state: unverified, ok, missing, corrupt, or error.",
        "COLUMN content_objects.verification_error": "Most recent object verification error detail.",
        "COLUMN content_objects.write_generation": "Monotonic write generation for future replicated object-store reconciliation.",
        "COLUMN content_objects.metadata_json": "Structured object metadata used by archive export and restore workflows.",
        "INDEX content_objects_verification_idx": "Accelerates object verification and repair scans.",
        "COLUMN messages.archive_state": "Archive lifecycle state: incomplete, complete, tombstoned, purge_pending, or purged.",
        "COLUMN messages.archive_checked_at": "Timestamp when archive completeness was last evaluated.",
        "COLUMN messages.archive_error": "Most recent archive completeness or retention error.",
        "COLUMN messages.tombstoned_at": "Timestamp when Gmail deletion was retained locally as a tombstone.",
        "COLUMN messages.purged_at": "Timestamp when local message payloads were purged by retention policy.",
        "COLUMN messages.deletion_source": "Source that reported deletion, such as gmail_history or operator.",
        "COLUMN messages.deletion_policy": "Retention policy action selected when deletion was processed.",
        "COLUMN messages.deleted_label_ids": "Label ids present when deletion policy was evaluated.",
        "INDEX messages_archive_state_idx": "Accelerates archive completeness and retention work queues.",
        "TABLE retention_policies": "Label/category-based deletion retention policy table.",
        "COLUMN retention_policies.id": "Primary key for a retention policy.",
        "COLUMN retention_policies.name": "Stable unique policy name.",
        "COLUMN retention_policies.match_kind": "Policy matcher kind, currently label or category.",
        "COLUMN retention_policies.match_value": "Label id/name or category value matched by the policy.",
        "COLUMN retention_policies.action": "Retention action, currently tombstone or purge.",
        "COLUMN retention_policies.enabled": "Whether the policy is active.",
        "COLUMN retention_policies.created_at": "Policy creation timestamp.",
        "COLUMN retention_policies.updated_at": "Policy update timestamp.",
        "INDEX retention_policies_enabled_idx": "Accelerates active retention policy lookup.",
        "TABLE attachment_metadata_versions": "CAS-backed immutable versions of attachment metadata sidecars.",
        "COLUMN attachment_metadata_versions.id": "Primary key for a metadata version.",
        "COLUMN attachment_metadata_versions.digest": "Attachment payload digest the metadata describes.",
        "COLUMN attachment_metadata_versions.metadata_digest": "CAS digest of the metadata JSON object.",
        "COLUMN attachment_metadata_versions.source": "Process or subsystem that wrote this metadata version.",
        "COLUMN attachment_metadata_versions.created_at": "Metadata version creation timestamp.",
        "INDEX attachment_metadata_versions_digest_idx": "Accelerates newest metadata lookup for an attachment digest.",
        "TABLE imap_mailboxes": "Read-only IMAP mailbox catalog derived from Gmail labels.",
        "COLUMN imap_mailboxes.id": "Primary key for an IMAP mailbox.",
        "COLUMN imap_mailboxes.name": "IMAP mailbox name exposed to clients.",
        "COLUMN imap_mailboxes.label_id": "Gmail label id backing this mailbox, null for synthetic mailboxes.",
        "COLUMN imap_mailboxes.uidvalidity": "Stable IMAP UIDVALIDITY value for this mailbox.",
        "COLUMN imap_mailboxes.created_at": "Mailbox creation timestamp.",
        "COLUMN imap_mailboxes.updated_at": "Mailbox update timestamp.",
        "TABLE imap_message_uids": "Stable per-mailbox IMAP UID assignments for messages.",
        "COLUMN imap_message_uids.mailbox_id": "IMAP mailbox that owns this UID assignment.",
        "COLUMN imap_message_uids.message_id": "Message assigned to this mailbox.",
        "COLUMN imap_message_uids.uid": "Stable IMAP UID within the mailbox.",
        "COLUMN imap_message_uids.created_at": "UID assignment creation timestamp.",
        "INDEX imap_message_uids_message_idx": "Accelerates lookup of all IMAP folders containing a message.",
        "TABLE archive_exports": "Archive export run records and manifest references.",
        "COLUMN archive_exports.id": "Primary key for an archive export run.",
        "COLUMN archive_exports.export_path": "Filesystem destination for the archive export.",
        "COLUMN archive_exports.status": "Export status: running, complete, or failed.",
        "COLUMN archive_exports.manifest_digest": "BLAKE3 digest of the export manifest JSON.",
        "COLUMN archive_exports.object_count": "Number of content objects included in the export manifest.",
        "COLUMN archive_exports.message_count": "Number of messages included in the export manifest.",
        "COLUMN archive_exports.error": "Failure detail when the export fails.",
        "COLUMN archive_exports.created_at": "Export start timestamp.",
        "COLUMN archive_exports.finished_at": "Export completion timestamp.",
        "INDEX archive_exports_created_idx": "Accelerates newest-first archive export status queries.",
        "CONSTRAINT retention_policies_name_key ON retention_policies": "Ensures retention policy names are stable and unique.",
        "CONSTRAINT attachment_metadata_versions_digest_metadata_digest_key ON attachment_metadata_versions": "Prevents duplicate metadata version rows for the same attachment and metadata object.",
        "CONSTRAINT imap_mailboxes_name_key ON imap_mailboxes": "Ensures IMAP mailbox names are unique.",
        "CONSTRAINT imap_message_uids_pkey ON imap_message_uids": "Primary key for mailbox/message UID assignments.",
        "CONSTRAINT imap_message_uids_mailbox_id_uid_key ON imap_message_uids": "Ensures IMAP UIDs are unique within a mailbox.",
    }
    for target, text in comments.items():
        op.execute(f"COMMENT ON {target} IS '{text}'")
