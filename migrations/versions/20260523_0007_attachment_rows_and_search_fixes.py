# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Alembic revision: attachment row identity.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0007
Revises: 20260523_0006
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0007"
down_revision = "20260523_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute("ALTER TABLE attachments DROP CONSTRAINT IF EXISTS attachments_pkey")
    op.execute("ALTER TABLE attachments ADD COLUMN IF NOT EXISTS id BIGSERIAL")
    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'attachments_pkey'
              AND conrelid = 'attachments'::regclass
          ) THEN
            ALTER TABLE attachments ADD CONSTRAINT attachments_pkey PRIMARY KEY (id);
          END IF;
        END $$
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS attachments_sha1_idx ON attachments(sha1)")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS attachments_message_part_sha1_uidx
        ON attachments(message_id, COALESCE(part_id, ''), sha1)
        """
    )
    _comment("COMMENT ON TABLE attachments", "Per-message attachment metadata rows. Payload bytes are deduplicated through the CAS digest.")
    _comment("COMMENT ON COLUMN attachments.id", "Surrogate row identifier for one message attachment part.")
    _comment(
        "COMMENT ON COLUMN attachments.sha1",
        "Attachment payload content identifier used by attachment APIs and CAS lookup; not unique by itself.",
    )
    _comment("COMMENT ON COLUMN attachments.digest", "BLAKE3 CAS digest for attachment payload bytes.")
    _comment("COMMENT ON COLUMN attachments.message_id", "Message that owns this attachment reference.")
    _comment("COMMENT ON COLUMN attachments.part_id", "Gmail MIME part identifier for this message-local attachment reference.")
    _comment("COMMENT ON INDEX attachments_pkey", "Primary key index for per-message attachment metadata rows.")
    _comment("COMMENT ON INDEX attachments_sha1_idx", "Accelerates lookup of all message references for a deduplicated attachment payload.")
    _comment(
        "COMMENT ON INDEX attachments_message_part_sha1_uidx",
        "Prevents duplicate metadata rows for the same message part and payload digest.",
    )
    _comment("COMMENT ON CONSTRAINT attachments_pkey ON attachments", "Primary key constraint for per-message attachment metadata rows.")


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP INDEX IF EXISTS attachments_message_part_sha1_uidx")
    op.execute("DROP INDEX IF EXISTS attachments_sha1_idx")


def _comment(prefix: str, comment: str) -> None:
    escaped = comment.replace("'", "''")
    op.execute(f"{prefix} IS '{escaped}'")
