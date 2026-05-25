# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Alembic revision: message search columns.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0009
Revises: 20260523_0008
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0009"
down_revision = "20260523_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS message_ts TIMESTAMPTZ")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender_addr TEXT")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender_domain TEXT")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS recipient_addrs TEXT[] NOT NULL DEFAULT '{}'::text[]")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS recipient_domains TEXT[] NOT NULL DEFAULT '{}'::text[]")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS search_tsv TSVECTOR")
    op.execute("CREATE INDEX IF NOT EXISTS messages_message_ts_idx ON messages(message_ts DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_sender_addr_idx ON messages(sender_addr)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_sender_domain_idx ON messages(sender_domain)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_recipient_addrs_gin_idx ON messages USING gin(recipient_addrs)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_recipient_domains_gin_idx ON messages USING gin(recipient_domains)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_search_tsv_gin_idx ON messages USING gin(search_tsv)")
    _comments(
        {
            "COLUMN messages.message_ts": "Parsed message timestamp used for indexed date filtering and chronological sorting.",
            "COLUMN messages.sender_addr": "Lower-case parsed sender email address for exact from: filtering.",
            "COLUMN messages.sender_domain": "Lower-case parsed sender domain for domain-level from: filtering.",
            "COLUMN messages.recipient_addrs": "Lower-case parsed recipient email addresses for exact to: filtering.",
            "COLUMN messages.recipient_domains": "Lower-case parsed recipient domains for domain-level to: filtering.",
            "COLUMN messages.search_tsv": "PostgreSQL full-text vector over message lexical search fields.",
            "INDEX messages_message_ts_idx": "Newest-first btree index over parsed message timestamps.",
            "INDEX messages_sender_addr_idx": "Accelerates exact sender address filtering.",
            "INDEX messages_sender_domain_idx": "Accelerates sender domain filtering.",
            "INDEX messages_recipient_addrs_gin_idx": "GIN index for recipient address array containment.",
            "INDEX messages_recipient_domains_gin_idx": "GIN index for recipient domain array containment.",
            "INDEX messages_search_tsv_gin_idx": "GIN index for PostgreSQL full-text message search.",
        }
    )


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP INDEX IF EXISTS messages_search_tsv_gin_idx")
    op.execute("DROP INDEX IF EXISTS messages_recipient_domains_gin_idx")
    op.execute("DROP INDEX IF EXISTS messages_recipient_addrs_gin_idx")
    op.execute("DROP INDEX IF EXISTS messages_sender_domain_idx")
    op.execute("DROP INDEX IF EXISTS messages_sender_addr_idx")
    op.execute("DROP INDEX IF EXISTS messages_message_ts_idx")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS search_tsv")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS recipient_domains")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS recipient_addrs")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS sender_domain")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS sender_addr")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS message_ts")


def _comments(values: dict[str, str]) -> None:
    for target, comment in values.items():
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON {target} IS '{escaped}'")
