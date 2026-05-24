# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Alembic revision: ingest issue lookup indexes.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0005
Revises: 20260523_0004
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0005"
down_revision = "20260523_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute("CREATE INDEX IF NOT EXISTS ingest_issues_message_idx ON ingest_issues(message_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ingest_issues_created_idx ON ingest_issues(created_at DESC)")
    op.execute("COMMENT ON INDEX ingest_issues_message_idx IS 'Accelerates lookup of quarantine and salvage issues by message identifier.'")
    op.execute("COMMENT ON INDEX ingest_issues_created_idx IS 'Accelerates newest-first ingest issue reporting.'")


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP INDEX IF EXISTS ingest_issues_created_idx")
    op.execute("DROP INDEX IF EXISTS ingest_issues_message_idx")
