# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Alembic revision: search status and deferred attachment hydration.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0018
Revises: 20260523_0017
Create Date: 2026-05-25
"""

from alembic import op

revision = "20260523_0018"
down_revision = "20260523_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS search_runs (
          search_id TEXT PRIMARY KEY,
          query TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT 'cache',
          status TEXT NOT NULL DEFAULT 'running',
          request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          live_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          message_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
          analysis_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          attachment_hydration_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          phase_timings_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          completed_at TIMESTAMPTZ
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS search_runs_updated_idx ON search_runs(updated_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS search_runs_status_idx ON search_runs(status, updated_at DESC)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS deferred_attachment_hydration (
          id BIGSERIAL PRIMARY KEY,
          message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
          thread_id TEXT,
          part_id TEXT NOT NULL,
          gmail_attachment_id TEXT NOT NULL,
          filename TEXT,
          mime_type TEXT,
          size BIGINT,
          status TEXT NOT NULL DEFAULT 'pending',
          priority INTEGER NOT NULL DEFAULT 100,
          attempts INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 5,
          locked_by TEXT,
          locked_at TIMESTAMPTZ,
          last_error TEXT,
          search_id TEXT REFERENCES search_runs(search_id) ON DELETE SET NULL,
          payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          completed_at TIMESTAMPTZ,
          UNIQUE(message_id, part_id, gmail_attachment_id)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS deferred_attachment_hydration_status_idx
        ON deferred_attachment_hydration(status, priority DESC, created_at)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS deferred_attachment_hydration_message_idx
        ON deferred_attachment_hydration(message_id, status)
        """
    )


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP TABLE IF EXISTS deferred_attachment_hydration")
    op.execute("DROP TABLE IF EXISTS search_runs")
