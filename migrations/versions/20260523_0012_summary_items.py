# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Alembic revision: summary and centroid items.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0012
Revises: 20260523_0011
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0012"
down_revision = "20260523_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS summary_items (
          scope_kind TEXT NOT NULL,
          scope_id TEXT NOT NULL,
          title TEXT NOT NULL,
          summary TEXT NOT NULL,
          message_count BIGINT NOT NULL DEFAULT 0,
          latest_message_ts TIMESTAMPTZ,
          centroid vector,
          centroid_dim INTEGER,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (scope_kind, scope_id)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS summary_items_scope_idx ON summary_items(scope_kind, message_count DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS summary_items_latest_idx ON summary_items(latest_message_ts DESC)")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS summary_items_centroid_768_hnsw_idx
        ON summary_items
        USING hnsw ((centroid::vector(768)) vector_cosine_ops)
        WHERE centroid IS NOT NULL AND centroid_dim = 768
        """
    )
    _comments(
        {
            "TABLE summary_items": (
                "Materialized summaries and centroid embeddings for threads, contacts, projects, labels, categories, and learned clusters."
            ),
            "COLUMN summary_items.scope_kind": "Summary scope type such as thread, contact, project, label, or category.",
            "COLUMN summary_items.scope_id": "Stable identifier within the summary scope.",
            "COLUMN summary_items.title": "Human-readable summary title.",
            "COLUMN summary_items.summary": "Compact deterministic summary text for the scope.",
            "COLUMN summary_items.message_count": "Number of messages contributing to the summary.",
            "COLUMN summary_items.latest_message_ts": "Newest parsed message timestamp contributing to the summary.",
            "COLUMN summary_items.centroid": "Average embedding vector for messages contributing to the summary when available.",
            "COLUMN summary_items.centroid_dim": "Embedding dimensionality of the centroid vector.",
            "COLUMN summary_items.metadata_json": "Scope-specific summary metadata.",
            "COLUMN summary_items.updated_at": "Last summary materialization timestamp.",
            "INDEX summary_items_pkey": "Primary key index for scoped summary lookup.",
            "INDEX summary_items_scope_idx": "Accelerates summary browsing by scope and message count.",
            "INDEX summary_items_latest_idx": "Accelerates recently active summary browsing.",
            "INDEX summary_items_centroid_768_hnsw_idx": "HNSW cosine ANN index for 768-dimensional summary centroids.",
        }
    )


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP TABLE IF EXISTS summary_items")


def _comments(values: dict[str, str]) -> None:
    for target, comment in values.items():
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON {target} IS '{escaped}'")
