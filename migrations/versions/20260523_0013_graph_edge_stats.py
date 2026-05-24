# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""graph edge weights

Revision ID: 20260523_0013
Revises: 20260523_0012
Create Date: 2026-05-23
"""
from __future__ import annotations

from alembic import op

revision = "20260523_0013"
down_revision = "20260523_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS graph_edge_stats (
          subject TEXT NOT NULL,
          predicate TEXT NOT NULL,
          object TEXT NOT NULL,
          evidence_count BIGINT NOT NULL DEFAULT 0,
          message_count BIGINT NOT NULL DEFAULT 0,
          first_message_ts TIMESTAMPTZ,
          last_message_ts TIMESTAMPTZ,
          weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (subject, predicate, object)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS graph_edge_stats_predicate_idx ON graph_edge_stats(predicate)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_edge_stats_weight_idx ON graph_edge_stats(weight)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_edge_stats_last_idx ON graph_edge_stats(last_message_ts DESC)")
    _comments(
        {
            "TABLE graph_edge_stats": "Materialized graph edge evidence counts, timestamps, and traversal weights.",
            "COLUMN graph_edge_stats.subject": "Graph edge subject node.",
            "COLUMN graph_edge_stats.predicate": "Graph edge predicate.",
            "COLUMN graph_edge_stats.object": "Graph edge object node.",
            "COLUMN graph_edge_stats.evidence_count": "Total triple evidence count for this edge.",
            "COLUMN graph_edge_stats.message_count": "Distinct source-message count for this edge.",
            "COLUMN graph_edge_stats.first_message_ts": "Earliest parsed message timestamp supporting this edge.",
            "COLUMN graph_edge_stats.last_message_ts": "Latest parsed message timestamp supporting this edge.",
            "COLUMN graph_edge_stats.weight": "Traversal cost; lower means stronger or more semantically useful edge.",
            "COLUMN graph_edge_stats.updated_at": "Last edge-stat materialization timestamp.",
            "INDEX graph_edge_stats_pkey": "Primary key index for graph edge stats.",
            "INDEX graph_edge_stats_predicate_idx": "Accelerates predicate-scoped graph diagnostics and traversal.",
            "INDEX graph_edge_stats_weight_idx": "Accelerates low-cost edge discovery.",
            "INDEX graph_edge_stats_last_idx": "Accelerates recent-edge timelines.",
        }
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS graph_edge_stats")


def _comments(values: dict[str, str]) -> None:
    for target, comment in values.items():
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON {target} IS '{escaped}'")
