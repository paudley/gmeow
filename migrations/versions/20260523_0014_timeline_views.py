# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Alembic revision: timeline and emergence views.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0014
Revises: 20260523_0013
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0014"
down_revision = "20260523_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS message_timeline_daily AS
        SELECT date_trunc('day', message_ts)::date AS day,
               COUNT(*) AS messages,
               COUNT(*) FILTER (WHERE hydrated) AS hydrated_messages,
               COUNT(DISTINCT sender_addr) AS senders,
               COUNT(DISTINCT thread_id) AS threads
        FROM messages
        WHERE message_ts IS NOT NULL
        GROUP BY date_trunc('day', message_ts)::date
        WITH NO DATA
        """
    )
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS graph_entity_emergence AS
        SELECT gnp.node,
               gnp.label,
               gnp.kind,
               gnp.visibility,
               MIN(m.message_ts) AS first_seen,
               MAX(m.message_ts) AS last_seen,
               COUNT(DISTINCT gt.source_message_id) AS messages,
               COUNT(*) AS triples
        FROM graph_node_profiles gnp
        JOIN graph_triples gt ON (gt.subject = gnp.node OR gt.object = gnp.node) AND gt.source_message_id IS NOT NULL
        JOIN messages m ON m.id = gt.source_message_id
        WHERE m.message_ts IS NOT NULL
        GROUP BY gnp.node, gnp.label, gnp.kind, gnp.visibility
        WITH NO DATA
        """
    )
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS message_timeline_daily_day_uidx ON message_timeline_daily(day)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS graph_entity_emergence_node_uidx ON graph_entity_emergence(node)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_entity_emergence_first_seen_idx ON graph_entity_emergence(first_seen DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_entity_emergence_last_seen_idx ON graph_entity_emergence(last_seen DESC)")
    _comments(
        {
            "MATERIALIZED VIEW message_timeline_daily": (
                "Daily mailbox activity timeline for operational diagnostics and time-window browsing."
            ),
            "COLUMN message_timeline_daily.day": "UTC day bucket for parsed message timestamps.",
            "COLUMN message_timeline_daily.messages": "Messages received or sent in the day bucket.",
            "COLUMN message_timeline_daily.hydrated_messages": "Hydrated cached messages in the day bucket.",
            "COLUMN message_timeline_daily.senders": "Distinct sender addresses in the day bucket.",
            "COLUMN message_timeline_daily.threads": "Distinct Gmail threads active in the day bucket.",
            "MATERIALIZED VIEW graph_entity_emergence": "Entity first/last-seen view for discovering newly emerging graph nodes.",
            "COLUMN graph_entity_emergence.node": "Graph node identifier.",
            "COLUMN graph_entity_emergence.label": "Human-readable graph node label.",
            "COLUMN graph_entity_emergence.kind": "Materialized graph node kind.",
            "COLUMN graph_entity_emergence.visibility": "Materialized graph node visibility.",
            "COLUMN graph_entity_emergence.first_seen": "Earliest parsed message timestamp linked to the node.",
            "COLUMN graph_entity_emergence.last_seen": "Latest parsed message timestamp linked to the node.",
            "COLUMN graph_entity_emergence.messages": "Distinct linked source-message count.",
            "COLUMN graph_entity_emergence.triples": "Triple evidence count linked to the node.",
            "INDEX message_timeline_daily_day_uidx": "Unique day-bucket index for concurrent timeline refreshes.",
            "INDEX graph_entity_emergence_node_uidx": "Unique node index for concurrent emergence refreshes.",
            "INDEX graph_entity_emergence_first_seen_idx": "Accelerates newly first-seen entity browsing.",
            "INDEX graph_entity_emergence_last_seen_idx": "Accelerates recently active entity browsing.",
        }
    )


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP MATERIALIZED VIEW IF EXISTS graph_entity_emergence")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS message_timeline_daily")


def _comments(values: dict[str, str]) -> None:
    for target, comment in values.items():
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON {target} IS '{escaped}'")
