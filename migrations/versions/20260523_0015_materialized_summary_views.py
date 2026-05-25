# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Alembic revision: materialized summary views.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0015
Revises: 20260523_0014
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0015"
down_revision = "20260523_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS message_search_summary AS
        SELECT m.id,
               m.thread_id,
               m.subject,
               m.sender,
               m.sender_addr,
               m.sender_domain,
               m.recipient_addrs,
               m.message_ts,
               m.snippet,
               m.hydrated,
               COALESCE(jsonb_agg(DISTINCT mc.category) FILTER (WHERE mc.category IS NOT NULL), '[]'::jsonb) AS categories
        FROM messages m
        LEFT JOIN message_categories mc ON mc.message_id = m.id
        GROUP BY m.id
        WITH NO DATA
        """
    )
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS thread_summary AS
        SELECT COALESCE(thread_id, id) AS thread_id,
               COUNT(*) AS messages,
               MIN(message_ts) AS first_message_ts,
               MAX(message_ts) AS latest_message_ts,
               MAX(subject) AS title
        FROM messages
        GROUP BY COALESCE(thread_id, id)
        WITH NO DATA
        """
    )
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS contact_summary AS
        SELECT sender_addr AS address,
               MAX(sender) AS display_name,
               MAX(sender_domain) AS domain,
               COUNT(*) AS messages,
               MAX(message_ts) AS latest_message_ts
        FROM messages
        WHERE sender_addr IS NOT NULL
        GROUP BY sender_addr
        WITH NO DATA
        """
    )
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS category_summary AS
        SELECT mc.category,
               MAX(c.name) AS name,
               COALESCE(bool_or(c.default_hidden), false) AS default_hidden,
               COUNT(DISTINCT mc.message_id) AS messages,
               MAX(m.message_ts) AS latest_message_ts
        FROM message_categories mc
        JOIN messages m ON m.id = mc.message_id
        LEFT JOIN categories c ON c.id = mc.category
        GROUP BY mc.category
        WITH NO DATA
        """
    )
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS project_summary AS
        SELECT scope_id AS project,
               title,
               message_count AS messages,
               latest_message_ts,
               metadata_json
        FROM summary_items
        WHERE scope_kind = 'project'
        WITH NO DATA
        """
    )
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS graph_node_summary AS
        SELECT node, label, kind, namespace, visibility, role, noise_reason, degree, message_count, updated_at
        FROM graph_node_profiles
        WITH NO DATA
        """
    )
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS message_search_summary_id_uidx ON message_search_summary(id)")
    op.execute("CREATE INDEX IF NOT EXISTS message_search_summary_ts_idx ON message_search_summary(message_ts DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS message_search_summary_sender_idx ON message_search_summary(sender_addr)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS thread_summary_id_uidx ON thread_summary(thread_id)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS contact_summary_address_uidx ON contact_summary(address)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS category_summary_category_uidx ON category_summary(category)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS project_summary_project_uidx ON project_summary(project)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS graph_node_summary_node_uidx ON graph_node_summary(node)")
    _comments(
        {
            "MATERIALIZED VIEW message_search_summary": "Refreshable compact message metadata view for search result composition.",
            "MATERIALIZED VIEW thread_summary": "Refreshable thread activity summary view.",
            "MATERIALIZED VIEW contact_summary": "Refreshable sender/contact summary view.",
            "MATERIALIZED VIEW category_summary": "Refreshable category activity summary view.",
            "MATERIALIZED VIEW project_summary": "Refreshable DOAP/project summary view.",
            "MATERIALIZED VIEW graph_node_summary": "Refreshable graph node statistics summary view.",
            "COLUMN message_search_summary.id": "Gmail message identifier.",
            "COLUMN message_search_summary.thread_id": "Gmail thread identifier.",
            "COLUMN message_search_summary.subject": "Message subject.",
            "COLUMN message_search_summary.sender": "Original sender header.",
            "COLUMN message_search_summary.sender_addr": "Parsed lower-case sender address.",
            "COLUMN message_search_summary.sender_domain": "Parsed lower-case sender domain.",
            "COLUMN message_search_summary.recipient_addrs": "Parsed lower-case recipient addresses.",
            "COLUMN message_search_summary.message_ts": "Parsed message timestamp.",
            "COLUMN message_search_summary.snippet": "Gmail message snippet.",
            "COLUMN message_search_summary.hydrated": "Whether the message has full cached content.",
            "COLUMN message_search_summary.categories": "Materialized message category identifiers.",
            "COLUMN thread_summary.thread_id": "Gmail thread identifier.",
            "COLUMN thread_summary.messages": "Message count in the thread.",
            "COLUMN thread_summary.first_message_ts": "Earliest parsed message timestamp in the thread.",
            "COLUMN thread_summary.latest_message_ts": "Latest parsed message timestamp in the thread.",
            "COLUMN thread_summary.title": "Representative thread title.",
            "COLUMN contact_summary.address": "Sender email address.",
            "COLUMN contact_summary.display_name": "Representative sender display name.",
            "COLUMN contact_summary.domain": "Sender email domain.",
            "COLUMN contact_summary.messages": "Messages from this sender.",
            "COLUMN contact_summary.latest_message_ts": "Latest parsed message timestamp from this sender.",
            "COLUMN category_summary.category": "Category identifier.",
            "COLUMN category_summary.name": "Human-readable category name.",
            "COLUMN category_summary.default_hidden": "Whether the category is hidden by default.",
            "COLUMN category_summary.messages": "Messages assigned to this category.",
            "COLUMN category_summary.latest_message_ts": "Latest parsed message timestamp in this category.",
            "COLUMN project_summary.project": "Project graph node identifier.",
            "COLUMN project_summary.title": "Project title.",
            "COLUMN project_summary.messages": "Messages linked to this project.",
            "COLUMN project_summary.latest_message_ts": "Latest parsed message timestamp linked to this project.",
            "COLUMN project_summary.metadata_json": "Project summary metadata.",
            "COLUMN graph_node_summary.node": "Graph node identifier.",
            "COLUMN graph_node_summary.label": "Graph node label.",
            "COLUMN graph_node_summary.kind": "Graph node kind.",
            "COLUMN graph_node_summary.namespace": "Graph node namespace.",
            "COLUMN graph_node_summary.visibility": "Graph node visibility.",
            "COLUMN graph_node_summary.role": "Graph node semantic role.",
            "COLUMN graph_node_summary.noise_reason": "Graph node noise or structural reason.",
            "COLUMN graph_node_summary.degree": "Graph node endpoint degree.",
            "COLUMN graph_node_summary.message_count": "Distinct message count linked to graph node.",
            "COLUMN graph_node_summary.updated_at": "Last graph node profile update time.",
            "INDEX message_search_summary_id_uidx": "Unique message id index for concurrent message search summary refresh.",
            "INDEX message_search_summary_ts_idx": "Newest-first message search summary index.",
            "INDEX message_search_summary_sender_idx": "Sender-address message search summary index.",
            "INDEX thread_summary_id_uidx": "Unique thread summary index for concurrent refresh.",
            "INDEX contact_summary_address_uidx": "Unique contact summary index for concurrent refresh.",
            "INDEX category_summary_category_uidx": "Unique category summary index for concurrent refresh.",
            "INDEX project_summary_project_uidx": "Unique project summary index for concurrent refresh.",
            "INDEX graph_node_summary_node_uidx": "Unique graph node summary index for concurrent refresh.",
        }
    )


def downgrade() -> None:
    """Downgrade."""
    for view in [
        "graph_node_summary",
        "project_summary",
        "category_summary",
        "contact_summary",
        "thread_summary",
        "message_search_summary",
    ]:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view}")


def _comments(values: dict[str, str]) -> None:
    for target, comment in values.items():
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON {target} IS '{escaped}'")
