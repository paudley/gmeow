# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Alembic revision: graph node profiles.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0010
Revises: 20260523_0009
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0010"
down_revision = "20260523_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS graph_node_profiles (
          node TEXT PRIMARY KEY,
          label TEXT NOT NULL,
          kind TEXT NOT NULL,
          namespace TEXT,
          visibility TEXT NOT NULL,
          role TEXT NOT NULL,
          noise_reason TEXT,
          degree BIGINT NOT NULL DEFAULT 0,
          message_count BIGINT NOT NULL DEFAULT 0,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS graph_node_profiles_kind_idx ON graph_node_profiles(kind)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_node_profiles_namespace_idx ON graph_node_profiles(namespace)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_node_profiles_visibility_idx ON graph_node_profiles(visibility)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_node_profiles_messages_idx ON graph_node_profiles(message_count DESC, degree DESC)")
    _comments(
        {
            "TABLE graph_node_profiles": "Materialized graph node profile metadata used for filtered graph discovery and diagnostics.",
            "COLUMN graph_node_profiles.node": "Stable graph node identifier.",
            "COLUMN graph_node_profiles.label": "Human-readable graph node label.",
            "COLUMN graph_node_profiles.kind": "Normalized graph node kind such as addresses, projects, orgs, labels, or ontology_classes.",
            "COLUMN graph_node_profiles.namespace": "Ontology namespace or local graph namespace when known.",
            "COLUMN graph_node_profiles.visibility": "Default graph discovery visibility: user, structural, or noise.",
            "COLUMN graph_node_profiles.role": "Semantic role assigned by the node profiler.",
            "COLUMN graph_node_profiles.noise_reason": "Reason a node is classified as noise or structural when applicable.",
            "COLUMN graph_node_profiles.degree": "Current endpoint occurrence count across graph triples.",
            "COLUMN graph_node_profiles.message_count": "Distinct source-message count connected to the node.",
            "COLUMN graph_node_profiles.updated_at": "Last profile materialization timestamp.",
            "INDEX graph_node_profiles_pkey": "Primary key index for graph node profile lookup.",
            "INDEX graph_node_profiles_kind_idx": "Accelerates graph node profile kind filtering.",
            "INDEX graph_node_profiles_namespace_idx": "Accelerates graph node profile namespace filtering.",
            "INDEX graph_node_profiles_visibility_idx": "Accelerates graph node profile visibility filtering.",
            "INDEX graph_node_profiles_messages_idx": "Accelerates top-node discovery by message and degree counts.",
        }
    )


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP TABLE IF EXISTS graph_node_profiles")


def _comments(values: dict[str, str]) -> None:
    for target, comment in values.items():
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON {target} IS '{escaped}'")
