# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""comment project constraints.

Revision ID: 20260523_0004
Revises: 20260523_0003
Create Date: 2026-05-23
"""

from __future__ import annotations

from alembic import op

revision = "20260523_0004"
down_revision = "20260523_0003"
branch_labels = None
depends_on = None


PROJECT_TABLES = (
    "alembic_version",
    "content_objects",
    "labels",
    "threads",
    "messages",
    "message_parts",
    "attachments",
    "sync_state",
    "priority_rules",
    "graph_triples",
    "intelligence_jobs",
    "categories",
    "message_categories",
    "category_rules",
    "category_overrides",
    "learned_category_runs",
    "people_aliases",
    "embedding_chunks",
    "ingest_issues",
)


def upgrade() -> None:
    """Upgrade."""
    bind = op.get_bind()
    rows = bind.exec_driver_sql(
        """
        SELECT conrelid::regclass::text AS table_name, conname, contype
        FROM pg_constraint
        WHERE connamespace = 'public'::regnamespace
          AND conrelid::regclass::text = ANY(%s)
        ORDER BY table_name, conname
        """,
        (list(PROJECT_TABLES),),
    )
    for table, name, kind in rows:
        label = {
            "p": "primary key",
            "f": "foreign key",
            "u": "unique",
            "c": "check",
            "x": "exclusion",
        }.get(kind, "database")
        _safe_comment(f"COMMENT ON CONSTRAINT {name} ON {table}", f"{label.title()} constraint {name} on {table}.")


def downgrade() -> None:
    """Downgrade."""


def _safe_comment(prefix: str, comment: str) -> None:
    escaped_comment = comment.replace("'", "''")
    sql = f"{prefix} IS '{escaped_comment}'"
    escaped_sql = sql.replace("'", "''")
    op.execute(
        f"""
        DO $$
        BEGIN
          EXECUTE '{escaped_sql}';
        EXCEPTION
          WHEN insufficient_privilege OR undefined_object THEN
            NULL;
        END $$;
        """
    )
