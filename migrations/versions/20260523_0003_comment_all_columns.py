# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Alembic revision: comment all project columns.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0003
Revises: 20260523_0002
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0003"
down_revision = "20260523_0002"
branch_labels = None
depends_on = None


TABLES = {
    "alembic_version": "Alembic migration tracking",
    "content_objects": "CAS object catalog",
    "labels": "Gmail label catalog",
    "threads": "Gmail thread catalog",
    "messages": "Gmail message catalog",
    "message_parts": "MIME part catalog",
    "attachments": "attachment catalog",
    "sync_state": "sync state store",
    "priority_rules": "priority sync rule store",
    "graph_triples": "knowledge graph triple store",
    "intelligence_jobs": "asynchronous intelligence job queue",
    "categories": "message category catalog",
    "message_categories": "message category assignment table",
    "category_rules": "manual category rule table",
    "category_overrides": "per-message category override table",
    "learned_category_runs": "learned category discovery history",
    "people_aliases": "contact canonicalization table",
    "embedding_chunks": "pgvector semantic chunk store",
    "ingest_issues": "ingest quarantine and salvage log",
}

SPECIAL = {
    ("messages", "label_ids"): "Gmail label identifiers as a JSONB array, resolved to label names at read time.",
    ("messages", "headers_json"): "Normalized message headers as JSONB.",
    ("attachments", "sha1"): "Attachment API identifier; currently the BLAKE3 CAS digest.",
    ("attachments", "metadata_json"): "Attachment sidecar metadata merged from Gmail, exiftool, and later enrichment.",
    ("embedding_chunks", "embedding"): "pgvector embedding for nearest-neighbor semantic search.",
    ("graph_triples", "source_message_id"): "Message identifier that produced this graph triple when message-scoped.",
}


def upgrade() -> None:
    """Upgrade."""
    bind = op.get_bind()
    for table, purpose in TABLES.items():
        rows = bind.exec_driver_sql(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        for row in rows:
            column = row[0]
            comment = SPECIAL.get((table, column), f"{column} column for the {purpose}.")
            _safe_comment(f"COMMENT ON COLUMN {table}.{column}", comment)


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
