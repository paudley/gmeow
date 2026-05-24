# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""complete sql object comments.

Revision ID: 20260523_0002
Revises: 20260523_0001
Create Date: 2026-05-23
"""

from __future__ import annotations

from alembic import op

revision = "20260523_0002"
down_revision = "20260523_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    comments = {
        "TABLE alembic_version": "Alembic schema migration version marker.",
        "INDEX alembic_version_pkc": "Primary key index for Alembic version marker.",
        "INDEX attachments_pkey": "Primary key index for attachment CAS identifiers.",
        "INDEX categories_pkey": "Primary key index for category identifiers.",
        "INDEX category_overrides_pkey": "Primary key index for per-message category overrides.",
        "INDEX category_rules_pkey": "Primary key index for category rule identifiers.",
        "INDEX content_objects_pkey": "Primary key index for CAS object digests.",
        "INDEX embedding_chunks_pkey": "Primary key index for semantic embedding chunk identifiers.",
        "INDEX graph_triples_subject_predicate_object_source_message_id_key": (
            "Uniqueness index preventing duplicate graph triples from the same source message."
        ),
        "INDEX ingest_issues_pkey": "Primary key index for ingest issue identifiers.",
        "INDEX intelligence_jobs_kind_target_id_key": "Uniqueness index ensuring one enrichment job per target.",
        "INDEX intelligence_jobs_pkey": "Primary key index for enrichment job identifiers.",
        "INDEX labels_pkey": "Primary key index for Gmail label identifiers.",
        "INDEX learned_category_runs_pkey": "Primary key index for learned category run identifiers.",
        "INDEX message_categories_pkey": "Primary key index for message category assignments.",
        "INDEX message_parts_pkey": "Primary key index for MIME parts within a message.",
        "INDEX messages_pkey": "Primary key index for Gmail message identifiers.",
        "INDEX people_aliases_pkey": "Primary key index for canonical contact aliases.",
        "INDEX priority_rules_pkey": "Primary key index for priority sync rules.",
        "INDEX sync_state_pkey": "Primary key index for sync state keys.",
        "INDEX threads_pkey": "Primary key index for Gmail thread identifiers.",
        "CONSTRAINT attachments_pkey ON attachments": "Primary key constraint for attachment CAS identifiers.",
        "CONSTRAINT categories_pkey ON categories": "Primary key constraint for categories.",
        "CONSTRAINT category_overrides_pkey ON category_overrides": "Primary key constraint for category overrides.",
        "CONSTRAINT category_rules_pkey ON category_rules": "Primary key constraint for category rules.",
        "CONSTRAINT content_objects_pkey ON content_objects": "Primary key constraint for CAS objects.",
        "CONSTRAINT embedding_chunks_pkey ON embedding_chunks": "Primary key constraint for semantic chunks.",
        "CONSTRAINT ingest_issues_pkey ON ingest_issues": "Primary key constraint for ingest issue records.",
        "CONSTRAINT intelligence_jobs_kind_target_id_key ON intelligence_jobs": "Unique constraint for one job per target.",
        "CONSTRAINT labels_pkey ON labels": "Primary key constraint for Gmail labels.",
        "CONSTRAINT messages_pkey ON messages": "Primary key constraint for Gmail messages.",
        "CONSTRAINT threads_pkey ON threads": "Primary key constraint for Gmail threads.",
    }
    for object_name, comment in comments.items():
        _safe_comment(f"COMMENT ON {object_name}", comment)


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
