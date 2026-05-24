# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Alembic revision: content object references.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0011
Revises: 20260523_0010
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0011"
down_revision = "20260523_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS content_object_refs (
          digest TEXT NOT NULL REFERENCES content_objects(digest) ON DELETE CASCADE,
          ref_table TEXT NOT NULL,
          ref_pk TEXT NOT NULL,
          ref_column TEXT NOT NULL,
          ref_kind TEXT NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (digest, ref_table, ref_pk, ref_column)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS content_object_refs_digest_idx ON content_object_refs(digest)")
    op.execute("CREATE INDEX IF NOT EXISTS content_object_refs_source_idx ON content_object_refs(ref_table, ref_pk)")
    op.execute("CREATE INDEX IF NOT EXISTS content_object_refs_kind_idx ON content_object_refs(ref_kind)")
    _comments(
        {
            "TABLE content_object_refs": (
                "Materialized references from catalog rows to CAS content objects for retention and audit diagnostics."
            ),
            "COLUMN content_object_refs.digest": "Referenced CAS object digest.",
            "COLUMN content_object_refs.ref_table": "Catalog table that owns this reference.",
            "COLUMN content_object_refs.ref_pk": "Text representation of the owning row primary key.",
            "COLUMN content_object_refs.ref_column": "Owning row column that stores the digest.",
            "COLUMN content_object_refs.ref_kind": (
                "Logical payload type such as raw_json, text_body, markdown, attachment, part_body, or ingest_artifact."
            ),
            "COLUMN content_object_refs.updated_at": "Last reference materialization timestamp.",
            "INDEX content_object_refs_pkey": "Primary key index for unique row-to-CAS references.",
            "INDEX content_object_refs_digest_idx": "Accelerates CAS reverse-reference lookup.",
            "INDEX content_object_refs_source_idx": "Accelerates owner-row reference audits.",
            "INDEX content_object_refs_kind_idx": "Accelerates payload-kind storage diagnostics.",
        }
    )


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP TABLE IF EXISTS content_object_refs")


def _comments(values: dict[str, str]) -> None:
    for target, comment in values.items():
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON {target} IS '{escaped}'")
