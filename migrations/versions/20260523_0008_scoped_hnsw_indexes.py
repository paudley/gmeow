# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""scoped hnsw indexes

Revision ID: 20260523_0008
Revises: 20260523_0007
Create Date: 2026-05-23
"""
from __future__ import annotations

from alembic import op

revision = "20260523_0008"
down_revision = "20260523_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS embedding_chunks_message_768_hnsw_idx
        ON embedding_chunks
        USING hnsw ((embedding::vector(768)) vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
        WHERE embedding IS NOT NULL AND embedding_dim = 768 AND source_kind = 'message'
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS embedding_chunks_attachment_768_hnsw_idx
        ON embedding_chunks
        USING hnsw ((embedding::vector(768)) vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
        WHERE embedding IS NOT NULL AND embedding_dim = 768 AND source_kind = 'attachment'
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS embedding_chunks_source_kind_dim_idx
        ON embedding_chunks(source_kind, embedding_dim)
        WHERE embedding IS NOT NULL
        """
    )
    _comment("COMMENT ON INDEX embedding_chunks_message_768_hnsw_idx", "Scoped HNSW cosine ANN index for 768-dimensional message embeddings.")
    _comment("COMMENT ON INDEX embedding_chunks_attachment_768_hnsw_idx", "Scoped HNSW cosine ANN index for 768-dimensional attachment embeddings.")
    _comment("COMMENT ON INDEX embedding_chunks_source_kind_dim_idx", "Accelerates source-kind and embedding-dimension filtered semantic scans.")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS embedding_chunks_source_kind_dim_idx")
    op.execute("DROP INDEX IF EXISTS embedding_chunks_attachment_768_hnsw_idx")
    op.execute("DROP INDEX IF EXISTS embedding_chunks_message_768_hnsw_idx")


def _comment(prefix: str, comment: str) -> None:
    escaped = comment.replace("'", "''")
    op.execute(f"{prefix} IS '{escaped}'")
