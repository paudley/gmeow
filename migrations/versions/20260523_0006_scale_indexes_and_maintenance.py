# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""scale indexes and maintenance metadata

Revision ID: 20260523_0006
Revises: 20260523_0005
Create Date: 2026-05-23
"""
from __future__ import annotations

from alembic import op

revision = "20260523_0006"
down_revision = "20260523_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE embedding_chunks ADD COLUMN IF NOT EXISTS embedding_dim INTEGER")
    op.execute("UPDATE embedding_chunks SET embedding_dim = vector_dims(embedding) WHERE embedding IS NOT NULL AND embedding_dim IS NULL")
    op.execute("COMMENT ON COLUMN embedding_chunks.embedding_dim IS 'Embedding vector dimensionality captured at index time for partial ANN indexes.'")
    op.execute("CREATE INDEX IF NOT EXISTS messages_history_id_idx ON messages(history_id)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_updated_at_idx ON messages(updated_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_labels_gin_idx ON messages USING gin(label_ids)")
    op.execute("CREATE INDEX IF NOT EXISTS categories_hidden_enabled_idx ON categories(default_hidden, enabled)")
    op.execute("CREATE INDEX IF NOT EXISTS message_categories_category_idx ON message_categories(category)")
    op.execute("CREATE INDEX IF NOT EXISTS attachments_digest_idx ON attachments(digest)")
    op.execute("CREATE INDEX IF NOT EXISTS content_objects_media_type_idx ON content_objects(media_type)")
    op.execute("CREATE INDEX IF NOT EXISTS content_objects_created_at_idx ON content_objects(created_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS embedding_chunks_message_idx ON embedding_chunks(message_id)")
    op.execute("CREATE INDEX IF NOT EXISTS embedding_chunks_dim_idx ON embedding_chunks(embedding_dim)")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS embedding_chunks_embedding_768_hnsw_idx
        ON embedding_chunks
        USING hnsw ((embedding::vector(768)) vector_cosine_ops)
        WHERE embedding IS NOT NULL AND embedding_dim = 768
        """
    )
    _comments(
        {
            "messages_history_id_idx": "Accelerates Gmail history cursor discovery.",
            "messages_updated_at_idx": "Accelerates recently updated message scans.",
            "messages_labels_gin_idx": "GIN index for JSONB label containment and key lookup.",
            "categories_hidden_enabled_idx": "Accelerates default hidden-category filtering.",
            "message_categories_category_idx": "Accelerates category membership scans.",
            "attachments_digest_idx": "Accelerates attachment CAS reverse lookup.",
            "content_objects_media_type_idx": "Accelerates CAS media-type accounting.",
            "content_objects_created_at_idx": "Accelerates newest-first CAS maintenance scans.",
            "embedding_chunks_message_idx": "Accelerates semantic chunk lookup by message.",
            "embedding_chunks_dim_idx": "Accelerates vector-dimension scoped maintenance.",
            "embedding_chunks_embedding_768_hnsw_idx": "HNSW cosine ANN index for 768-dimensional Nomic embeddings.",
        }
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS embedding_chunks_embedding_768_hnsw_idx")
    op.execute("DROP INDEX IF EXISTS embedding_chunks_dim_idx")
    op.execute("DROP INDEX IF EXISTS embedding_chunks_message_idx")
    op.execute("DROP INDEX IF EXISTS content_objects_created_at_idx")
    op.execute("DROP INDEX IF EXISTS content_objects_media_type_idx")
    op.execute("DROP INDEX IF EXISTS attachments_digest_idx")
    op.execute("DROP INDEX IF EXISTS message_categories_category_idx")
    op.execute("DROP INDEX IF EXISTS categories_hidden_enabled_idx")
    op.execute("DROP INDEX IF EXISTS messages_labels_gin_idx")
    op.execute("DROP INDEX IF EXISTS messages_updated_at_idx")
    op.execute("DROP INDEX IF EXISTS messages_history_id_idx")
    op.execute("ALTER TABLE embedding_chunks DROP COLUMN IF EXISTS embedding_dim")


def _comments(values: dict[str, str]) -> None:
    for index_name, comment in values.items():
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON INDEX {index_name} IS '{escaped}'")
