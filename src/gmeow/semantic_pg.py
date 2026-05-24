# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

from typing import Any

import psycopg
from pgvector.psycopg import register_vector
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .kg import clean_text_for_kg
from .semantic import NomicEmbeddingClient, chunk_text


class PgSemanticIndex:
    def __init__(self, dsn: str, model_name: str, endpoint: str, chunk_size: int = 384, chunk_overlap: int = 48):
        self.dsn = dsn
        self.model_name = model_name
        self.endpoint = endpoint
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self._embedder = NomicEmbeddingClient(endpoint, model_name)
        self._ensure_ready()

    def _connect(self):
        conn = psycopg.connect(self.dsn, row_factory=dict_row)
        register_vector(conn)
        return conn

    def _ensure_ready(self) -> None:
        with self._connect() as conn:
            conn.execute("SELECT 1 FROM embedding_chunks LIMIT 1")

    def available(self) -> bool:
        try:
            with self._connect() as conn:
                conn.execute("SELECT 1 FROM embedding_chunks LIMIT 1")
            return True
        except Exception:
            return False

    def index_message(self, message_id: str, text: str, metadata: dict[str, Any] | None = None) -> None:
        self._index("message", message_id, clean_text_for_kg(text), metadata={**(metadata or {}), "message_id": message_id}, message_id=message_id)

    def index_attachment(self, sha1: str, text: str, metadata: dict[str, Any] | None = None) -> None:
        self._index("attachment", sha1, clean_text_for_kg(text), metadata=metadata or {}, message_id=(metadata or {}).get("message_id"))

    def _index(self, source_kind: str, source_id: str, text: str, metadata: dict[str, Any], message_id: str | None) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM embedding_chunks WHERE source_kind = %s AND source_id = %s", (source_kind, source_id))
            if not text.strip():
                return
            chunks = chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)
            if not chunks:
                return
            embeddings = self._embedder.embed_documents(chunks)
            rows = []
            for idx, chunk in enumerate(chunks):
                item = dict(metadata)
                item.update({"chunk_index": idx, "chunk_count": len(chunks), "source_kind": source_kind, "source_id": source_id})
                rows.append((f"{source_kind}:{source_id}:{idx}", source_kind, source_id, message_id, idx, len(chunks), chunk, Jsonb(item), embeddings[idx], len(embeddings[idx])))
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO embedding_chunks(id, source_kind, source_id, message_id, chunk_index, chunk_count, text, metadata, embedding, embedding_dim)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO UPDATE SET
                      text=excluded.text,
                      metadata=excluded.metadata,
                      embedding=excluded.embedding,
                      embedding_dim=excluded.embedding_dim,
                      updated_at=now()
                    """,
                    rows,
                )

    def search(self, query: str, limit: int = 10, source_kind: str | None = None) -> list[dict[str, Any]]:
        embedding = self._embedder.embed_query(query)
        where = ["embedding IS NOT NULL"]
        params: list[Any] = [embedding]
        if source_kind:
            where.append("source_kind = %s")
            params.append(source_kind)
        params.extend([embedding, limit])
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, source_kind, source_id, message_id, chunk_index, text AS document, metadata,
                       embedding <=> %s::vector AS distance
                FROM embedding_chunks
                WHERE {" AND ".join(where)}
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]
