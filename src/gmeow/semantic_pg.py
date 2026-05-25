# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Persist semantic embeddings in PostgreSQL with pgvector.

The PgSemanticIndex manages embedding chunks, similarity search, and metadata lookups so MCP and
HTTP search surfaces can blend lexical and semantic signals. It also encapsulates the connection
setup required to register the pgvector adapter on every Postgres session.
"""

from typing import Any, cast

import psycopg
from pgvector.psycopg import register_vector
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .kg import clean_text_for_kg
from .semantic import NomicEmbeddingClient, chunk_text

DEFAULT_DICT_ANY = cast(dict[str, Any], None)
DEFAULT_STR = cast(str, None)


class PgSemanticIndex:
    """Represent PgSemanticIndex data and behavior."""

    def __init__(self, dsn: str, model_name: str, endpoint: str, chunk_size: int = 384, chunk_overlap: int = 48) -> None:
        """Initialize PgSemanticIndex."""
        self.dsn = dsn
        self.model_name = model_name
        self.endpoint = endpoint
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self._embedder = NomicEmbeddingClient(endpoint, model_name)
        self._ensure_ready()

    def _connect(self) -> psycopg.Connection[dict[str, object]]:
        conn = psycopg.connect(self.dsn, row_factory=dict_row)
        register_vector(conn)
        return conn

    def _ensure_ready(self) -> None:
        connection = self._connect()
        with connection:
            connection.execute("SELECT 1 FROM embedding_chunks LIMIT 1")

    def available(self) -> bool:
        """Available."""
        try:
            connection = self._connect()
            with connection:
                connection.execute("SELECT 1 FROM embedding_chunks LIMIT 1")
        except psycopg.Error:
            return False
        return True

    def index_message(self, message_id: str, text: str, metadata: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        """Index message."""
        self._index(
            "message", message_id, clean_text_for_kg(text), metadata={**(metadata or {}), "message_id": message_id}, message_id=message_id
        )

    def index_attachment(self, sha1: str, text: str, metadata: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        """Index attachment."""
        self._index(
            "attachment", sha1, clean_text_for_kg(text), metadata=metadata or {}, message_id=str((metadata or {}).get("message_id") or "")
        )

    def _index(self, source_kind: str, source_id: str, text: str, metadata: dict[str, Any], message_id: str = DEFAULT_STR) -> None:
        connection = self._connect()
        with connection:
            connection.execute("DELETE FROM embedding_chunks WHERE source_kind = %s AND source_id = %s", (source_kind, source_id))
            if not text.strip():
                return
            chunks = chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)
            if not chunks:
                return
            embeddings = self._embedder.embed_documents(chunks)
            rows: list[tuple[str, str, str, str, int, int, str, Jsonb, list[float], int]] = []
            for idx, chunk in enumerate(chunks):
                item = dict(metadata)
                item.update({"chunk_index": idx, "chunk_count": len(chunks), "source_kind": source_kind, "source_id": source_id})
                rows.append(
                    (
                        f"{source_kind}:{source_id}:{idx}",
                        source_kind,
                        source_id,
                        message_id,
                        idx,
                        len(chunks),
                        chunk,
                        Jsonb(item),
                        embeddings[idx],
                        len(embeddings[idx]),
                    )
                )
            with connection.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO embedding_chunks(
                        id, source_kind, source_id, message_id, chunk_index, chunk_count,
                        text, metadata, embedding, embedding_dim
                    )
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

    def search(self, query: str, limit: int = 10, source_kind: str = DEFAULT_STR) -> list[dict[str, Any]]:
        """Search."""
        embedding = self._embedder.embed_query(query)
        where = ["embedding IS NOT NULL"]
        params: list[Any] = [embedding]
        if source_kind:
            where.append("source_kind = %s")
            params.append(source_kind)
        params.extend([embedding, limit])
        connection = self._connect()
        with connection:
            rows = connection.execute(
                sql.SQL(
                    """
                SELECT id, source_kind, source_id, message_id, chunk_index, text AS document, metadata,
                       embedding <=> %s::vector AS distance
                FROM embedding_chunks
                WHERE {where_sql}
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """
                ).format(where_sql=sql.SQL(" AND ").join(sql.SQL(condition) for condition in where)),
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]
