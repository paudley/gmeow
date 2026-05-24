# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide semantic functionality for Gmeow."""

from __future__ import annotations

from typing import Any

import blake3
import semchunk

from .http_json import HttpJsonError, post_json

MAX_EMBEDDING_TOKEN_LENGTH = 512


def _embedding_index(row: dict[str, Any]) -> int:
    return int(row.get("index", 0))


class NomicEmbeddingClient:
    """Represent NomicEmbeddingClient data and behavior."""

    def __init__(self, endpoint: str, model_name: str, batch_size: int = 64) -> None:
        """Initialize NomicEmbeddingClient."""
        self.endpoint = endpoint
        self.model_name = model_name
        self.batch_size = max(1, batch_size)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed documents."""
        return self._embed([f"search_document: {text}" for text in texts])

    def embed_query(self, text: str) -> list[float]:
        """Embed query."""
        return self._embed([f"search_query: {text}"])[0]

    def _embed(self, texts: list[str]) -> list[list[float]]:
        embeddings = []
        for start in range(0, len(texts), self.batch_size):
            embeddings.extend(self._embed_batch(texts[start : start + self.batch_size]))
        return embeddings

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        try:
            _, data = post_json(self.endpoint, {"model": self.model_name, "input": texts}, timeout=120)
        except HttpJsonError as exc:
            msg = f"embedding server returned {exc}"
            raise RuntimeError(msg) from exc
        if data.get("error"):
            raise RuntimeError(data["error"])
        rows = sorted(data["data"], key=_embedding_index)
        if len(rows) != len(texts):
            msg = f"embedding server returned {len(rows)} embeddings for {len(texts)} inputs"
            raise RuntimeError(msg)
        return [row["embedding"] for row in rows]


def chunk_text(text: str, chunk_size: int = 384, overlap: int = 48) -> list[str]:
    """Chunk text."""
    clean = _sanitize_for_embedding(text)
    if not clean:
        return []

    def token_counter(value: str) -> int:
        """Token counter."""
        return max(len(value.split()), len(value) // 4, 1)

    chunks = semchunk.chunk(clean, chunk_size=chunk_size, token_counter=token_counter, overlap=overlap)
    return [chunk for chunk in chunks if chunk.strip()]


def _sanitize_for_embedding(text: str) -> str:
    tokens = " ".join(text.replace("\x00", " ").split()).split()
    split_tokens: list[str] = []
    for token in tokens:
        if len(token) <= MAX_EMBEDDING_TOKEN_LENGTH:
            split_tokens.append(token)
            continue
        split_tokens.extend(token[index : index + MAX_EMBEDDING_TOKEN_LENGTH] for index in range(0, len(token), MAX_EMBEDDING_TOKEN_LENGTH))
    return " ".join(split_tokens)


class HashSemanticIndex:
    """Represent HashSemanticIndex data and behavior."""

    def __init__(self) -> None:
        """Initialize HashSemanticIndex."""
        self.documents: dict[str, str] = {}

    def index_message(self, message_id: str, text: str, _metadata: dict[str, Any] | None = None) -> None:
        """Index message."""
        self.documents[message_id] = text

    def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search."""
        query_terms = set(query.lower().split())
        scored = []
        for message_id, text in self.documents.items():
            terms = set(text.lower().split())
            overlap = len(query_terms & terms)
            digest = int(blake3.blake3(text.encode()).hexdigest()[:8], 16)
            scored.append((overlap, -digest, message_id, text))
        scored.sort(reverse=True)
        return [{"id": message_id, "score": overlap, "document": text} for overlap, _, message_id, text in scored[:limit] if overlap > 0]
