# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
import json
from typing import Any
from urllib import request
from urllib.error import HTTPError

import semchunk


class NomicEmbeddingClient:
    def __init__(self, endpoint: str, model_name: str, batch_size: int = 64):
        self.endpoint = endpoint
        self.model_name = model_name
        self.batch_size = max(1, batch_size)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return self._embed([f"search_document: {text}" for text in texts])

    def embed_query(self, text: str) -> list[float]:
        return self._embed([f"search_query: {text}"])[0]

    def _embed(self, texts: list[str]) -> list[list[float]]:
        embeddings = []
        for start in range(0, len(texts), self.batch_size):
            embeddings.extend(self._embed_batch(texts[start : start + self.batch_size]))
        return embeddings

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        payload = json.dumps({"model": self.model_name, "input": texts}).encode()
        req = request.Request(self.endpoint, data=payload, headers={"content-type": "application/json"}, method="POST")
        try:
            with request.urlopen(req, timeout=120) as response:
                data = json.loads(response.read().decode())
        except HTTPError as exc:
            body = exc.read().decode(errors="replace")
            raise RuntimeError(f"embedding server returned HTTP {exc.code}: {body[:500]}") from exc
        if data.get("error"):
            raise RuntimeError(data["error"])
        rows = sorted(data["data"], key=lambda item: item.get("index", 0))
        if len(rows) != len(texts):
            raise RuntimeError(f"embedding server returned {len(rows)} embeddings for {len(texts)} inputs")
        return [row["embedding"] for row in rows]


def chunk_text(text: str, chunk_size: int = 384, overlap: int = 48) -> list[str]:
    clean = _sanitize_for_embedding(text)
    if not clean:
        return []
    token_counter = lambda value: max(len(value.split()), len(value) // 4, 1)
    chunks = semchunk.chunk(clean, chunk_size=chunk_size, token_counter=token_counter, overlap=overlap)
    return [chunk for chunk in chunks if chunk.strip()]


def _sanitize_for_embedding(text: str) -> str:
    tokens = " ".join(text.replace("\x00", " ").split()).split()
    split_tokens: list[str] = []
    for token in tokens:
        if len(token) <= 512:
            split_tokens.append(token)
            continue
        split_tokens.extend(token[index : index + 512] for index in range(0, len(token), 512))
    return " ".join(split_tokens)


class HashSemanticIndex:
    def __init__(self):
        self.documents: dict[str, str] = {}

    def index_message(self, message_id: str, text: str, metadata: dict[str, Any] | None = None) -> None:
        self.documents[message_id] = text

    def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        query_terms = set(query.lower().split())
        scored = []
        for message_id, text in self.documents.items():
            terms = set(text.lower().split())
            overlap = len(query_terms & terms)
            digest = int(hashlib.sha1(text.encode()).hexdigest()[:8], 16)
            scored.append((overlap, -digest, message_id, text))
        scored.sort(reverse=True)
        return [{"id": message_id, "score": overlap, "document": text} for overlap, _, message_id, text in scored[:limit] if overlap > 0]
