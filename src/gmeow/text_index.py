# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Maintain the Tantivy text index for cached messages.

The index complements the PostgreSQL full-text search by giving sync and MCP callers a faster,
in-process surface for exact and proximity queries. Documents are written on hydration and removed
when the cache retires a message.
"""

from pathlib import Path
from typing import Any

import tantivy


class TantivyMessageIndex:
    """Represent TantivyMessageIndex data and behavior."""

    def __init__(self, path: Path) -> None:
        """Initialize TantivyMessageIndex."""
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)
        self.schema = _schema()
        if tantivy.Index.exists(str(self.path)):
            self.index = tantivy.Index.open(str(self.path))
        else:
            self.index = tantivy.Index(self.schema, path=str(self.path))

    def upsert_message(self, message: dict[str, Any]) -> None:
        """Upsert message."""
        writer = self.index.writer()
        writer.delete_documents("id", message["id"])
        writer.add_document(
            tantivy.Document.from_dict(
                {
                    "id": [message["id"]],
                    "subject": [message.get("subject") or ""],
                    "sender": [message.get("sender") or ""],
                    "recipients": [message.get("recipients") or ""],
                    "snippet": [message.get("snippet") or ""],
                    "body": [message.get("text_body") or ""],
                    "markdown": [message.get("markdown") or ""],
                    "headers": [message.get("headers_text") or ""],
                    "labels": [" ".join(message.get("label_ids") or [])],
                    "categories": [" ".join(message.get("categories") or [])],
                }
            )
        )
        writer.commit()
        writer.wait_merging_threads()
        self.index.reload()

    def search(self, query: str, limit: int = 20) -> list[str]:
        """Search."""
        searcher = self.index.searcher()
        parsed, _errors = self.index.parse_query_lenient(
            query or "*",
            ["subject", "sender", "recipients", "snippet", "body", "markdown", "headers", "labels", "categories"],
        )
        hits = searcher.search(parsed, limit).hits
        ids: list[str] = []
        for _, address in hits:
            doc = searcher.doc(address)
            value = doc.get_first("id")
            if value is not None:
                ids.append(str(value))
        return ids


def _schema() -> tantivy.Schema:
    builder = tantivy.SchemaBuilder()
    builder.add_text_field("id", stored=True)
    for name in ["subject", "sender", "recipients", "snippet", "body", "markdown", "headers", "labels", "categories"]:
        builder.add_text_field(name, stored=False)
    return builder.build()
