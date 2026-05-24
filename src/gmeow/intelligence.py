# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import os
import socket
from typing import Any

from .cache import _attachment_text_from_metadata
from .categories import CategoryEngine
from .graph import extract_attachment_sidecar_triples, extract_triples
from .kg import clean_text_for_kg
from .parser import parse_gmail_message


class IntelligenceWorker:
    def __init__(self, cache: Any, semantic: Any, graph: Any | None = None):
        self.cache = cache
        self.semantic = semantic
        self.graph = graph
        self.categories = CategoryEngine(cache)
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}"

    def enqueue_all(self) -> dict[str, int]:
        return self.cache.enqueue_all_intelligence_jobs()

    def run_until_empty(self, limit: int | None = None) -> dict[str, int]:
        processed = 0
        failed = 0
        while limit is None or processed < limit:
            job = self.cache.claim_next_intelligence_job(worker_id=self.worker_id)
            if job is None:
                break
            try:
                self.process_job(job)
            except Exception as exc:
                self.cache.fail_intelligence_job(job["id"], repr(exc))
                failed += 1
            else:
                self.cache.complete_intelligence_job(job["id"])
                processed += 1
        return {"processed": processed, "failed": failed, "remaining": self.cache.intelligence_job_status().get("pending", 0)}

    def process_job(self, job: dict[str, Any]) -> None:
        if job["kind"] == "message":
            self.process_message(job["target_id"])
            return
        if job["kind"] == "attachment":
            self.process_attachment(job["target_id"])
            return
        raise ValueError(f"Unknown intelligence job kind: {job['kind']}")

    def process_message(self, message_id: str) -> None:
        message = self.cache.get_message(message_id)
        if message is None:
            raise KeyError(message_id)
        parsed = parse_gmail_message(message["raw"])
        attachment_sha1s = [attachment["sha1"] for attachment in self.cache.attachments_for_message(message_id)]
        triples = extract_triples(parsed, attachment_sha1s)
        self.cache.delete_graph_triples_for_message(message_id)
        self.cache.add_triples(triples)
        if self.graph is not None:
            self.graph.add_triples(triples)
        self.categories.categorize_message(message_id)
        text = "\n\n".join([message.get("subject") or "", message.get("text_body") or "", message.get("markdown") or ""])
        categories = ",".join(self.cache.get_message(message_id).get("categories", []))
        self.semantic.index_message(message_id, clean_text_for_kg(text), {"thread_id": message.get("thread_id") or "", "categories": categories})

    def process_attachment(self, sha1: str) -> None:
        attachments = [attachment for attachment in self.cache.list_attachments() if attachment["sha1"] == sha1]
        if not attachments:
            raise KeyError(sha1)
        attachment = attachments[0]
        metadata = attachment.get("metadata", {})
        triples = extract_attachment_sidecar_triples(sha1, metadata)
        self.cache.delete_graph_triples_for_attachment(sha1)
        self.cache.add_triples(triples)
        if self.graph is not None:
            self.graph.add_triples(triples)
        text = _attachment_text_from_metadata(metadata) or json.dumps(metadata, sort_keys=True)
        self.semantic.index_attachment(
            sha1,
            text,
            {
                "message_id": attachment["message_id"],
                "filename": attachment.get("filename") or "",
                "mime_type": attachment.get("mime_type") or "",
            },
        )
