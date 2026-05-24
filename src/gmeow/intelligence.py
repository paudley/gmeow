# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Run intelligence jobs for cached Gmail artifacts.

The module processes message and attachment jobs, updating semantic indexes, graph triples,
summaries, and category assignments. It owns worker retry behavior around individual analysis
failures.
"""

import json
import os
import socket
from typing import Any, Protocol, cast

from .cache import attachment_text_from_metadata
from .categories import CategoryEngine
from .graph import extract_attachment_sidecar_triples, extract_triples
from .kg import clean_text_for_kg
from .parser import parse_gmail_message

GraphTriple = tuple[str, str, str, Any]
DEFAULT_TARGETS = cast(list[tuple[str, str]], None)

INTELLIGENCE_EXCEPTIONS = (RuntimeError, ValueError, KeyError, TypeError, OSError)


class _IntelligenceCache(Protocol):
    """Provide cache operations required by the intelligence worker."""

    def enqueue_all_intelligence_jobs(self) -> dict[str, int]:
        """Enqueue every eligible cached artifact for analysis."""
        ...

    def claim_next_intelligence_job(self, worker_id: str, targets: list[tuple[str, str]]) -> dict[str, Any]:
        """Claim the next available intelligence job."""
        ...

    def fail_intelligence_job(self, job_id: int, error: str) -> None:
        """Record an intelligence job failure."""
        ...

    def complete_intelligence_job(self, job_id: int) -> None:
        """Mark an intelligence job complete."""
        ...

    def intelligence_job_status(self) -> dict[str, int]:
        """Return intelligence queue status counts."""
        ...

    def get_message(self, message_id: str) -> dict[str, Any]:
        """Return a cached message mapping."""
        ...

    def attachments_for_message(self, message_id: str) -> list[dict[str, Any]]:
        """Return cached attachments for a message."""
        ...

    def list_attachments(self) -> list[dict[str, Any]]:
        """Return every cached attachment."""
        ...

    def delete_graph_triples_for_message(self, message_id: str) -> None:
        """Delete graph triples associated with a message."""
        ...

    def delete_graph_triples_for_attachment(self, sha1: str) -> None:
        """Delete graph triples associated with an attachment."""
        ...

    def add_triples(self, triples: list[GraphTriple]) -> None:
        """Persist graph triples."""
        ...


class _IntelligenceSemantic(Protocol):
    """Provide semantic index operations used by the worker."""

    def index_message(self, message_id: str, text: str, metadata: dict[str, Any]) -> None:
        """Index a message document."""
        ...

    def index_attachment(self, sha1: str, text: str, metadata: dict[str, Any]) -> None:
        """Index an attachment document."""
        ...


class _IntelligenceGraph(Protocol):
    """Mirror extracted triples to the external graph store."""

    def add_triples(self, triples: list[GraphTriple]) -> None:
        """Persist graph triples."""
        ...


class _NoGraph:
    """Discard graph triples when an external graph store is disabled."""

    def add_triples(self, triples: list[GraphTriple]) -> None:
        """Ignore graph triples."""
        _ = triples


DEFAULT_GRAPH = _NoGraph()


class IntelligenceWorker:
    """Represent IntelligenceWorker data and behavior."""

    def __init__(self, cache: _IntelligenceCache, semantic: _IntelligenceSemantic, graph: _IntelligenceGraph = DEFAULT_GRAPH) -> None:
        """Initialize IntelligenceWorker."""
        self.cache = cache
        self.semantic = semantic
        self.graph = graph
        self.categories = CategoryEngine(cache)
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}"

    def enqueue_all(self) -> dict[str, int]:
        """Enqueue all."""
        return self.cache.enqueue_all_intelligence_jobs()

    def run_until_empty(self, limit: int = 0, targets: list[tuple[str, str]] = DEFAULT_TARGETS) -> dict[str, int]:
        """Run until empty."""
        processed = 0
        failed = 0
        while limit <= 0 or processed < limit:
            job = self.cache.claim_next_intelligence_job(worker_id=self.worker_id, targets=targets)
            if not job:
                break
            try:
                self.process_job(job)
            except INTELLIGENCE_EXCEPTIONS as exc:
                self.cache.fail_intelligence_job(job["id"], repr(exc))
                failed += 1
            else:
                self.cache.complete_intelligence_job(job["id"])
                processed += 1
        return {"processed": processed, "failed": failed, "remaining": self.cache.intelligence_job_status().get("pending", 0)}

    def process_job(self, job: dict[str, Any]) -> None:
        """Process job."""
        if job["kind"] == "message":
            self.process_message(job["target_id"])
            return
        if job["kind"] == "attachment":
            self.process_attachment(job["target_id"])
            return
        msg = f"Unknown intelligence job kind: {job['kind']}"
        raise ValueError(msg)

    def process_message(self, message_id: str) -> None:
        """Process message."""
        message = self.cache.get_message(message_id)
        if not message:
            raise KeyError(message_id)
        parsed = parse_gmail_message(message["raw"])
        attachment_sha1s = [str(attachment["sha1"]) for attachment in self.cache.attachments_for_message(message_id)]
        triples = extract_triples(parsed, attachment_sha1s)
        self.cache.delete_graph_triples_for_message(message_id)
        self.cache.add_triples(triples)
        self.graph.add_triples(triples)
        self.categories.categorize_message(message_id)
        text = "\n\n".join([str(message.get("subject") or ""), str(message.get("text_body") or ""), str(message.get("markdown") or "")])
        updated_message = self.cache.get_message(message_id)
        categories = ",".join(str(category) for category in updated_message.get("categories", []))
        self.semantic.index_message(
            message_id, clean_text_for_kg(text), {"thread_id": str(message.get("thread_id") or ""), "categories": categories}
        )

    def process_attachment(self, sha1: str) -> None:
        """Process attachment."""
        attachments = [attachment for attachment in self.cache.list_attachments() if attachment["sha1"] == sha1]
        if not attachments:
            raise KeyError(sha1)
        attachment = attachments[0]
        metadata = cast(dict[str, Any], attachment.get("metadata", {}))
        triples = extract_attachment_sidecar_triples(sha1, metadata)
        self.cache.delete_graph_triples_for_attachment(sha1)
        self.cache.add_triples(triples)
        self.graph.add_triples(triples)
        text = attachment_text_from_metadata(metadata) or json.dumps(metadata, sort_keys=True)
        self.semantic.index_attachment(
            sha1,
            text,
            {
                "message_id": str(attachment["message_id"]),
                "filename": str(attachment.get("filename") or ""),
                "mime_type": str(attachment.get("mime_type") or ""),
            },
        )
