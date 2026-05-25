# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Shared Protocol declarations for Gmeow cache, semantic, and graph dependencies.

This module hosts the structural typing surface used by both ``SyncService`` and
``IntelligenceWorker`` so they can take the same concrete ``PgCache`` and semantic backend without
casts. The combined :class:`GmeowCache` and :class:`GmeowSemantic` protocols collect the methods
each subsystem needs, while the individual subsystem protocols (e.g. :class:`SyncCache`,
:class:`IntelligenceCache`) stay available for narrower call sites.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from .parser import ParsedMessage

GraphTriple = tuple[str, str, str, Any]

DEFAULT_STR = cast(str, None)
DEFAULT_DICT_ANY = cast(dict[str, Any], None)
DEFAULT_LIST_STR = cast(list[str], None)


@dataclass(frozen=True, slots=True)
class StoredAttachmentObject:
    """Attachment object metadata returned by transitional attachment services."""

    sha1: str
    digest: str
    path: Path
    sidecar_path: Path
    size: int
    metadata: dict[str, Any]
    compression: str


class SyncCache(Protocol):
    """Cache surface required by ``SyncService``."""

    def start_sync_run(self, run_kind: str, start_cursor: str = DEFAULT_STR, request: dict[str, Any] = DEFAULT_DICT_ANY) -> int:
        """Record the start of a sync run."""
        ...

    def finish_sync_run(
        self,
        run_id: int,
        status: str,
        end_cursor: str = DEFAULT_STR,
        result: dict[str, Any] = DEFAULT_DICT_ANY,
        error: str = DEFAULT_STR,
    ) -> None:
        """Record the final state of a sync run."""
        ...

    def upsert_label(self, label: dict[str, Any]) -> None:
        """Store or update a Gmail label."""
        ...

    def upsert_rule(self, name: str, priority: int, rule: dict[str, Any]) -> None:
        """Store or update a priority rule."""
        ...

    def get_state(self, key: str) -> str:
        """Return a persisted sync state value."""
        ...

    def set_state(self, key: str, value: str) -> None:
        """Persist a sync state value."""
        ...

    def delete_state(self, key: str) -> None:
        """Delete a persisted sync state value."""
        ...

    def upsert_thread(self, thread: dict[str, Any]) -> None:
        """Store or update a Gmail thread."""
        ...

    def upsert_message(self, message: ParsedMessage, raw_json: dict[str, Any], markdown: str, *, hydrated: bool) -> None:
        """Store or update a parsed message."""
        ...

    def enqueue_intelligence_job(self, kind: str, target_id: str, payload: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        """Queue intelligence processing for a target."""
        ...

    def intelligence_target_status(self, targets: list[tuple[str, str]]) -> dict[str, Any]:
        """Return aggregate intelligence status for targets."""
        ...

    def message_backfill_complete(self, message_id: str, *, require_raw: bool = True) -> dict[str, Any]:
        """Return backfill completeness for a message."""
        ...

    def text_search(
        self,
        query: str,
        limit: int = 20,
        after: str = DEFAULT_STR,
        before: str = DEFAULT_STR,
        include_categories: list[str] = DEFAULT_LIST_STR,
        exclude_categories: list[str] = DEFAULT_LIST_STR,
    ) -> list[dict[str, Any]]:
        """Search cached message text."""
        ...

    def graph_search(
        self,
        term: str,
        limit: int = 50,
        *,
        include_noise: bool = False,
        kind: str = DEFAULT_STR,
        namespace: str = DEFAULT_STR,
        visibility: str = "user",
    ) -> list[dict[str, Any]]:
        """Search cached graph content."""
        ...

    def category_allowed(self, categories: list[str], include_categories: list[str], exclude_categories: list[str]) -> bool:
        """Return whether categories satisfy visibility filters."""
        ...

    def raw_rfc822(self, message_id: str) -> bytes:
        """Return cached raw RFC822 bytes."""
        ...

    def set_raw_rfc822(self, message_id: str, raw: bytes) -> None:
        """Store raw RFC822 bytes."""
        ...

    def archive_incomplete_message_ids(self, limit: int = 25) -> list[str]:
        """Return message ids missing archive artifacts."""
        ...

    def latest_history_id(self) -> str:
        """Return the latest cached Gmail history id."""
        ...

    def record_operational_event(
        self,
        event_type: str,
        severity: str,
        component: str,
        subject_id: str = DEFAULT_STR,
        detail: str = DEFAULT_STR,
        metadata: dict[str, Any] = DEFAULT_DICT_ANY,
    ) -> int:
        """Record an operational event."""
        ...

    def apply_retention_policy(self, message_id: str, source: str = "operator", *, dry_run: bool = True) -> dict[str, Any]:
        """Apply retention policy to a message."""
        ...

    def add_attachment(self, record: dict[str, Any]) -> None:
        """Store attachment metadata."""
        ...

    def record_search_run(self, search_id: str, query: str, source: str, request: dict[str, Any], status: str = "running") -> None:
        """Record search run start."""
        ...

    def finish_search_run(
        self,
        search_id: str,
        *,
        status: str,
        source: str,
        live: dict[str, Any],
        message_ids: list[str],
        analysis: dict[str, Any],
        attachment_hydration: dict[str, Any],
        phase_timings: dict[str, Any],
        error: str = DEFAULT_STR,
    ) -> None:
        """Record search run completion."""
        ...

    def search_run(self, search_id: str) -> dict[str, Any]:
        """Return a search run."""
        ...

    def enqueue_deferred_attachment_hydration(self, ref: dict[str, Any], payload: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        """Queue deferred attachment hydration."""
        ...

    def deferred_attachment_hydration_status(self, message_ids: list[str] = DEFAULT_LIST_STR) -> dict[str, Any]:
        """Return deferred attachment hydration status."""
        ...


class IntelligenceCache(Protocol):
    """Cache surface required by ``IntelligenceWorker``."""

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


class GmeowCache(SyncCache, IntelligenceCache, Protocol):
    """Combined cache contract used wherever both sync and intelligence surfaces are required."""


class SyncSemantic(Protocol):
    """Semantic index search surface used by ``SyncService``."""

    def search(self, query: str, limit: int = 10, source_kind: str = DEFAULT_STR) -> list[dict[str, Any]]:
        """Search semantic chunks."""
        ...

    def available(self) -> bool:
        """Return whether semantic search is available."""
        ...


class IntelligenceSemantic(Protocol):
    """Semantic index write surface used by ``IntelligenceWorker``."""

    def index_message(self, message_id: str, text: str, metadata: dict[str, Any]) -> None:
        """Index a message document."""
        ...

    def index_attachment(self, sha1: str, text: str, metadata: dict[str, Any]) -> None:
        """Index an attachment document."""
        ...


class GmeowSemantic(SyncSemantic, IntelligenceSemantic, Protocol):
    """Combined semantic contract satisfied by the concrete pgvector-backed index."""


class IntelligenceGraph(Protocol):
    """Mirror extracted triples to the external graph store."""

    def add_triples(self, triples: list[GraphTriple]) -> None:
        """Persist graph triples."""
        ...


class NoGraph:
    """Discard graph triples when an external graph store is disabled."""

    def add_triples(self, triples: list[GraphTriple]) -> None:
        """Ignore graph triples."""
        _ = triples


class SyncAttachments(Protocol):
    """Attachment storage protocol used by ``SyncService``."""

    def put(
        self,
        content: bytes,
        metadata: dict[str, Any],
        *,
        extract_metadata: bool = True,
        media_type: str = "application/octet-stream",
    ) -> StoredAttachmentObject:
        """Store attachment content."""
        ...
