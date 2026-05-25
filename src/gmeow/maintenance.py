# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Schedule and execute Gmeow maintenance tasks.

The module coordinates periodic sync, backfill, analysis, archive refresh, and derived-data refresh
work. It protects Gmail-facing tasks with a lock so maintenance does not run overlapping mailbox
operations.
"""

import asyncio
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from googleapiclient.errors import HttpError

from .config import MaintenanceConfig
from .intelligence import IntelligenceWorker
from .object_store import StoredAttachmentObject

DEFAULT_ANY = cast(Any, None)
DEFAULT_FLOAT = cast(float, None)
DEFAULT_OBJECT = cast(object, None)
DEFAULT_STR = cast(str, None)

MAINTENANCE_EXCEPTIONS = (RuntimeError, ValueError, KeyError, TypeError, OSError, HttpError)
MAX_RESULT_SUMMARY_CHARS = 2000


class _MaintenanceCache(Protocol):
    """Cache surface used by maintenance tasks."""

    def analyze_storage_tables(self) -> dict[str, Any]:
        """Analyze storage tables."""
        ...

    def set_state(self, key: str, value: str) -> None:
        """Persist a state value."""
        ...

    def record_operational_event(
        self,
        event_type: str,
        severity: str,
        component: str,
        subject_id: str = DEFAULT_STR,
        detail: str = DEFAULT_STR,
        metadata: dict[str, Any] = DEFAULT_ANY,
    ) -> int:
        """Record an operational event."""
        ...

    def get_state(self, key: str) -> str:
        """Return a state value."""
        ...

    def refresh_message_search_columns(self) -> dict[str, Any]:
        """Refresh message search columns."""
        ...

    def refresh_content_object_refs(self) -> dict[str, Any]:
        """Refresh content object references."""
        ...

    def refresh_graph_node_profiles(self) -> dict[str, Any]:
        """Refresh graph node profiles."""
        ...

    def refresh_graph_edge_stats(self) -> dict[str, Any]:
        """Refresh graph edge statistics."""
        ...

    def refresh_summary_items(self) -> dict[str, Any]:
        """Refresh summary items."""
        ...

    def refresh_timeline_views(self) -> dict[str, Any]:
        """Refresh timeline views."""
        ...

    def refresh_materialized_summary_views(self) -> dict[str, Any]:
        """Refresh materialized summary views."""
        ...

    def list_attachments(self) -> list[dict[str, Any]]:
        """Return stored attachments."""
        ...

    def get_message(self, message_id: str) -> dict[str, Any]:
        """Return a cached message."""
        ...

    def add_attachment(self, record: dict[str, Any]) -> None:
        """Store attachment metadata."""
        ...

    def enqueue_intelligence_job(self, kind: str, target_id: str, payload: dict[str, Any] = DEFAULT_ANY) -> None:
        """Queue intelligence processing."""
        ...

    def claim_deferred_attachment_hydration(self, limit: int, worker_id: str) -> list[dict[str, Any]]:
        """Claim deferred attachment hydration jobs."""
        ...

    def complete_deferred_attachment_hydration(self, job_id: int, sha1: str) -> None:
        """Complete deferred attachment hydration."""
        ...

    def fail_deferred_attachment_hydration(self, job_id: int, error: str) -> None:
        """Fail deferred attachment hydration."""
        ...


class _MaintenanceSync(Protocol):
    """Sync surface used by maintenance tasks."""

    def gmail_available(self) -> bool:
        """Return whether Gmail is configured."""
        ...

    def sync_history(self, limit: int = 500) -> dict[str, Any]:
        """Run Gmail history sync."""
        ...

    def sync_priority(self, limit_per_rule: int = 100) -> dict[str, Any]:
        """Run priority sync."""
        ...

    def backfill_batch(self, batch_size: int = 50, *, validate: bool = False, max_empty_windows: int = 120) -> dict[str, Any]:
        """Run one backfill batch."""
        ...

    def hydrate_deferred_attachment(self, job: dict[str, Any]) -> str:
        """Hydrate one deferred attachment and return its digest."""
        ...


class _MaintenanceAttachments(Protocol):
    """Attachment surface used by maintenance tasks."""

    def refresh_sidecar(self, sha1: str, source_metadata: dict[str, Any] = DEFAULT_ANY) -> StoredAttachmentObject:
        """Refresh an attachment sidecar."""
        ...


@dataclass(slots=True)
class TimedTaskState:
    """Represent TimedTaskState data and behavior."""

    name: str
    interval_seconds: int
    running: bool = False
    runs: int = 0
    failures: int = 0
    last_started_at: str = DEFAULT_STR
    last_finished_at: str = DEFAULT_STR
    last_duration_seconds: float = DEFAULT_FLOAT
    last_result: Any = DEFAULT_ANY
    last_error: str = DEFAULT_STR
    next_run_at: str = DEFAULT_STR
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


class MaintenanceScheduler:
    """Represent MaintenanceScheduler data and behavior."""

    def __init__(
        self,
        config: MaintenanceConfig,
        cache: _MaintenanceCache,
        sync: _MaintenanceSync,
        attachments: _MaintenanceAttachments,
        semantic: object,
        graph: object = DEFAULT_OBJECT,
    ) -> None:
        """Initialize MaintenanceScheduler."""
        self.config = config
        self.cache = cache
        self.sync = sync
        self.attachments = attachments
        self.semantic = semantic
        self.graph = graph
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()
        self._states: dict[str, TimedTaskState] = {}
        self._gmail_lock = threading.Lock()

    def start(self) -> None:
        """Start."""
        if not self.config.enabled:
            return
        specs: list[tuple[str, int, Callable[[], Any]]] = [
            ("sync_history", self.config.sync_history_seconds, self._sync_history),
            ("sync_priority", self.config.sync_priority_seconds, self._sync_priority),
            ("attachment_hydration", self.config.attachment_hydration_seconds, self._hydrate_deferred_attachments),
            ("intelligence", self.config.intelligence_seconds, self._run_intelligence),
            ("derived_refresh", self.config.derived_refresh_seconds, self._refresh_derived),
            ("analyze", self.config.analyze_seconds, self.cache.analyze_storage_tables),
        ]
        if self.config.backfill_enabled:
            specs.append(("backfill", self.config.backfill_seconds, self._backfill))
        if self.config.attachment_sidecars_seconds:
            specs.append(("attachment_sidecars", self.config.attachment_sidecars_seconds, self._refresh_attachment_sidecars))
        for name, interval, callback in specs:
            if not interval or interval <= 0:
                continue
            state = self._states.setdefault(name, TimedTaskState(name=name, interval_seconds=interval))
            self._tasks.append(asyncio.create_task(self._loop(state, callback), name=f"gmeow-maintenance-{name}"))

    async def stop(self) -> None:
        """Stop."""
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def status(self) -> dict[str, Any]:
        """Status."""
        return {
            "enabled": self.config.enabled,
            "tasks": {
                name: {
                    "interval_seconds": state.interval_seconds,
                    "running": state.running,
                    "runs": state.runs,
                    "failures": state.failures,
                    "last_started_at": state.last_started_at,
                    "last_finished_at": state.last_finished_at,
                    "last_duration_seconds": state.last_duration_seconds,
                    "last_result": state.last_result,
                    "last_error": state.last_error,
                    "next_run_at": state.next_run_at,
                }
                for name, state in sorted(self._states.items())
            },
        }

    async def run_now(self, name: str) -> dict[str, Any]:
        """Run now."""
        callbacks: dict[str, Callable[[], Any]] = {
            "sync_history": self._sync_history,
            "sync_priority": self._sync_priority,
            "backfill": self._backfill,
            "attachment_hydration": self._hydrate_deferred_attachments,
            "intelligence": self._run_intelligence,
            "derived_refresh": self._refresh_derived,
            "analyze": self.cache.analyze_storage_tables,
            "attachment_sidecars": self._refresh_attachment_sidecars,
        }
        if name not in callbacks:
            raise KeyError(name)
        state = self._states.setdefault(name, TimedTaskState(name=name, interval_seconds=0))
        await self._run_once(state, callbacks[name])
        return cast(dict[str, Any], self.status()["tasks"][name])

    async def _loop(self, state: TimedTaskState, callback: Callable[[], Any]) -> None:
        delay = 0.0 if self.config.run_on_startup else float(state.interval_seconds)
        while not self._stopping.is_set():
            state.next_run_at = _future_iso(delay)
            stop_task = asyncio.create_task(self._stopping.wait())
            done, pending = await asyncio.wait({stop_task}, timeout=delay)
            for task in pending:
                task.cancel()
            if stop_task in done:
                break
            await self._run_once(state, callback)
            delay = float(state.interval_seconds)

    async def _run_once(self, state: TimedTaskState, callback: Callable[[], Any]) -> None:
        if state.lock.locked():
            state.last_error = "previous run still active; skipped overlapping run"
            state.next_run_at = _future_iso(float(state.interval_seconds))
            return
        async with state.lock:
            started = time.monotonic()
            state.running = True
            state.last_started_at = _now_iso()
            state.last_error = ""
            self.cache.set_state(f"maintenance.{state.name}.last_started_at", state.last_started_at)
            self.cache.record_operational_event(
                "maintenance.started", "info", "maintenance", state.name, f"Started maintenance task {state.name}."
            )
            try:
                result = await asyncio.to_thread(callback)
            except MAINTENANCE_EXCEPTIONS as exc:
                state.failures += 1
                state.last_error = repr(exc)
                self.cache.set_state(f"maintenance.{state.name}.last_error", state.last_error)
                self.cache.record_operational_event("maintenance.failed", "error", "maintenance", state.name, state.last_error)
            else:
                state.runs += 1
                state.last_result = result
                self.cache.set_state(f"maintenance.{state.name}.last_result", _summarize_result(result))
                self.cache.record_operational_event(
                    "maintenance.completed",
                    "info",
                    "maintenance",
                    state.name,
                    f"Completed maintenance task {state.name}.",
                    {"result": _summarize_result(result)},
                )
            finally:
                state.running = False
                state.last_finished_at = _now_iso()
                state.last_duration_seconds = round(time.monotonic() - started, 3)
                self.cache.set_state(f"maintenance.{state.name}.last_finished_at", state.last_finished_at)

    def _run_intelligence(self) -> dict[str, int]:
        return IntelligenceWorker(cast(Any, self.cache), cast(Any, self.semantic), cast(Any, self.graph)).run_until_empty(
            limit=self.config.intelligence_limit
        )

    def _hydrate_deferred_attachments(self) -> dict[str, int]:
        if not self.sync.gmail_available():
            return {"processed": 0, "failed": 0, "skipped": 1}
        if not self._gmail_lock.acquire(blocking=False):
            return {"processed": 0, "failed": 0, "skipped": 1}
        processed = 0
        failed = 0
        try:
            jobs = self.cache.claim_deferred_attachment_hydration(
                limit=self.config.attachment_hydration_limit,
                worker_id=f"maintenance:{threading.get_ident()}",
            )
            for job in jobs:
                try:
                    sha1 = self.sync.hydrate_deferred_attachment(job)
                except MAINTENANCE_EXCEPTIONS as exc:
                    self.cache.fail_deferred_attachment_hydration(int(job["id"]), repr(exc))
                    failed += 1
                else:
                    self.cache.complete_deferred_attachment_hydration(int(job["id"]), sha1)
                    self.cache.enqueue_intelligence_job("attachment", sha1, {"source": "deferred_attachment_hydration"})
                    processed += 1
        finally:
            self._gmail_lock.release()
        return {"processed": processed, "failed": failed, "skipped": 0}

    def _sync_history(self) -> dict[str, Any]:
        if not self.sync.gmail_available():
            return {"skipped": "gmail client is not configured"}
        if not self._gmail_lock.acquire(blocking=False):
            return {"skipped": "another Gmail maintenance task is running"}
        try:
            try:
                return self.sync.sync_history(limit=self.config.sync_history_limit)
            except RuntimeError as exc:
                if "No Gmail history cursor is available" in str(exc):
                    return {"skipped": str(exc)}
                raise
        finally:
            self._gmail_lock.release()

    def _sync_priority(self) -> dict[str, Any]:
        if not self.sync.gmail_available():
            return {"skipped": "gmail client is not configured"}
        if not self._gmail_lock.acquire(blocking=False):
            return {"skipped": "another Gmail maintenance task is running"}
        try:
            return self.sync.sync_priority(limit_per_rule=self.config.sync_priority_limit_per_rule)
        finally:
            self._gmail_lock.release()

    def _backfill(self) -> dict[str, Any]:
        if not self.sync.gmail_available():
            return {"skipped": "gmail client is not configured"}
        if not self._gmail_lock.acquire(blocking=False):
            return {"skipped": "another Gmail maintenance task is running"}
        try:
            return self.sync.backfill_batch(
                batch_size=self.config.backfill_batch_size,
                max_empty_windows=self.config.backfill_max_empty_windows,
            )
        finally:
            self._gmail_lock.release()

    def _refresh_derived(self) -> dict[str, Any]:
        return {
            "message_search": self.cache.refresh_message_search_columns(),
            "content_refs": self.cache.refresh_content_object_refs(),
            "graph_profiles": self.cache.refresh_graph_node_profiles(),
            "graph_edges": self.cache.refresh_graph_edge_stats(),
            "summaries": self.cache.refresh_summary_items(),
            "timelines": self.cache.refresh_timeline_views(),
            "summary_views": self.cache.refresh_materialized_summary_views(),
        }

    def _refresh_attachment_sidecars(self) -> dict[str, int]:
        refreshed = 0
        for attachment in self.cache.list_attachments():
            message = self.cache.get_message(attachment["message_id"]) or {}
            source_metadata = {
                "gmail": {
                    "message_id": attachment["message_id"],
                    "part_id": attachment.get("part_id"),
                    "attachment_id": attachment.get("gmail_attachment_id"),
                    "filename": attachment.get("filename"),
                    "mime_type": attachment.get("mime_type"),
                    "size": attachment.get("size"),
                },
                "message": {
                    "thread_id": message.get("thread_id"),
                    "subject": message.get("subject"),
                    "from": message.get("sender"),
                    "to": message.get("recipients"),
                    "date": message.get("message_date"),
                    "labels": message.get("label_ids", []),
                },
            }
            stored = self.attachments.refresh_sidecar(attachment["sha1"], source_metadata=source_metadata)
            attachment["metadata"] = stored.metadata
            attachment["digest"] = stored.digest
            attachment["compression"] = stored.compression
            attachment["path"] = str(stored.path)
            self.cache.add_attachment(attachment)
            self.cache.enqueue_intelligence_job("attachment", attachment["sha1"])
            refreshed += 1
        return {"refreshed": refreshed}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _future_iso(seconds: float) -> str:
    return datetime.fromtimestamp(time.time() + seconds, tz=UTC).isoformat().replace("+00:00", "Z")


def _summarize_result(value: object) -> str:
    text = repr(value)
    return text if len(text) <= MAX_RESULT_SUMMARY_CHARS else text[: MAX_RESULT_SUMMARY_CHARS - 3] + "..."
