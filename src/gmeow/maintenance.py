# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from .config import MaintenanceConfig
from .intelligence import IntelligenceWorker


@dataclass(slots=True)
class TimedTaskState:
    name: str
    interval_seconds: int
    running: bool = False
    runs: int = 0
    failures: int = 0
    last_started_at: str | None = None
    last_finished_at: str | None = None
    last_duration_seconds: float | None = None
    last_result: Any = None
    last_error: str | None = None
    next_run_at: str | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


class MaintenanceScheduler:
    def __init__(self, config: MaintenanceConfig, cache: Any, sync: Any, attachments: Any, semantic: Any, graph: Any | None = None):
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
        if not self.config.enabled:
            return
        specs: list[tuple[str, int | None, Callable[[], Any]]] = [
            ("sync_history", self.config.sync_history_seconds, self._sync_history),
            ("sync_priority", self.config.sync_priority_seconds, self._sync_priority),
            ("intelligence", self.config.intelligence_seconds, self._run_intelligence),
            ("derived_refresh", self.config.derived_refresh_seconds, self._refresh_derived),
            ("analyze", self.config.analyze_seconds, self.cache.analyze_storage_tables),
            ("attachment_sidecars", self.config.attachment_sidecars_seconds, self._refresh_attachment_sidecars),
        ]
        for name, interval, callback in specs:
            if interval is None:
                continue
            state = self._states.setdefault(name, TimedTaskState(name=name, interval_seconds=interval))
            self._tasks.append(asyncio.create_task(self._loop(state, callback), name=f"gmeow-maintenance-{name}"))

    async def stop(self) -> None:
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def status(self) -> dict[str, Any]:
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
        callbacks: dict[str, Callable[[], Any]] = {
            "sync_history": self._sync_history,
            "sync_priority": self._sync_priority,
            "intelligence": self._run_intelligence,
            "derived_refresh": self._refresh_derived,
            "analyze": self.cache.analyze_storage_tables,
            "attachment_sidecars": self._refresh_attachment_sidecars,
        }
        if name not in callbacks:
            raise KeyError(name)
        state = self._states.setdefault(name, TimedTaskState(name=name, interval_seconds=0))
        await self._run_once(state, callbacks[name])
        return self.status()["tasks"][name]

    async def _loop(self, state: TimedTaskState, callback: Callable[[], Any]) -> None:
        delay = 0.0 if self.config.run_on_startup else float(state.interval_seconds)
        while not self._stopping.is_set():
            state.next_run_at = _future_iso(delay)
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                break
            except asyncio.TimeoutError:
                pass
            await self._run_once(state, callback)
            delay = float(state.interval_seconds)

    async def _run_once(self, state: TimedTaskState, callback: Callable[[], Any]) -> None:
        if state._lock.locked():
            state.last_error = "previous run still active; skipped overlapping run"
            state.next_run_at = _future_iso(float(state.interval_seconds))
            return
        async with state._lock:
            started = time.monotonic()
            state.running = True
            state.last_started_at = _now_iso()
            state.last_error = None
            self.cache.set_state(f"maintenance.{state.name}.last_started_at", state.last_started_at)
            self.cache.record_operational_event("maintenance.started", "info", "maintenance", state.name, f"Started maintenance task {state.name}.")
            try:
                result = await asyncio.to_thread(callback)
            except Exception as exc:
                state.failures += 1
                state.last_error = repr(exc)
                self.cache.set_state(f"maintenance.{state.name}.last_error", state.last_error)
                self.cache.record_operational_event("maintenance.failed", "error", "maintenance", state.name, state.last_error)
            else:
                state.runs += 1
                state.last_result = result
                self.cache.set_state(f"maintenance.{state.name}.last_result", _summarize_result(result))
                self.cache.record_operational_event("maintenance.completed", "info", "maintenance", state.name, f"Completed maintenance task {state.name}.", {"result": _summarize_result(result)})
            finally:
                state.running = False
                state.last_finished_at = _now_iso()
                state.last_duration_seconds = round(time.monotonic() - started, 3)
                self.cache.set_state(f"maintenance.{state.name}.last_finished_at", state.last_finished_at)

    def _run_intelligence(self) -> dict[str, int]:
        return IntelligenceWorker(self.cache, self.semantic, self.graph).run_until_empty(limit=self.config.intelligence_limit)

    def _sync_history(self) -> dict[str, Any]:
        if self.sync.gmail is None:
            return {"skipped": "gmail client is not configured"}
        if not self._gmail_lock.acquire(blocking=False):
            return {"skipped": "another Gmail maintenance task is running"}
        try:
            return self.sync.sync_history(limit=self.config.sync_history_limit)
        finally:
            self._gmail_lock.release()

    def _sync_priority(self) -> dict[str, Any]:
        if self.sync.gmail is None:
            return {"skipped": "gmail client is not configured"}
        if not self._gmail_lock.acquire(blocking=False):
            return {"skipped": "another Gmail maintenance task is running"}
        try:
            return self.sync.sync_priority(limit_per_rule=self.config.sync_priority_limit_per_rule)
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
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _future_iso(seconds: float) -> str:
    return datetime.fromtimestamp(time.time() + seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _summarize_result(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 2000 else text[:1997] + "..."
