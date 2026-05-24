# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Synchronize Gmail data into the Gmeow cache.

The module implements priority sync, history sync, oldest-first backfill, hydration, archive
completion, and user-visible Gmail mutations. It coordinates raw saves, attachment storage,
intelligence job enqueueing, and checkpoint advancement.
"""

import base64
import json
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, NoReturn, cast

from .categories import CategoryEngine
from .config import GmeowConfig, PriorityRule
from .gmail import GmailClient
from .intelligence import IntelligenceWorker
from .markdown import message_to_markdown
from .parser import ParsedMessage, parse_gmail_message
from .protocols import GmeowCache, GmeowSemantic, IntelligenceGraph, NoGraph, SyncAttachments
from .sync_backfill import (
    backfill_query,
    current_month_start,
    history_deleted_message_ids,
    history_message_ids,
    metadata_drift,
    next_month,
    parse_window_start,
)

DEFAULT_DICT_ANY = cast(dict[str, Any], None)
DEFAULT_GRAPH: IntelligenceGraph = NoGraph()
DEFAULT_LIST_STR = cast(list[str], None)
DEFAULT_OBJECT = cast(object, None)
DEFAULT_PRIORITY_RULE = cast(PriorityRule, None)
DEFAULT_STR = cast(str, None)

SYNC_EXCEPTIONS = (RuntimeError, ValueError, KeyError, TypeError, OSError, TimeoutError)
BACKFILL_BATCH_STATE = "backfill.current_batch"
BACKFILL_WINDOW_STATE = "backfill.window_start"
BACKFILL_PAGE_STATE = "backfill.page_token"


class MissingGmailClient:
    """Represent an unavailable Gmail client with the GmailClient protocol."""

    def _raise(self) -> NoReturn:
        msg = "Gmail client is not configured."
        raise RuntimeError(msg)

    def list_labels(self) -> list[dict[str, Any]]:
        """List labels."""
        self._raise()

    def search_messages(self, query: str, limit: int = 100) -> list[str]:
        """Search messages."""
        del query, limit
        self._raise()

    def search_messages_page(self, query: str, page_token: str = DEFAULT_STR, page_size: int = 50) -> dict[str, Any]:
        """Search one page of messages."""
        del query, page_token, page_size
        self._raise()

    def get_message_metadata(self, message_id: str) -> dict[str, Any]:
        """Get lightweight message metadata."""
        del message_id
        self._raise()

    def list_history(self, start_history_id: str, label_id: str = DEFAULT_STR, limit: int = 500) -> dict[str, Any]:
        """List history."""
        del start_history_id, label_id, limit
        self._raise()

    def get_message(self, message_id: str, fmt: str = "full") -> dict[str, Any]:
        """Get message."""
        del message_id, fmt
        self._raise()

    def get_thread(self, thread_id: str) -> dict[str, Any]:
        """Get thread."""
        del thread_id
        self._raise()

    def get_attachment(self, message_id: str, attachment_id: str) -> bytes:
        """Get attachment."""
        del message_id, attachment_id
        self._raise()

    def modify_message(
        self, message_id: str, add_label_ids: list[str] = DEFAULT_LIST_STR, remove_label_ids: list[str] = DEFAULT_LIST_STR
    ) -> dict[str, Any]:
        """Modify message."""
        del message_id, add_label_ids, remove_label_ids
        self._raise()


DEFAULT_GMAIL_CLIENT = MissingGmailClient()


@dataclass(slots=True)
class HybridFilters:
    """Hold filters shared by hybrid search scoring helpers."""

    after: str
    before: str
    include_categories: list[str]
    exclude_categories: list[str]


def _priority_rule_rank(rule: PriorityRule) -> int:
    return rule.priority


def _hybrid_result_rank(item: dict[str, Any]) -> tuple[float, str]:
    return item["score"], item["message"].get("message_date_iso") or ""


class SyncService:
    """Represent SyncService data and behavior."""

    def __init__(
        self,
        config: GmeowConfig,
        cache: GmeowCache,
        attachments: SyncAttachments,
        semantic: GmeowSemantic,
        gmail: GmailClient = DEFAULT_GMAIL_CLIENT,
        graph: IntelligenceGraph = DEFAULT_GRAPH,
    ) -> None:
        """Initialize SyncService."""
        self.config = config
        self.cache = cache
        self.attachments = attachments
        self.semantic = semantic
        self.gmail = gmail
        self.graph = graph
        self.maintenance_scheduler = DEFAULT_OBJECT

    def gmail_available(self) -> bool:
        """Return whether Gmail-backed operations can run."""
        return not isinstance(self.gmail, MissingGmailClient)

    def sync_priority(self, limit_per_rule: int = 100) -> dict[str, Any]:
        """Sync priority."""
        if not self.gmail_available():
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        run_id = self.cache.start_sync_run("priority", request={"limit_per_rule": limit_per_rule})
        labels = self.gmail.list_labels()
        try:
            for label in labels:
                self.cache.upsert_label(label)
            summary: dict[str, Any] = {"labels": len(labels), "rules": [], "messages": 0}
            for rule in sorted(self.config.priority_rules, key=_priority_rule_rank, reverse=True):
                self.cache.upsert_rule(rule.name, rule.priority, asdict(rule))
                query = rule.to_gmail_query()
                if not query:
                    continue
                ids = self.gmail.search_messages(query, limit=limit_per_rule)
                hydrated = 0
                filtered = 0
                for message_id in ids:
                    if self.hydrate_message(message_id, rule=rule):
                        hydrated += 1
                    else:
                        filtered += 1
                summary["rules"].append(
                    {"name": rule.name, "query": query, "matched": len(ids), "hydrated": hydrated, "filtered": filtered}
                )
                summary["messages"] += hydrated
            self.cache.set_state("last_priority_sync", datetime.now(UTC).isoformat())
            self.cache.set_state("last_priority_summary", json.dumps(summary, sort_keys=True))
            self._update_history_cursor_from_cache()
            self.cache.finish_sync_run(run_id, "complete", end_cursor=self.cache.get_state("gmail_history_id"), result=summary)
        except SYNC_EXCEPTIONS as exc:
            self.cache.finish_sync_run(run_id, "failed", error=repr(exc))
            raise
        else:
            return summary

    def sync_history(self, limit: int = 500) -> dict[str, Any]:
        """Sync history."""
        if not self.gmail_available():
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        start = self.cache.get_state("gmail_history_id") or self._latest_cached_history_id()
        if not start:
            msg = "No Gmail history cursor is available. Run priority sync or hydrate at least one message first."
            raise RuntimeError(msg)
        run_id = self.cache.start_sync_run("history", start_cursor=str(start), request={"limit": limit})
        try:
            result = self.gmail.list_history(start, limit=limit)
            message_ids = history_message_ids(result.get("history", []))
            deleted_ids = history_deleted_message_ids(result.get("history", []))
            hydrated = 0
            for message_id in message_ids:
                if self.hydrate_message(message_id, update_history_cursor=False):
                    hydrated += 1
            deleted = 0
            for message_id in deleted_ids:
                if self.cache.get_message(message_id):
                    self.cache.apply_retention_policy(message_id, source="gmail_history", dry_run=False)
                    deleted += 1
            truncated = bool(result.get("truncated"))
            if result.get("history_id") and not truncated:
                self.cache.set_state("gmail_history_id", str(result["history_id"]))
            summary = {
                "start_history_id": start,
                "history_id": result.get("history_id"),
                "history_records": len(result.get("history", [])),
                "messages": len(message_ids),
                "hydrated": hydrated,
                "deleted": deleted,
                "truncated": truncated,
                "cursor_advanced": bool(result.get("history_id")) and not truncated,
            }
            self.cache.set_state("last_history_sync", datetime.now(UTC).isoformat())
            self.cache.set_state("last_history_summary", json.dumps(summary, sort_keys=True))
            self.cache.finish_sync_run(run_id, "complete", end_cursor=str(result.get("history_id") or ""), result=summary)
        except SYNC_EXCEPTIONS as exc:
            self.cache.finish_sync_run(run_id, "failed", error=repr(exc))
            raise
        else:
            return summary

    def backfill_batch(self, batch_size: int = 50, *, validate: bool = False, max_empty_windows: int = 120) -> dict[str, Any]:
        """Backfill one oldest-first Gmail batch and fully analyze it before advancing."""
        if not self.gmail_available():
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        run_id = self.cache.start_sync_run(
            "backfill",
            start_cursor=self.cache.get_state(BACKFILL_WINDOW_STATE),
            request={"batch_size": batch_size, "validate": validate, "max_empty_windows": max_empty_windows},
        )
        try:
            summary = self._backfill_batch(batch_size=max(1, batch_size), validate=validate, max_empty_windows=max_empty_windows)
            self.cache.finish_sync_run(
                run_id,
                "complete",
                end_cursor=self.cache.get_state(BACKFILL_WINDOW_STATE),
                result=summary,
            )
        except SYNC_EXCEPTIONS as exc:
            self.cache.finish_sync_run(run_id, "failed", error=repr(exc))
            raise
        else:
            return summary

    def _backfill_batch(self, *, batch_size: int, validate: bool, max_empty_windows: int) -> dict[str, Any]:
        current_batch = self.cache.get_state(BACKFILL_BATCH_STATE)
        if current_batch:
            batch = json.loads(current_batch)
        else:
            batch = self._next_backfill_batch(batch_size=batch_size, max_empty_windows=max_empty_windows)
            if batch.get("complete"):
                self.cache.delete_state(BACKFILL_BATCH_STATE)
                self.cache.delete_state(BACKFILL_PAGE_STATE)
                self.cache.delete_state(BACKFILL_WINDOW_STATE)
                self.cache.set_state("backfill.completed_at", datetime.now(UTC).isoformat())
                self._update_history_cursor_from_cache()
                return batch
            if not batch.get("message_ids") and batch.get("reason"):
                return batch
            self.cache.set_state(BACKFILL_BATCH_STATE, json.dumps(batch, sort_keys=True))
        message_ids = list(batch.get("message_ids", []))
        stats = {"hydrated": 0, "skipped": 0, "metadata_checked": 0, "changed": 0}
        targets: list[tuple[str, str]] = []
        for message_id in message_ids:
            self._process_backfill_message(message_id, validate=validate, stats=stats)
            message_targets = self._backfill_targets_for_message(message_id)
            targets.extend(target for target in message_targets if target not in targets)
        target_status = self.cache.intelligence_target_status(targets)
        if targets and not target_status["terminal"]:
            worker = IntelligenceWorker(self.cache, self.semantic, self.graph)
            worker.run_until_empty(targets=targets)
            target_status = self.cache.intelligence_target_status(targets)
        target_status = self._wait_for_running_backfill_targets(targets, target_status)
        if not target_status["terminal"]:
            return {
                **batch,
                "validate": validate,
                **stats,
                "advanced": False,
                "target_status": target_status,
            }
        self._advance_backfill_checkpoint(batch)
        return {
            **batch,
            "validate": validate,
            **stats,
            "advanced": True,
            "target_status": target_status,
        }

    def _process_backfill_message(self, message_id: str, *, validate: bool, stats: dict[str, int]) -> None:
        cached = self.cache.get_message(message_id)
        completion = self.cache.message_backfill_complete(message_id, require_raw=self.config.archive.require_rfc822)
        needs_hydrate = not completion["exists"] or not completion["complete"]
        if cached and validate:
            stats["metadata_checked"] += 1
            metadata = self.gmail.get_message_metadata(message_id)
            if metadata_drift(cached.get("raw") or {}, metadata):
                stats["changed"] += 1
                needs_hydrate = True
        if needs_hydrate:
            message = self.hydrate_message(message_id, update_history_cursor=False)
            if message:
                stats["hydrated"] += 1
            return
        stats["skipped"] += 1
        if completion.get("target_status", {}).get("missing"):
            self.cache.enqueue_intelligence_job("message", message_id)

    def _next_backfill_batch(self, *, batch_size: int, max_empty_windows: int) -> dict[str, Any]:
        window_start = parse_window_start(self.cache.get_state(BACKFILL_WINDOW_STATE))
        page_token = self.cache.get_state(BACKFILL_PAGE_STATE)
        skipped_windows = 0
        while window_start <= current_month_start():
            query = backfill_query(window_start)
            page = self.gmail.search_messages_page(query, page_token=page_token, page_size=batch_size)
            message_ids = [item["id"] for item in page.get("messages", []) if item.get("id")]
            if message_ids:
                return {
                    "complete": False,
                    "query": query,
                    "window_start": window_start.isoformat(),
                    "window_end": next_month(window_start).isoformat(),
                    "page_token": page_token,
                    "next_page_token": str(page.get("next_page_token") or ""),
                    "message_ids": message_ids,
                    "started_at": datetime.now(UTC).isoformat(),
                    "skipped_empty_windows": skipped_windows,
                    "result_size_estimate": page.get("result_size_estimate"),
                }
            next_page_token = str(page.get("next_page_token") or "")
            if next_page_token:
                page_token = next_page_token
                self.cache.set_state(BACKFILL_PAGE_STATE, page_token)
                continue
            skipped_windows += 1
            if skipped_windows > max_empty_windows:
                return {
                    "complete": False,
                    "query": query,
                    "window_start": window_start.isoformat(),
                    "window_end": next_month(window_start).isoformat(),
                    "page_token": "",
                    "next_page_token": "",
                    "message_ids": [],
                    "started_at": datetime.now(UTC).isoformat(),
                    "skipped_empty_windows": skipped_windows,
                    "advanced": False,
                    "reason": "max_empty_windows_reached",
                }
            window_start = next_month(window_start)
            page_token = ""
            self.cache.set_state(BACKFILL_WINDOW_STATE, window_start.isoformat())
            self.cache.delete_state(BACKFILL_PAGE_STATE)
        return {"complete": True, "skipped_empty_windows": skipped_windows, "completed_at": datetime.now(UTC).isoformat()}

    def _advance_backfill_checkpoint(self, batch: dict[str, Any]) -> None:
        next_page_token = batch.get("next_page_token")
        if next_page_token:
            self.cache.set_state(BACKFILL_PAGE_STATE, str(next_page_token))
        else:
            self.cache.set_state(BACKFILL_WINDOW_STATE, str(batch["window_end"]))
            self.cache.delete_state(BACKFILL_PAGE_STATE)
        self.cache.delete_state(BACKFILL_BATCH_STATE)
        self.cache.delete_state("backfill.completed_at")

    def _backfill_targets_for_message(self, message_id: str) -> list[tuple[str, str]]:
        targets = [("message", message_id)]
        targets.extend(("attachment", attachment["sha1"]) for attachment in self.cache.attachments_for_message(message_id))
        return targets

    def _wait_for_running_backfill_targets(self, targets: list[tuple[str, str]], status: dict[str, Any]) -> dict[str, Any]:
        if not targets or not status.get("running"):
            return status
        deadline = time.monotonic() + 30.0
        while status.get("running") and not status.get("pending") and time.monotonic() < deadline:
            time.sleep(0.5)
            status = self.cache.intelligence_target_status(targets)
        return status

    def hydrate_message(
        self, message_id: str, rule: PriorityRule = DEFAULT_PRIORITY_RULE, *, update_history_cursor: bool = True
    ) -> dict[str, Any]:
        """Hydrate message."""
        if not self.gmail_available():
            cached = self.cache.get_message(message_id)
            if cached:
                return cached
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        raw = self.gmail.get_message(message_id, fmt="full")
        try:
            parsed = parse_gmail_message(raw)
        except SYNC_EXCEPTIONS as exc:
            self._record_ingest_issue(message_id, "gmail.full", "error", f"parse_gmail_message failed: {exc!r}", raw)
            raise
        if rule and not message_matches_rule(parsed, rule):
            return {}
        attachment_sha1s = self._download_attachments(parsed)
        markdown = message_to_markdown(parsed, attachment_sha1s)
        self.cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
        self.cache.upsert_message(parsed, raw, markdown=markdown, hydrated=True)
        try:
            self.hydrate_raw_rfc822(parsed.gmail_id)
        except SYNC_EXCEPTIONS as exc:
            self._record_ingest_issue(parsed.gmail_id, "gmail.raw", "warning", f"raw RFC822 hydration failed: {exc!r}", raw)
        if update_history_cursor and raw.get("historyId"):
            current = self.cache.get_state("gmail_history_id")
            if not current or int(raw["historyId"]) > int(current):
                self.cache.set_state("gmail_history_id", str(raw["historyId"]))
        CategoryEngine(self.cache).categorize_message(parsed.gmail_id, manual_profiles={})
        self.cache.enqueue_intelligence_job("message", parsed.gmail_id)
        for sha1 in attachment_sha1s:
            self.cache.enqueue_intelligence_job("attachment", sha1)
        return self.cache.get_message(parsed.gmail_id) or {}

    def _record_ingest_issue(
        self, message_id: str, source: str, severity: str, detail: str, artifact: dict[str, Any] = DEFAULT_DICT_ANY
    ) -> None:
        recorder = getattr(self.cache, "record_ingest_issue", None)
        if recorder is None:
            return
        recorder(message_id=message_id, source=source, severity=severity, detail=detail, artifact=artifact)

    def search_gmail(self, query: str, limit: int = 20) -> dict[str, Any]:
        """Search gmail."""
        if not self.gmail_available():
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        labels = self.gmail.list_labels()
        for label in labels:
            self.cache.upsert_label(label)
        ids = self.gmail.search_messages(query, limit=limit)
        hydrated_ids: list[str] = []
        for message_id in ids:
            message = self.cache.get_message(message_id)
            if not message or not message.get("hydrated"):
                message = self.hydrate_message(message_id)
            if message:
                hydrated_ids.append(message["id"])
        self.cache.set_state(
            "last_gmail_search", json.dumps({"query": query, "matched": len(ids), "hydrated": len(hydrated_ids)}, sort_keys=True)
        )
        return {"query": query, "matched": len(ids), "hydrated": len(hydrated_ids), "message_ids": hydrated_ids}

    def search(
        self,
        query: str,
        limit: int = 20,
        after: str = DEFAULT_STR,
        before: str = DEFAULT_STR,
        include_categories: list[str] = DEFAULT_LIST_STR,
        exclude_categories: list[str] = DEFAULT_LIST_STR,
    ) -> dict[str, Any]:
        """Search."""
        live = should_search_gmail(query, after=after, before=before)
        live_result: dict[str, Any] = {}
        live_ids: list[str] = []
        if live:
            gmail_query = query
            if after and f"after:{after}" not in gmail_query:
                gmail_query = f"({gmail_query}) after:{after}"
            if before and f"before:{before}" not in gmail_query:
                gmail_query = f"({gmail_query}) before:{before}"
            try:
                live_result = self.search_gmail(gmail_query, limit=limit)
                live_ids = live_result.get("message_ids", [])
            except SYNC_EXCEPTIONS as exc:
                live_result = {"query": gmail_query, "error": repr(exc), "matched": 0, "hydrated": 0, "message_ids": []}
        if live_ids:
            messages = [message for message_id in live_ids if (message := self.cache.get_message(message_id))]
            messages = _filter_messages(messages, include_categories=include_categories, exclude_categories=exclude_categories)
        else:
            messages = self.cache.text_search(
                query,
                limit=limit,
                after=after,
                before=before,
                include_categories=include_categories,
                exclude_categories=exclude_categories,
            )
        return {"query": query, "source": "gmail+cache" if live else "cache", "live": live_result, "messages": messages}

    def hybrid_search(
        self,
        query: str,
        limit: int = 20,
        after: str = DEFAULT_STR,
        before: str = DEFAULT_STR,
        include_categories: list[str] = DEFAULT_LIST_STR,
        exclude_categories: list[str] = DEFAULT_LIST_STR,
    ) -> dict[str, Any]:
        """Hybrid search."""
        pool_limit = max(limit * 4, 20)
        scored: dict[str, dict[str, Any]] = {}
        filters = HybridFilters(after, before, include_categories, exclude_categories)
        lexical = self.search(
            query,
            limit=pool_limit,
            after=after,
            before=before,
            include_categories=include_categories,
            exclude_categories=exclude_categories,
        )
        _score_lexical_results(scored, lexical["messages"])
        _score_semantic_results(
            scored,
            self.cache,
            _semantic_hybrid_results(self.semantic, query, pool_limit),
            filters,
        )
        _score_graph_results(scored, self.cache, query, pool_limit, filters)
        _finalize_hybrid_scores(scored, self.cache, filters)
        ranked = sorted(scored.values(), key=_hybrid_result_rank, reverse=True)
        return {
            "query": query,
            "source": "hybrid",
            "lexical_source": lexical.get("source"),
            "messages": [item["message"] for item in ranked[:limit]],
            "scores": [
                {"message_id": item["message"]["id"], "score": item["score"], "sources": item["sources"], "evidence": item["evidence"][:3]}
                for item in ranked[:limit]
            ],
        }

    def _download_attachments(self, parsed: ParsedMessage) -> list[str]:
        if not self.gmail_available():
            return []
        sha1s: list[str] = []
        for part in parsed.parts:
            if not part.attachment_id:
                continue
            content = self.gmail.get_attachment(parsed.gmail_id, part.attachment_id)
            stored = self.attachments.put(
                content=content,
                metadata={
                    "gmail": {
                        "message_id": parsed.gmail_id,
                        "thread_id": parsed.thread_id,
                        "part_id": part.part_id,
                        "attachment_id": part.attachment_id,
                        "filename": part.filename,
                        "mime_type": part.mime_type,
                        "size": len(content),
                    },
                    "message": {
                        "subject": parsed.subject,
                        "from": parsed.sender,
                        "to": parsed.recipients,
                        "date": parsed.date,
                        "labels": parsed.label_ids,
                    },
                },
            )
            self.cache.add_attachment(
                {
                    "sha1": stored.sha1,
                    "digest": getattr(stored, "digest", stored.sha1),
                    "message_id": parsed.gmail_id,
                    "part_id": part.part_id,
                    "gmail_attachment_id": part.attachment_id,
                    "filename": part.filename,
                    "mime_type": part.mime_type,
                    "size": len(content),
                    "stored_size": getattr(stored, "stored_size", len(content)),
                    "compression": getattr(stored, "compression", "identity"),
                    "metadata": stored.metadata,
                    "path": str(stored.path),
                }
            )
            sha1s.append(stored.sha1)
        return sha1s

    def apply_label(self, message_id: str, label_id: str) -> dict[str, Any]:
        """Apply label."""
        return self._modify(message_id, add=[label_id], remove=[])

    def remove_label(self, message_id: str, label_id: str) -> dict[str, Any]:
        """Remove label."""
        return self._modify(message_id, add=[], remove=[label_id])

    def archive(self, message_id: str) -> dict[str, Any]:
        """Archive."""
        return self._modify(message_id, add=[], remove=["INBOX"])

    def mark_read(self, message_id: str, *, read: bool = True) -> dict[str, Any]:
        """Mark read."""
        return self._modify(message_id, add=[] if read else ["UNREAD"], remove=["UNREAD"] if read else [])

    def star(self, message_id: str, *, starred: bool = True) -> dict[str, Any]:
        """Star."""
        return self._modify(message_id, add=["STARRED"] if starred else [], remove=[] if starred else ["STARRED"])

    def hydrate_raw_rfc822(self, message_id: str) -> bytes:
        """Hydrate raw rfc822."""
        cached = self.cache.raw_rfc822(message_id)
        if cached:
            return cached
        if not self.gmail_available():
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        raw_message = self.gmail.get_message(message_id, fmt="raw")
        raw = raw_message.get("raw")
        if not raw:
            msg = f"Gmail raw payload missing for {message_id}"
            raise KeyError(msg)
        padding = "=" * (-len(raw) % 4)
        content = base64.urlsafe_b64decode(raw + padding)
        self.cache.set_raw_rfc822(message_id, content)
        return content

    def complete_archive(self, limit: int = 25) -> dict[str, Any]:
        """Complete archive."""
        if not self.gmail_available():
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        ids = self.cache.archive_incomplete_message_ids(limit=limit)
        completed = failed = 0
        errors: list[dict[str, str]] = []
        for message_id in ids:
            try:
                self.hydrate_raw_rfc822(message_id)
                completed += 1
            except SYNC_EXCEPTIONS as exc:
                failed += 1
                errors.append({"message_id": message_id, "error": repr(exc)})
        self.cache.record_operational_event(
            "archive.completed_batch",
            "warning" if failed else "info",
            "archive",
            "",
            "Completed bounded archive RFC822 hydration.",
            {"requested": limit, "matched": len(ids), "completed": completed, "failed": failed},
        )
        return {"requested": limit, "matched": len(ids), "completed": completed, "failed": failed, "errors": errors[:20]}

    def _latest_cached_history_id(self) -> str:
        return self.cache.latest_history_id()

    def _update_history_cursor_from_cache(self) -> None:
        latest = self._latest_cached_history_id()
        if latest:
            self.cache.set_state("gmail_history_id", latest)

    def _modify(self, message_id: str, add: list[str], remove: list[str]) -> dict[str, Any]:
        if not self.gmail_available():
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        result = self.gmail.modify_message(message_id, add_label_ids=add, remove_label_ids=remove)
        self.hydrate_message(message_id)
        return result


def message_matches_rule(message: ParsedMessage, rule: PriorityRule) -> bool:
    """Message matches rule."""
    checks = [
        _headers_match(message, rule),
        _attachment_mimes_match(message, rule),
        _attachment_filenames_match(message, rule),
        _sender_matches(message, rule.senders),
        _sender_matches(message, rule.from_domains),
        _recipients_match(message, rule),
    ]
    return all(checks)


def _headers_match(message: ParsedMessage, rule: PriorityRule) -> bool:
    return not rule.header_contains or all(
        expected.lower() in message.headers.get(name.lower(), "").lower() for name, expected in rule.header_contains.items()
    )


def _attachment_mimes_match(message: ParsedMessage, rule: PriorityRule) -> bool:
    if not rule.attachment_mime:
        return True
    allowed = [value.lower() for value in rule.attachment_mime]
    return any(any(part.mime_type.lower().startswith(value) for value in allowed) for part in message.parts)


def _attachment_filenames_match(message: ParsedMessage, rule: PriorityRule) -> bool:
    if not rule.attachment_filename_contains:
        return True
    needles = [value.lower() for value in rule.attachment_filename_contains]
    filenames = [part.filename.lower() for part in message.parts if part.filename]
    return any(any(needle in filename for needle in needles) for filename in filenames)


def _sender_matches(message: ParsedMessage, values: list[str]) -> bool:
    sender = (message.sender or "").lower()
    return not values or any(value.lower() in sender for value in values)


def _recipients_match(message: ParsedMessage, rule: PriorityRule) -> bool:
    recipients = (message.recipients or "").lower()
    return not rule.recipients or any(value.lower() in recipients for value in rule.recipients)


def should_search_gmail(_query: str, after: str = DEFAULT_STR, before: str = DEFAULT_STR) -> bool:
    """Return whether a query should hydrate from Gmail."""
    return not (after or before)


def _filter_messages(
    messages: list[dict[str, Any]],
    include_categories: list[str],
    exclude_categories: list[str],
) -> list[dict[str, Any]]:
    included = set(include_categories or [])
    excluded = set(exclude_categories or [])
    if not included and not excluded:
        return messages
    result: list[dict[str, Any]] = []
    for message in messages:
        categories = set(message.get("categories", []))
        if included and not categories & included:
            continue
        if excluded and categories & excluded:
            continue
        result.append(message)
    return result


def _merge_hybrid_score(
    scored: dict[str, dict[str, Any]],
    message: dict[str, Any],
    source: str,
    score: float,
    evidence: dict[str, Any] = DEFAULT_DICT_ANY,
) -> None:
    if not message:
        return
    item = scored.setdefault(message["id"], {"message": message, "score": 0.0, "sources": {}, "evidence": []})
    item["score"] += score
    item["sources"][source] = round(item["sources"].get(source, 0.0) + score, 6)
    if evidence:
        item["evidence"].append({"source": source, **evidence})


def _score_lexical_results(scored: dict[str, dict[str, Any]], messages: list[dict[str, Any]]) -> None:
    for rank, message in enumerate(messages, start=1):
        _merge_hybrid_score(scored, message, "lexical", 1.0 / rank)


def _semantic_hybrid_results(semantic: GmeowSemantic, query: str, limit: int) -> list[dict[str, Any]]:
    try:
        return semantic.search(query, limit=limit, source_kind="message")
    except SYNC_EXCEPTIONS:
        return []


def _score_semantic_results(
    scored: dict[str, dict[str, Any]],
    cache: GmeowCache,
    results: list[dict[str, Any]],
    filters: HybridFilters,
) -> None:
    for rank, result in enumerate(results, start=1):
        message = cache.get_message(_semantic_message_id(result))
        if _message_matches_filters(cache, message, filters):
            distance = float(result.get("distance") or 1.0)
            score = max(0.0, 1.0 - distance) + (0.15 / rank)
            _merge_hybrid_score(scored, message, "semantic", score, evidence={"chunk": result.get("document"), "distance": distance})


def _semantic_message_id(result: dict[str, Any]) -> str:
    metadata: object = result.get("metadata") or {}
    metadata_message_id = str(cast(dict[str, Any], metadata).get("message_id") or "") if isinstance(metadata, dict) else ""
    return str(result.get("message_id") or metadata_message_id or str(result.get("id", "")).split(":", 1)[0])


def _score_graph_results(
    scored: dict[str, dict[str, Any]],
    cache: GmeowCache,
    query: str,
    limit: int,
    filters: HybridFilters,
) -> None:
    for message_id, hits in _graph_hit_counts(cache.graph_search(query, limit=limit)).items():
        message = cache.get_message(message_id)
        if _message_matches_filters(cache, message, filters):
            _merge_hybrid_score(scored, message, "graph", min(1.0, 0.2 * hits), evidence={"matching_edges": hits})


def _graph_hit_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    graph_hits: dict[str, int] = {}
    for result in results:
        message_id = result.get("source_message_id")
        if message_id:
            graph_hits[message_id] = graph_hits.get(message_id, 0) + 1
    return graph_hits


def _finalize_hybrid_scores(scored: dict[str, dict[str, Any]], cache: GmeowCache, filters: HybridFilters) -> None:
    for item in scored.values():
        recency = _recency_score(item["message"].get("message_date_iso"))
        category_penalty = _category_penalty(cache, item["message"], filters)
        item["score"] = round(item["score"] + recency - category_penalty, 6)
        item["message"]["hybrid_score"] = item["score"]
        item["message"]["hybrid_sources"] = item["sources"]


def _category_penalty(cache: GmeowCache, message: dict[str, Any], filters: HybridFilters) -> float:
    if cache.category_allowed(message.get("categories", []), filters.include_categories, filters.exclude_categories):
        return 0.0
    return 0.25


def _message_matches_filters(cache: GmeowCache, message: dict[str, Any], filters: HybridFilters) -> bool:
    if not message:
        return False
    date = message.get("message_date_iso") or ""
    if filters.after and (not date or date < filters.after):
        return False
    if filters.before and (not date or date > filters.before):
        return False
    return cache.category_allowed(message.get("categories", []), filters.include_categories, filters.exclude_categories)


def _recency_score(value: str) -> float:
    if not value:
        return 0.0
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return 0.0
    age_seconds = max(0.0, (datetime.now(UTC) - parsed.astimezone(UTC)).total_seconds())
    age_days = age_seconds / 86400.0
    return max(0.0, 0.2 * (1.0 - min(age_days, 30.0) / 30.0))
