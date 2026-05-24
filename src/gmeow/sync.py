# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide sync functionality for Gmeow."""

from __future__ import annotations

import base64
import json
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from .categories import CategoryEngine
from .config import GmeowConfig, PriorityRule
from .gmail import GmailClient
from .markdown import message_to_markdown
from .parser import ParsedMessage, parse_gmail_message

SYNC_EXCEPTIONS = (RuntimeError, ValueError, KeyError, TypeError, OSError, TimeoutError)


def _priority_rule_rank(rule: PriorityRule) -> int:
    return rule.priority


def _hybrid_result_rank(item: dict[str, Any]) -> tuple[float, str]:
    return item["score"], item["message"].get("message_date_iso") or ""


class SyncService:
    """Represent SyncService data and behavior."""

    def __init__(
        self,
        config: GmeowConfig,
        cache: object,
        attachments: object,
        semantic: object,
        gmail: GmailClient | None = None,
        graph: object | None = None,
    ) -> None:
        """Initialize SyncService."""
        self.config = config
        self.cache = cache
        self.attachments = attachments
        self.semantic = semantic
        self.gmail = gmail
        self.graph = graph

    def sync_priority(self, limit_per_rule: int = 100) -> dict[str, Any]:
        """Sync priority."""
        if self.gmail is None:
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        run_id = self.cache.start_sync_run("priority", request={"limit_per_rule": limit_per_rule})
        labels = self.gmail.list_labels()
        try:
            for label in labels:
                self.cache.upsert_label(label)
            summary = {"labels": len(labels), "rules": [], "messages": 0}
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
        if self.gmail is None:
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        start = self.cache.get_state("gmail_history_id") or self._latest_cached_history_id()
        if not start:
            msg = "No Gmail history cursor is available. Run priority sync or hydrate at least one message first."
            raise RuntimeError(msg)
        run_id = self.cache.start_sync_run("history", start_cursor=str(start), request={"limit": limit})
        try:
            result = self.gmail.list_history(start, limit=limit)
            message_ids = _history_message_ids(result.get("history", []))
            deleted_ids = _history_deleted_message_ids(result.get("history", []))
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

    def hydrate_message(
        *, self, message_id: str, rule: PriorityRule | None = None, update_history_cursor: bool = True
    ) -> dict[str, Any] | None:
        """Hydrate message."""
        if self.gmail is None:
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
        if rule is not None and not message_matches_rule(parsed, rule):
            return None
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
            if current is None or int(raw["historyId"]) > int(current):
                self.cache.set_state("gmail_history_id", str(raw["historyId"]))
        CategoryEngine(self.cache).categorize_message(parsed.gmail_id, manual_profiles={})
        self.cache.enqueue_intelligence_job("message", parsed.gmail_id)
        for sha1 in attachment_sha1s:
            self.cache.enqueue_intelligence_job("attachment", sha1)
        return self.cache.get_message(parsed.gmail_id) or {}

    def _record_ingest_issue(
        self, message_id: str, source: str, severity: str, detail: str, artifact: dict[str, Any] | None = None
    ) -> None:
        recorder = getattr(self.cache, "record_ingest_issue", None)
        if recorder is None:
            return
        recorder(message_id=message_id, source=source, severity=severity, detail=detail, artifact=artifact)

    def search_gmail(self, query: str, limit: int = 20) -> dict[str, Any]:
        """Search gmail."""
        if self.gmail is None:
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        labels = self.gmail.list_labels()
        for label in labels:
            self.cache.upsert_label(label)
        ids = self.gmail.search_messages(query, limit=limit)
        hydrated_ids = []
        for message_id in ids:
            message = self.cache.get_message(message_id)
            if message is None or not message.get("hydrated"):
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
        after: str | None = None,
        before: str | None = None,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
    ) -> dict[str, Any]:
        """Search."""
        live = should_search_gmail(query, after=after, before=before)
        live_result = None
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
            messages = [message for message_id in live_ids if (message := self.cache.get_message(message_id)) is not None]
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
        after: str | None = None,
        before: str | None = None,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
    ) -> dict[str, Any]:
        """Hybrid search."""
        pool_limit = max(limit * 4, 20)
        scored: dict[str, dict[str, Any]] = {}

        lexical = self.search(
            query,
            limit=pool_limit,
            after=after,
            before=before,
            include_categories=include_categories,
            exclude_categories=exclude_categories,
        )
        for rank, message in enumerate(lexical["messages"], start=1):
            _merge_hybrid_score(scored, message, "lexical", 1.0 / rank)

        semantic_results: list[dict[str, Any]] = []
        try:
            semantic_results = self.semantic.search(query, limit=pool_limit, source_kind="message")
        except SYNC_EXCEPTIONS:
            semantic_results = []
        for rank, result in enumerate(semantic_results, start=1):
            message_id = (
                result.get("message_id") or (result.get("metadata") or {}).get("message_id") or str(result.get("id", "")).split(":", 1)[0]
            )
            message = self.cache.get_message(message_id)
            if not _message_matches_filters(self.cache, message, after, before, include_categories, exclude_categories):
                continue
            distance = float(result.get("distance") or 1.0)
            score = max(0.0, 1.0 - distance) + (0.15 / rank)
            _merge_hybrid_score(scored, message, "semantic", score, evidence={"chunk": result.get("document"), "distance": distance})

        graph_results = self.cache.graph_search(query, limit=pool_limit)
        graph_hits: dict[str, int] = {}
        for result in graph_results:
            message_id = result.get("source_message_id")
            if message_id:
                graph_hits[message_id] = graph_hits.get(message_id, 0) + 1
        for message_id, hits in graph_hits.items():
            message = self.cache.get_message(message_id)
            if not _message_matches_filters(self.cache, message, after, before, include_categories, exclude_categories):
                continue
            _merge_hybrid_score(scored, message, "graph", min(1.0, 0.2 * hits), evidence={"matching_edges": hits})

        for item in scored.values():
            recency = _recency_score(item["message"].get("message_date_iso"))
            category_penalty = (
                0.25
                if not self.cache.category_allowed(item["message"].get("categories", []), include_categories, exclude_categories)
                else 0.0
            )
            item["score"] = round(item["score"] + recency - category_penalty, 6)
            item["message"]["hybrid_score"] = item["score"]
            item["message"]["hybrid_sources"] = item["sources"]

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
        if self.gmail is None:
            return []
        sha1s: list[str] = []
        for part in parsed.parts:
            if not part.attachment_id:
                continue
            content = self.gmail.get_attachment(parsed.gmail_id, part.attachment_id)
            stored = self.attachments.put(
                content,
                {
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
        if cached is not None:
            return cached
        if self.gmail is None:
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
        if self.gmail is None:
            msg = "Gmail client is not configured."
            raise RuntimeError(msg)
        ids = self.cache.archive_incomplete_message_ids(limit=limit)
        completed = failed = 0
        errors = []
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
            None,
            "Completed bounded archive RFC822 hydration.",
            {"requested": limit, "matched": len(ids), "completed": completed, "failed": failed},
        )
        return {"requested": limit, "matched": len(ids), "completed": completed, "failed": failed, "errors": errors[:20]}

    def _latest_cached_history_id(self) -> str | None:
        return self.cache.latest_history_id()

    def _update_history_cursor_from_cache(self) -> None:
        latest = self._latest_cached_history_id()
        if latest:
            self.cache.set_state("gmail_history_id", latest)

    def _modify(self, message_id: str, add: list[str], remove: list[str]) -> dict[str, Any]:
        if self.gmail is None:
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


def should_search_gmail(_query: str, after: str | None = None, before: str | None = None) -> bool:
    """Return whether a query should hydrate from Gmail."""
    return not (after or before)


def _history_message_ids(records: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in ["messagesAdded", "labelsAdded", "labelsRemoved"]:
            for item in record.get(key, []) or []:
                message = item.get("message") or {}
                message_id = message.get("id")
                if message_id and message_id not in seen:
                    seen.add(message_id)
                    ids.append(message_id)
        for message in record.get("messages", []) or []:
            message_id = message.get("id")
            if message_id and message_id not in seen:
                seen.add(message_id)
                ids.append(message_id)
    return ids


def _history_deleted_message_ids(records: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for record in records:
        for item in record.get("messagesDeleted", []) or []:
            message = item.get("message") or {}
            message_id = message.get("id")
            if message_id and message_id not in seen:
                seen.add(message_id)
                ids.append(message_id)
    return ids


def _filter_messages(
    messages: list[dict[str, Any]],
    include_categories: list[str] | None,
    exclude_categories: list[str] | None,
) -> list[dict[str, Any]]:
    if include_categories is None and exclude_categories is None:
        return messages
    included = set(include_categories or [])
    excluded = set(exclude_categories or [])
    result = []
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
    message: dict[str, Any] | None,
    source: str,
    score: float,
    evidence: dict[str, Any] | None = None,
) -> None:
    if not message:
        return
    item = scored.setdefault(message["id"], {"message": message, "score": 0.0, "sources": {}, "evidence": []})
    item["score"] += score
    item["sources"][source] = round(item["sources"].get(source, 0.0) + score, 6)
    if evidence:
        item["evidence"].append({"source": source, **evidence})


def _message_matches_filters(
    cache: object,
    message: dict[str, Any] | None,
    after: str | None,
    before: str | None,
    include_categories: list[str] | None,
    exclude_categories: list[str] | None,
) -> bool:
    if not message:
        return False
    date = message.get("message_date_iso") or ""
    if after and (not date or date < after):
        return False
    if before and (not date or date > before):
        return False
    return cache.category_allowed(message.get("categories", []), include_categories, exclude_categories)


def _recency_score(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return 0.0
    age_seconds = max(0.0, (datetime.now(UTC) - parsed.astimezone(UTC)).total_seconds())
    age_days = age_seconds / 86400.0
    return max(0.0, 0.2 * (1.0 - min(age_days, 30.0) / 30.0))
