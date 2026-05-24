# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Helper routines for Gmail history and oldest-first backfill sync.

The functions here keep Gmail pagination, monthly window math, and lightweight metadata drift
checks out of the main SyncService implementation. They operate on parsed Gmail API dictionaries
and return deterministic values so interrupted backfills can resume from persisted checkpoints.
"""

from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

BACKFILL_DEFAULT_START = date(1970, 1, 1)
DECEMBER = 12


def history_message_ids(records: list[dict[str, Any]]) -> list[str]:
    """Extract unique message ids from Gmail history records."""
    ids: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in ["messagesAdded", "labelsAdded", "labelsRemoved"]:
            for item in _history_items(record, key):
                message = _history_message(item)
                message_id = str(message.get("id") or "")
                if message_id and message_id not in seen:
                    seen.add(message_id)
                    ids.append(message_id)
        for message in _history_items(record, "messages"):
            message_id = str(message.get("id") or "")
            if message_id and message_id not in seen:
                seen.add(message_id)
                ids.append(message_id)
    return ids


def history_deleted_message_ids(records: list[dict[str, Any]]) -> list[str]:
    """Extract unique deleted message ids from Gmail history records."""
    ids: list[str] = []
    seen: set[str] = set()
    for record in records:
        for item in _history_items(record, "messagesDeleted"):
            message = _history_message(item)
            message_id = str(message.get("id") or "")
            if message_id and message_id not in seen:
                seen.add(message_id)
                ids.append(message_id)
    return ids


def parse_window_start(value: str) -> date:
    """Parse the persisted monthly backfill window start."""
    if not value:
        return BACKFILL_DEFAULT_START
    parsed = date.fromisoformat(value)
    return date(parsed.year, parsed.month, 1)


def current_month_start() -> date:
    """Return the first day of the current UTC month."""
    today = datetime.now(UTC).date()
    return date(today.year, today.month, 1)


def next_month(value: date) -> date:
    """Return the first day of the month after value."""
    if value.month == DECEMBER:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def backfill_query(window_start: date) -> str:
    """Build the Gmail query for one monthly backfill window."""
    after = window_start - timedelta(days=1)
    before = next_month(window_start)
    return f"in:anywhere after:{_gmail_date(after)} before:{_gmail_date(before)}"


def metadata_drift(cached_raw: dict[str, Any], metadata: dict[str, Any]) -> bool:
    """Return whether lightweight Gmail metadata differs from cached raw metadata."""
    for key in ["historyId", "threadId", "internalDate"]:
        if metadata.get(key) and str(cached_raw.get(key) or "") != str(metadata.get(key) or ""):
            return True
    metadata_labels = _string_values(metadata.get("labelIds", []))
    cached_labels = _string_values(cached_raw.get("labelIds", []))
    if metadata_labels and cached_labels != metadata_labels:
        return True
    metadata_headers = _payload_headers(metadata.get("payload") or {})
    cached_headers = _payload_headers(cached_raw.get("payload") or {})
    return bool(metadata_headers and not cached_headers)


def _history_items(record: dict[str, Any], key: str) -> list[dict[str, Any]]:
    raw_items: object = record.get(key, []) or []
    if not isinstance(raw_items, list):
        return []
    items = cast(list[object], raw_items)
    return [cast(dict[str, Any], item) for item in items if isinstance(item, dict)]


def _history_message(item: dict[str, Any]) -> dict[str, Any]:
    message: object = item.get("message") or {}
    return cast(dict[str, Any], message) if isinstance(message, dict) else {}


def _gmail_date(value: date) -> str:
    return value.strftime("%Y/%m/%d")


def _string_values(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    values = cast(list[object], value)
    return sorted(str(item) for item in values)


def _payload_headers(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    headers: object = cast(dict[str, Any], payload).get("headers", []) or []
    if not isinstance(headers, list):
        return []
    header_items = cast(list[object], headers)
    return [cast(dict[str, Any], header) for header in header_items if isinstance(header, dict)]
