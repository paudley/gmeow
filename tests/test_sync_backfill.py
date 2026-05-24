# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Postgres-free unit tests for the backfill helper module.

These tests exercise the pure functions in :mod:`gmeow.sync_backfill` (history extraction,
window arithmetic, query construction, and metadata drift detection) without standing up a real
Postgres instance. They protect the deterministic edges (empty input, year rollovers, missing
keys) that the end-to-end backfill suite implicitly assumes.
"""

from datetime import date
from typing import Any

from gmeow.sync_backfill import (
    BACKFILL_DEFAULT_START,
    backfill_query,
    history_deleted_message_ids,
    history_message_ids,
    metadata_drift,
    next_month,
    parse_window_start,
)


def test_parse_window_start_empty_returns_default() -> None:
    assert parse_window_start("") == BACKFILL_DEFAULT_START


def test_parse_window_start_truncates_to_month() -> None:
    assert parse_window_start("2026-05-15") == date(2026, 5, 1)


def test_parse_window_start_already_first_of_month_is_idempotent() -> None:
    assert parse_window_start("2026-05-01") == date(2026, 5, 1)


def test_next_month_advances_within_year() -> None:
    assert next_month(date(2026, 5, 1)) == date(2026, 6, 1)


def test_next_month_rolls_over_year_boundary() -> None:
    assert next_month(date(2026, 12, 1)) == date(2027, 1, 1)


def test_backfill_query_brackets_the_window() -> None:
    assert backfill_query(date(2026, 5, 1)) == "in:anywhere after:2026/04/30 before:2026/06/01"


def test_backfill_query_handles_december_window() -> None:
    assert backfill_query(date(2026, 12, 1)) == "in:anywhere after:2026/11/30 before:2027/01/01"


def test_history_message_ids_empty_record_list() -> None:
    assert history_message_ids([]) == []


def test_history_message_ids_collects_unique_ids_across_keys() -> None:
    records: list[dict[str, Any]] = [
        {"messagesAdded": [{"message": {"id": "m1"}}, {"message": {"id": "m2"}}]},
        {"labelsAdded": [{"message": {"id": "m2"}}, {"message": {"id": "m3"}}]},
        {"labelsRemoved": [{"message": {"id": "m3"}}]},
        {"messages": [{"id": "m4"}]},
    ]
    assert history_message_ids(records) == ["m1", "m2", "m3", "m4"]


def test_history_deleted_message_ids_pulls_from_messages_deleted() -> None:
    records: list[dict[str, Any]] = [
        {"messagesDeleted": [{"message": {"id": "d1"}}, {"message": {"id": "d2"}}]},
        {"messagesDeleted": [{"message": {"id": "d2"}}]},
    ]
    assert history_deleted_message_ids(records) == ["d1", "d2"]


def test_metadata_drift_reports_no_change_when_fields_match() -> None:
    cached = {"historyId": "100", "threadId": "t1", "internalDate": "1700000000000", "labelIds": ["INBOX"]}
    metadata = {"historyId": "100", "threadId": "t1", "internalDate": "1700000000000", "labelIds": ["INBOX"]}
    assert metadata_drift(cached, metadata) is False


def test_metadata_drift_detects_history_id_change() -> None:
    assert metadata_drift({"historyId": "100"}, {"historyId": "101"}) is True


def test_metadata_drift_detects_label_change() -> None:
    cached = {"labelIds": ["INBOX"]}
    metadata = {"labelIds": ["INBOX", "STARRED"]}
    assert metadata_drift(cached, metadata) is True


def test_metadata_drift_detects_new_headers_when_cached_missing() -> None:
    cached: dict[str, Any] = {}
    metadata = {"payload": {"headers": [{"name": "Subject", "value": "Hello"}]}}
    assert metadata_drift(cached, metadata) is True
