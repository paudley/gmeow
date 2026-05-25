# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Unit tests for the shared Gmail mutation dispatch module.

The wrappers in :mod:`gmeow.gmail_actions` exist so the HTTP API and the MCP tool surface route
all Gmail mutations through the same code path. These tests use a recording stand-in for the
mutation Protocol to confirm each wrapper forwards its arguments verbatim and returns the
underlying service result unchanged.
"""

from dataclasses import dataclass, field
from typing import Any

from gmeow.gmail_actions import (
    apply_message_label,
    archive_message,
    remove_message_label,
    set_message_read_state,
    set_message_star_state,
)


@dataclass(slots=True)
class _RecordingSync:
    """Capture sync method calls so tests can assert wrapper dispatch."""

    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = field(default_factory=list)

    def _result(self, name: str) -> dict[str, Any]:
        return {"called": name, "ok": True}

    def apply_label(self, message_id: str, label_id: str) -> dict[str, Any]:
        self.calls.append(("apply_label", (message_id, label_id), {}))
        return self._result("apply_label")

    def remove_label(self, message_id: str, label_id: str) -> dict[str, Any]:
        self.calls.append(("remove_label", (message_id, label_id), {}))
        return self._result("remove_label")

    def archive(self, message_id: str) -> dict[str, Any]:
        self.calls.append(("archive", (message_id,), {}))
        return self._result("archive")

    def mark_read(self, message_id: str, *, read: bool = True) -> dict[str, Any]:
        self.calls.append(("mark_read", (message_id,), {"read": read}))
        return self._result("mark_read")

    def star(self, message_id: str, *, starred: bool = True) -> dict[str, Any]:
        self.calls.append(("star", (message_id,), {"starred": starred}))
        return self._result("star")


def test_apply_message_label_forwards_arguments_and_result() -> None:
    sync = _RecordingSync()
    result = apply_message_label(sync, "m1", "Label_42")
    assert sync.calls == [("apply_label", ("m1", "Label_42"), {})]
    assert result == {"called": "apply_label", "ok": True}


def test_remove_message_label_forwards_arguments() -> None:
    sync = _RecordingSync()
    remove_message_label(sync, "m2", "Label_X")
    assert sync.calls == [("remove_label", ("m2", "Label_X"), {})]


def test_archive_message_forwards_message_id() -> None:
    sync = _RecordingSync()
    archive_message(sync, "m3")
    assert sync.calls == [("archive", ("m3",), {})]


def test_set_message_read_state_threads_read_flag() -> None:
    sync = _RecordingSync()
    set_message_read_state(sync, "m4", read=False)
    assert sync.calls == [("mark_read", ("m4",), {"read": False})]


def test_set_message_star_state_threads_starred_flag() -> None:
    sync = _RecordingSync()
    set_message_star_state(sync, "m5", starred=True)
    assert sync.calls == [("star", ("m5",), {"starred": True})]
