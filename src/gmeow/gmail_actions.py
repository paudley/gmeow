# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Shared Gmail message actions for API and MCP surfaces.

The HTTP API and MCP tools expose the same Gmail mutations with different request framing. This
module keeps the mutation dispatch in one place so both surfaces preserve behavior without copying
the service calls.
"""

from typing import Any

from .sync import SyncService


def apply_message_label(sync: SyncService, message_id: str, label_id: str) -> dict[str, Any]:
    """Apply a Gmail label to a message."""
    return sync.apply_label(message_id, label_id)


def remove_message_label(sync: SyncService, message_id: str, label_id: str) -> dict[str, Any]:
    """Remove a Gmail label from a message."""
    return sync.remove_label(message_id, label_id)


def archive_message(sync: SyncService, message_id: str) -> dict[str, Any]:
    """Archive a Gmail message."""
    return sync.archive(message_id)


def set_message_read_state(sync: SyncService, message_id: str, *, read: bool) -> dict[str, Any]:
    """Set Gmail message read state."""
    return sync.mark_read(message_id, read=read)


def set_message_star_state(sync: SyncService, message_id: str, *, starred: bool) -> dict[str, Any]:
    """Set Gmail message star state."""
    return sync.star(message_id, starred=starred)
