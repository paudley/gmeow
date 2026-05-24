# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Shared Gmail message actions for API and MCP surfaces.

The HTTP API and MCP tools expose the same Gmail mutations with different request framing. This
module keeps the mutation dispatch in one place so both surfaces preserve behavior without copying
the service calls.
"""

from typing import Any, Protocol


class _GmailMutations(Protocol):
    """Structural surface of the Gmail mutation methods these wrappers dispatch to."""

    def apply_label(self, message_id: str, label_id: str) -> dict[str, Any]:
        """Apply a Gmail label."""
        ...

    def remove_label(self, message_id: str, label_id: str) -> dict[str, Any]:
        """Remove a Gmail label."""
        ...

    def archive(self, message_id: str) -> dict[str, Any]:
        """Archive a Gmail message."""
        ...

    def mark_read(self, message_id: str, *, read: bool = True) -> dict[str, Any]:
        """Set Gmail read state."""
        ...

    def star(self, message_id: str, *, starred: bool = True) -> dict[str, Any]:
        """Set Gmail star state."""
        ...


def apply_message_label(sync: _GmailMutations, message_id: str, label_id: str) -> dict[str, Any]:
    """Apply a Gmail label to a message."""
    return sync.apply_label(message_id, label_id)


def remove_message_label(sync: _GmailMutations, message_id: str, label_id: str) -> dict[str, Any]:
    """Remove a Gmail label from a message."""
    return sync.remove_label(message_id, label_id)


def archive_message(sync: _GmailMutations, message_id: str) -> dict[str, Any]:
    """Archive a Gmail message."""
    return sync.archive(message_id)


def set_message_read_state(sync: _GmailMutations, message_id: str, *, read: bool) -> dict[str, Any]:
    """Set Gmail message read state."""
    return sync.mark_read(message_id, read=read)


def set_message_star_state(sync: _GmailMutations, message_id: str, *, starred: bool) -> dict[str, Any]:
    """Set Gmail message star state."""
    return sync.star(message_id, starred=starred)
