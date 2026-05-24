# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""IMAP projection helpers for PgCache.

This module exposes the cache surface used by the read-only IMAP server, including mailbox
listings, message envelopes, and RFC822 byte fetches. It keeps the IMAP-shaped queries close to
the Postgres cache while reusing the shared SQL helpers in pg_cache_helpers.
"""

from collections.abc import Iterator
from typing import Any, Protocol, Self

from .pg_cache_helpers import imap_mailbox_name


class _ImapConnection(Protocol):
    """Connection surface used by IMAP projection helpers."""

    def __enter__(self) -> Self:
        """Enter connection context."""
        ...

    def __exit__(self, *args: object) -> None:
        """Exit connection context."""
        ...

    def execute(self, query: object, params: object = ()) -> "_ImapResult":
        """Execute SQL."""
        ...


class _ImapResult(Protocol):
    """Result surface used by IMAP projection helpers."""

    def fetchone(self) -> dict[str, Any]:
        """Fetch one row."""
        ...

    def fetchall(self) -> list[dict[str, Any]]:
        """Fetch all rows."""
        ...

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Iterate rows."""
        ...


class _ImapObjectStore(Protocol):
    """Object store surface used by IMAP helpers."""

    def get(self, digest: str, compression: str = "identity") -> bytes:
        """Read object bytes."""
        ...


class _ImapCache(Protocol):
    """Cache surface used by IMAP helpers."""

    objects: _ImapObjectStore

    def connection(self) -> _ImapConnection:
        """Open a database connection."""
        ...

    def refresh_imap_mailboxes(self) -> dict[str, int]:
        """Refresh IMAP mailboxes."""
        ...

    def imap_mailbox(self, name: str) -> dict[str, Any]:
        """Return an IMAP mailbox or an empty dict."""
        ...

    def imap_mailboxes(self) -> list[dict[str, Any]]:
        """Return IMAP mailboxes."""
        ...


def refresh_imap_mailboxes(cache: _ImapCache) -> dict[str, int]:
    """Refresh IMAP mailboxes from labels and archived messages."""
    with cache.connection() as conn:
        labels = [dict(row) for row in conn.execute("SELECT id, name FROM labels ORDER BY name")]
        for label in labels:
            name = imap_mailbox_name(label["name"] or label["id"])
            row = conn.execute(
                """
                INSERT INTO imap_mailboxes(name, label_id)
                VALUES(%s, %s)
                ON CONFLICT(name) DO UPDATE SET label_id=excluded.label_id, updated_at=now()
                RETURNING id
                """,
                (name, label["id"]),
            ).fetchone()
            mailbox_id = row["id"]
            messages = conn.execute(
                """
                SELECT id FROM messages
                WHERE archive_state IN ('complete', 'tombstoned')
                  AND raw_rfc822_digest IS NOT NULL
                  AND label_ids ? %s
                ORDER BY COALESCE(message_ts, updated_at), id
                """,
                (label["id"],),
            ).fetchall()
            for message in messages:
                conn.execute(
                    """
                    INSERT INTO imap_message_uids(mailbox_id, message_id)
                    VALUES(%s, %s)
                    ON CONFLICT(mailbox_id, message_id) DO NOTHING
                    """,
                    (mailbox_id, message["id"]),
                )
    return {"mailboxes": len(labels)}


def imap_mailboxes(cache: _ImapCache) -> list[dict[str, Any]]:
    """List IMAP mailboxes."""
    cache.refresh_imap_mailboxes()
    with cache.connection() as conn:
        rows = conn.execute(
            """
            SELECT mb.id, mb.name, mb.label_id, mb.uidvalidity,
                   COUNT(mu.message_id) AS messages,
                   COALESCE(MAX(mu.uid), 0) AS uidnext_base
            FROM imap_mailboxes mb
            LEFT JOIN imap_message_uids mu ON mu.mailbox_id = mb.id
            GROUP BY mb.id, mb.name, mb.label_id, mb.uidvalidity
            ORDER BY mb.name
            """
        )
        return [{**dict(row), "uidnext": int(row["uidnext_base"] or 0) + 1} for row in rows]


def imap_mailbox(cache: _ImapCache, name: str) -> dict[str, Any]:
    """Find one IMAP mailbox by name."""
    cache.refresh_imap_mailboxes()
    with cache.connection() as conn:
        row = conn.execute(
            """
            SELECT mb.id, mb.name, mb.label_id, mb.uidvalidity,
                   COUNT(mu.message_id) AS messages,
                   COALESCE(MAX(mu.uid), 0) AS uidnext_base
            FROM imap_mailboxes mb
            LEFT JOIN imap_message_uids mu ON mu.mailbox_id = mb.id
            WHERE lower(mb.name) = lower(%s)
            GROUP BY mb.id, mb.name, mb.label_id, mb.uidvalidity
            """,
            (name,),
        ).fetchone()
        if not row:
            return {}
        item = dict(row)
        item["uidnext"] = int(item.pop("uidnext_base") or 0) + 1
        return item


def imap_messages(cache: _ImapCache, mailbox: str, limit: int = 0) -> list[dict[str, Any]]:
    """List messages visible in an IMAP mailbox."""
    box = cache.imap_mailbox(mailbox)
    if not box:
        return []
    query = """
        SELECT mu.uid, m.id, m.subject, m.sender, m.recipients, m.message_date, m.message_ts, m.raw_rfc822_digest, co.compression
        FROM imap_message_uids mu
        JOIN messages m ON m.id = mu.message_id
        JOIN content_objects co ON co.digest = m.raw_rfc822_digest
        WHERE mu.mailbox_id = %s
          AND m.archive_state IN ('complete', 'tombstoned')
          AND m.raw_rfc822_digest IS NOT NULL
        ORDER BY mu.uid
    """
    params: tuple[Any, ...] = (box["id"],)
    if limit > 0:
        query += " LIMIT %s"
        params = (box["id"], limit)
    with cache.connection() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def imap_message_bytes(cache: _ImapCache, mailbox: str, uid: int) -> bytes:
    """Load a raw RFC822 message by mailbox UID."""
    box = cache.imap_mailbox(mailbox)
    if not box:
        return b""
    with cache.connection() as conn:
        row = conn.execute(
            """
            SELECT m.raw_rfc822_digest, co.compression
            FROM imap_message_uids mu
            JOIN messages m ON m.id = mu.message_id
            JOIN content_objects co ON co.digest = m.raw_rfc822_digest
            WHERE mu.mailbox_id = %s AND mu.uid = %s
            """,
            (box["id"], uid),
        ).fetchone()
    if not row:
        return b""
    return cache.objects.get(row["raw_rfc822_digest"], compression=row["compression"])


def imap_status(cache: _ImapCache) -> dict[str, Any]:
    """Return IMAP projection status."""
    boxes = cache.imap_mailboxes()
    return {
        "mailboxes": len(boxes),
        "messages": sum(int(box["messages"] or 0) for box in boxes),
        "read_only": True,
        "uid_model": "per_label_mailbox",
    }
