# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""IMAP projection helpers for PgCache."""

from __future__ import annotations

from typing import Any

from .pg_cache_helpers import imap_mailbox_name


def refresh_imap_mailboxes(cache: Any) -> dict[str, int]:
    """Refresh IMAP mailboxes from labels and archived messages."""
    with cache._connect() as conn:
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


def imap_mailboxes(cache: Any) -> list[dict[str, Any]]:
    """List IMAP mailboxes."""
    cache.refresh_imap_mailboxes()
    with cache._connect() as conn:
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


def imap_mailbox(cache: Any, name: str) -> dict[str, Any] | None:
    """Find one IMAP mailbox by name."""
    cache.refresh_imap_mailboxes()
    with cache._connect() as conn:
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
            return None
        item = dict(row)
        item["uidnext"] = int(item.pop("uidnext_base") or 0) + 1
        return item


def imap_messages(cache: Any, mailbox: str, limit: int | None = None) -> list[dict[str, Any]]:
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
    if limit is not None:
        query += " LIMIT %s"
        params = (box["id"], limit)
    with cache._connect() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def imap_message_bytes(cache: Any, mailbox: str, uid: int) -> bytes | None:
    """Load a raw RFC822 message by mailbox UID."""
    box = cache.imap_mailbox(mailbox)
    if not box:
        return None
    with cache._connect() as conn:
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
        return None
    return cache.objects.get(row["raw_rfc822_digest"], compression=row["compression"])


def imap_status(cache: Any) -> dict[str, Any]:
    """Return IMAP projection status."""
    boxes = cache.imap_mailboxes()
    return {
        "mailboxes": len(boxes),
        "messages": sum(int(box["messages"] or 0) for box in boxes),
        "read_only": True,
        "uid_model": "per_label_mailbox",
    }
