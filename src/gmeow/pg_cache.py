# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Persist and query the PostgreSQL cache for Gmeow.

The module owns message, attachment, graph, category, archive, sync, and operational database
operations. It provides the shared cache surface used by the API, CLI, MCP server, maintenance
scheduler, and sync service.
"""

import json
from collections.abc import Iterator
from email.utils import getaddresses
from pathlib import Path
from typing import Any, Protocol, Self, cast

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from . import pg_cache_archive, pg_cache_imap, pg_cache_jobs
from .cache import (
    DEFAULT_EXCLUDED_CATEGORIES,
    categorize_message,
)
from .cache import (
    attachment_text_from_metadata as _attachment_text_from_metadata,
)
from .cache import (
    best_contact_name as _best_contact_name,
)
from .cache import (
    filter_by_date as _filter_by_date,
)
from .cache import (
    graph_node_label as _graph_node_label,
)
from .cache import (
    graph_node_profile as _graph_node_profile,
)
from .cache import (
    parse_local_query as _parse_local_query,
)
from .cache import (
    parse_message_date as _parse_message_date,
)
from .cache import (
    person_key as _person_key,
)
from .cache import (
    title_category as _title_category,
)
from .categories import deterministic_assignments
from .db import run_migrations
from .graph import DOAP, ONTOLOGY_PROFILE, RDF_TYPE, GraphProjector, WeightedGraphProjector
from .object_store import ObjectStore, StoredObject
from .parser import ParsedMessage
from .pg_cache_helpers import (
    PROJECT_TABLES,
)
from .pg_cache_helpers import (
    address_filter_value as _address_filter_value,
)
from .pg_cache_helpers import (
    age_sql as _age_sql,
)
from .pg_cache_helpers import (
    algorithm_profile_allowed as _algorithm_profile_allowed,
)
from .pg_cache_helpers import (
    apply_address_filters as _apply_address_filters,
)
from .pg_cache_helpers import (
    apply_category_filters_sql as _apply_category_filters_sql,
)
from .pg_cache_helpers import (
    apply_date_filters as _apply_date_filters,
)
from .pg_cache_helpers import (
    apply_fts_filter as _apply_fts_filter,
)
from .pg_cache_helpers import (
    apply_subject_label_filters as _apply_subject_label_filters,
)
from .pg_cache_helpers import (
    kind_prefixes as _kind_prefixes,
)
from .pg_cache_helpers import (
    message_search_fields as _message_search_fields,
)
from .pg_cache_helpers import (
    message_search_text as _message_search_text,
)
from .pg_cache_helpers import (
    normalize_kind as _normalize_kind,
)
from .pg_cache_helpers import (
    profile_allowed as _profile_allowed,
)
from .pg_cache_helpers import (
    read_only_cypher as _read_only_cypher,
)
from .text_index import TantivyMessageIndex

DEFAULT_INT = cast(int, None)
DEFAULT_LIST_STR = cast(list[str], None)
DEFAULT_STR = cast(str, None)
DEFAULT_DICT_ANY = cast(dict[str, Any], None)
DEFAULT_INGEST_ARTIFACT = cast(dict[str, Any] | bytes | str, None)
DEFAULT_TARGETS = cast(list[tuple[str, str]], None)

PG_CACHE_EXCEPTIONS = (psycopg.Error, OSError, ValueError, KeyError, TypeError, RuntimeError)
MIN_GRAPH_CYCLE_LENGTH = 2


class _PgContext(Protocol):
    """Context manager returned by psycopg helper APIs."""

    def __enter__(self) -> object:
        """Enter the context manager."""
        ...

    def __exit__(self, *args: object) -> None:
        """Exit the context manager."""
        ...


class _PgResult(Protocol):
    """Database result surface used by the cache."""

    rowcount: int

    def fetchone(self) -> dict[str, Any]:
        """Fetch one row."""
        ...

    def fetchall(self) -> list[dict[str, Any]]:
        """Fetch all rows."""
        ...

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Iterate result rows."""
        ...


class _PgCursor(_PgContext, Protocol):
    """Database cursor surface used by the cache."""

    def __enter__(self) -> Self:
        """Enter the cursor context manager."""
        ...

    def execute(self, query: object, params: object = ()) -> _PgResult:
        """Execute a statement."""
        ...

    def executemany(self, query: object, _params_seq: object) -> None:
        """Execute a statement for several parameter sets."""
        ...


class _PgConnection(_PgContext, Protocol):
    """Database connection surface used by the cache."""

    def __enter__(self) -> Self:
        """Enter the connection context manager."""
        ...

    def execute(self, query: object, params: object = ()) -> _PgResult:
        """Execute a statement."""
        ...

    def cursor(self) -> _PgCursor:
        """Open a cursor."""
        ...

    def transaction(self) -> _PgContext:
        """Open a transaction context."""
        ...


def _where_sql(conditions: list[str], *, default: str = "true") -> sql.Composable:
    if not conditions:
        return sql.SQL(cast(Any, default))
    return sql.SQL(" AND ").join(sql.SQL(cast(Any, condition)) for condition in conditions)


def _where_clause_sql(conditions: list[str]) -> sql.Composable:
    if not conditions:
        return sql.SQL("")
    return sql.SQL("WHERE ").join([sql.SQL(""), _where_sql(conditions)])


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


class PgCache:
    """Represent PgCache data and behavior."""

    def __init__(self, dsn: str, objects: ObjectStore, text_index: TantivyMessageIndex) -> None:
        """Initialize PgCache."""
        self.dsn = dsn
        self.objects = objects
        self.text_index = text_index
        self.age_graph_error: str = DEFAULT_STR
        run_migrations(dsn)
        self._pool = ConnectionPool(
            conninfo=dsn,
            kwargs={"row_factory": dict_row},
            min_size=1,
            max_size=10,
            open=True,
            name="gmeow-cache",
        )
        self._ensure_age_graph()
        self.ensure_default_categories()

    def _connect(self) -> Any:
        return self._pool.connection()

    def connection(self) -> _PgConnection:
        """Open a row-dict database connection for helper modules."""
        return self._connect()

    def close(self) -> None:
        """Close."""
        self._pool.close()

    def _ensure_age_graph(self) -> None:
        try:
            with self._connect() as conn:
                conn.execute("SET search_path=ag_catalog, public")
                exists = conn.execute("SELECT 1 FROM ag_graph WHERE name = 'gmeow_graph'").fetchone()
                if not exists:
                    conn.execute("SELECT create_graph('gmeow_graph')")
        except PG_CACHE_EXCEPTIONS as exc:
            self.age_graph_error = repr(exc)

    def age_status(self) -> dict[str, Any]:
        """Age status."""
        try:
            with self._connect() as conn:
                conn.execute("SET search_path=ag_catalog, public")
                graph = conn.execute("SELECT graphid, name FROM ag_graph WHERE name = 'gmeow_graph'").fetchone()
                if not graph:
                    return {"available": False, "graph": "gmeow_graph", "nodes": None, "error": "graph is not created"}
                row = conn.execute(_age_sql("MATCH (n) RETURN count(n)", "nodes agtype")).fetchone()
                return {"available": True, "graph": graph["name"], "graphid": graph["graphid"], "nodes": str(row["nodes"]) if row else "0"}
        except PG_CACHE_EXCEPTIONS as exc:
            return {"available": False, "graph": "gmeow_graph", "nodes": None, "error": repr(exc)}

    def age_cypher(self, query: str, columns: str = "value agtype", limit: int = 100) -> list[dict[str, Any]]:
        """Age cypher."""
        if not _read_only_cypher(query):
            msg = "Only read-only MATCH/RETURN Cypher queries are allowed."
            raise ValueError(msg)
        query = query.strip().rstrip(";")
        if " limit " not in f" {query.lower()} ":
            query = f"{query} LIMIT {max(1, min(limit, 1000))}"
        with self._connect() as conn:
            conn.execute("SET search_path=ag_catalog, public")
            rows = conn.execute(_age_sql(query, columns)).fetchall()
            return [{key: str(value) for key, value in dict(row).items()} for row in rows]

    def _record_object(self, stored: StoredObject) -> None:
        with self._connect() as conn:
            self._record_object_conn(conn, stored)

    def _record_object_conn(self, conn: _PgConnection, stored: StoredObject) -> None:
        conn.execute(
            """
            INSERT INTO content_objects(digest, path, media_type, compression, original_size, stored_size, verification_status, verified_at)
            VALUES(%s, %s, %s, %s, %s, %s, 'ok', now())
            ON CONFLICT(digest) DO UPDATE SET
              path=excluded.path,
              media_type=excluded.media_type,
              compression=excluded.compression,
              original_size=excluded.original_size,
              stored_size=excluded.stored_size,
              verification_status='ok',
              verification_error=NULL,
              verified_at=now()
            """,
            (stored.digest, str(stored.path), stored.media_type, stored.compression, stored.original_size, stored.stored_size),
        )

    def _upsert_content_ref_conn(
        self, conn: _PgConnection, digest: str, ref_table: str, ref_pk: str, ref_column: str, ref_kind: str
    ) -> None:
        if not digest:
            return
        conn.execute(
            """
            INSERT INTO content_object_refs(digest, ref_table, ref_pk, ref_column, ref_kind, updated_at)
            VALUES(%s, %s, %s, %s, %s, now())
            ON CONFLICT(digest, ref_table, ref_pk, ref_column) DO UPDATE SET
              ref_kind=excluded.ref_kind,
              updated_at=now()
            """,
            (digest, ref_table, ref_pk, ref_column, ref_kind),
        )

    def _stored_text(self, digest: str) -> str:
        if not digest:
            return ""
        row = self._object_row(digest)
        return self.objects.get_text(digest, compression=row["compression"]) if row else ""

    def _stored_json(self, digest: str) -> object:
        text = self._stored_text(digest)
        return json.loads(text) if text else {}

    def _stored_bytes(self, digest: str) -> bytes:
        if not digest:
            return b""
        row = self._object_row(digest)
        return self.objects.get(digest, compression=row["compression"]) if row else b""

    def _object_row(self, digest: str) -> dict[str, Any]:
        with self._connect() as conn:
            return conn.execute("SELECT * FROM content_objects WHERE digest = %s", (digest,)).fetchone() or {}

    def upsert_label(self, label: dict[str, Any]) -> None:
        """Upsert label."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO labels(id, name, type, raw_json) VALUES(%s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET name=excluded.name, type=excluded.type, raw_json=excluded.raw_json
                """,
                (label["id"], label.get("name", label["id"]), label.get("type"), Jsonb(label)),
            )

    def list_labels(self) -> list[dict[str, Any]]:
        """List labels."""
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("SELECT id, name, type FROM labels ORDER BY name")]

    def label_name(self, label_id: str) -> str:
        """Label name."""
        with self._connect() as conn:
            row = conn.execute("SELECT name FROM labels WHERE id = %s", (label_id,)).fetchone()
        return row["name"] if row else label_id

    def upsert_thread(self, thread: dict[str, Any]) -> None:
        """Upsert thread."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO threads(id, snippet, raw_json) VALUES(%s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET snippet=excluded.snippet, raw_json=excluded.raw_json, updated_at=now()
                """,
                (thread["id"], thread.get("snippet", ""), Jsonb(thread)),
            )

    def upsert_message(self, message: ParsedMessage, raw_json: dict[str, Any], markdown: str, *, hydrated: bool) -> None:
        """Upsert message."""
        raw_obj = self.objects.put_json(raw_json)
        text_obj = self.objects.put_text(message.text)
        markdown_obj = self.objects.put_text(markdown, media_type="text/markdown; charset=utf-8")
        history_id = raw_json.get("historyId")
        search_fields = _message_search_fields(message.sender or "", message.recipients or "", message.date or "")
        search_text = _message_search_text(
            {
                "subject": message.subject,
                "sender": message.sender,
                "recipients": message.recipients,
                "snippet": message.snippet,
                "text_body": message.text,
                "markdown": markdown,
                "headers": message.headers,
                "label_ids": message.label_ids,
            }
        )
        with self._connect() as conn:
            for stored in [raw_obj, text_obj, markdown_obj]:
                self._record_object_conn(conn, stored)
            conn.execute("DELETE FROM content_object_refs WHERE ref_table = 'messages' AND ref_pk = %s", (message.gmail_id,))
            conn.execute("DELETE FROM content_object_refs WHERE ref_table = 'message_parts' AND ref_pk LIKE %s", (f"{message.gmail_id}:%",))
            conn.execute(
                """
                INSERT INTO messages(id, thread_id, subject, sender, recipients, message_date, snippet, label_ids,
                  headers_json, raw_json_digest, text_body_digest, markdown_digest, hydrated, history_id,
                  message_ts, sender_addr, sender_domain, recipient_addrs, recipient_domains, search_tsv)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_tsvector('simple', %s))
                ON CONFLICT(id) DO UPDATE SET
                  thread_id=excluded.thread_id, subject=excluded.subject, sender=excluded.sender,
                  recipients=excluded.recipients, message_date=excluded.message_date, snippet=excluded.snippet,
                  label_ids=excluded.label_ids, headers_json=excluded.headers_json,
                  raw_json_digest=excluded.raw_json_digest, text_body_digest=excluded.text_body_digest,
                  markdown_digest=excluded.markdown_digest, hydrated=excluded.hydrated,
                  history_id=excluded.history_id, message_ts=excluded.message_ts,
                  sender_addr=excluded.sender_addr, sender_domain=excluded.sender_domain,
                  recipient_addrs=excluded.recipient_addrs, recipient_domains=excluded.recipient_domains,
                  search_tsv=excluded.search_tsv, updated_at=now()
                """,
                (
                    message.gmail_id,
                    message.thread_id,
                    message.subject,
                    message.sender,
                    message.recipients,
                    message.date,
                    message.snippet,
                    Jsonb(message.label_ids),
                    Jsonb(message.headers),
                    raw_obj.digest,
                    text_obj.digest,
                    markdown_obj.digest,
                    hydrated,
                    int(str(history_id)) if str(history_id or "").isdigit() else None,
                    search_fields["message_ts"],
                    search_fields["sender_addr"],
                    search_fields["sender_domain"],
                    search_fields["recipient_addrs"],
                    search_fields["recipient_domains"],
                    search_text,
                ),
            )
            self._upsert_content_ref_conn(conn, raw_obj.digest, "messages", message.gmail_id, "raw_json_digest", "raw_json")
            self._upsert_content_ref_conn(conn, text_obj.digest, "messages", message.gmail_id, "text_body_digest", "text_body")
            self._upsert_content_ref_conn(conn, markdown_obj.digest, "messages", message.gmail_id, "markdown_digest", "markdown")
            conn.execute("DELETE FROM message_parts WHERE message_id = %s", (message.gmail_id,))
            for part in message.parts:
                body_digest: str = cast(str, None)
                if part.body_text:
                    body_obj = self.objects.put_text(part.body_text, media_type=part.mime_type)
                    self._record_object_conn(conn, body_obj)
                    body_digest = body_obj.digest
                    self._upsert_content_ref_conn(
                        conn, body_digest, "message_parts", f"{message.gmail_id}:{part.part_id}", "body_text_digest", "part_body"
                    )
                conn.execute(
                    """
                    INSERT INTO message_parts(message_id, part_id, mime_type, filename, body_text_digest, attachment_id, size, headers_json)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        message.gmail_id,
                        part.part_id,
                        part.mime_type,
                        part.filename,
                        body_digest,
                        part.attachment_id,
                        part.size,
                        Jsonb(part.headers),
                    ),
                )
            self._update_archive_state_conn(conn, message.gmail_id)
        self.replace_message_categories(
            message.gmail_id,
            deterministic_assignments(
                {
                    "id": message.gmail_id,
                    "subject": message.subject,
                    "sender": message.sender,
                    "recipients": message.recipients,
                    "snippet": message.snippet,
                    "text_body": message.text,
                    "label_ids": message.label_ids,
                    "labels": message.label_ids,
                }
            ),
            sources=("system",),
        )
        self.text_index.upsert_message(
            {
                "id": message.gmail_id,
                "subject": message.subject,
                "sender": message.sender,
                "recipients": message.recipients,
                "snippet": message.snippet,
                "text_body": message.text,
                "markdown": markdown,
                "headers_text": json.dumps(message.headers, sort_keys=True),
                "label_ids": message.label_ids,
                "categories": [item["category"] for item in self.message_category_assignments(message.gmail_id)],
            }
        )

    def set_raw_rfc822(self, message_id: str, raw: bytes) -> None:
        """Set raw rfc822."""
        stored = self.objects.put(raw, media_type="message/rfc822")
        with self._connect() as conn:
            self._record_object_conn(conn, stored)
            self._upsert_content_ref_conn(conn, stored.digest, "messages", message_id, "raw_rfc822_digest", "raw_rfc822")
            conn.execute("UPDATE messages SET raw_rfc822_digest = %s, updated_at = now() WHERE id = %s", (stored.digest, message_id))
            self._update_archive_state_conn(conn, message_id)

    def raw_rfc822(self, message_id: str) -> bytes:
        """Raw rfc822."""
        with self._connect() as conn:
            row = conn.execute("SELECT raw_rfc822_digest FROM messages WHERE id = %s", (message_id,)).fetchone()
        return self._stored_bytes(row["raw_rfc822_digest"]) if row else b""

    def add_attachment(self, record: dict[str, Any]) -> None:
        """Add attachment."""
        digest = record.get("digest") or record["sha1"]
        metadata = cast(dict[str, Any], record.get("metadata") or {})
        stored = StoredObject(
            digest=digest,
            path=record["path"],
            media_type=record.get("mime_type") or "application/octet-stream",
            compression=str(record.get("compression") or metadata.get("compression") or "identity"),
            original_size=int(record.get("size", 0)),
            stored_size=int(record.get("stored_size", record.get("size", 0))),
        )
        with self._connect() as conn:
            self._record_object_conn(conn, stored)
            conn.execute(
                """
                INSERT INTO messages(id, label_ids, headers_json, hydrated)
                VALUES(%s, '[]'::jsonb, '{}'::jsonb, false)
                ON CONFLICT(id) DO NOTHING
                """,
                (record["message_id"],),
            )
            conn.execute(
                """
                DELETE FROM attachments
                WHERE message_id = %s
                  AND COALESCE(part_id, '') = COALESCE(%s, '')
                  AND sha1 = %s
                """,
                (record["message_id"], record.get("part_id"), record["sha1"]),
            )
            conn.execute(
                """
                INSERT INTO attachments(
                    sha1, digest, message_id, part_id, gmail_attachment_id, filename,
                    mime_type, size, metadata_json, path, compression
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(message_id, COALESCE(part_id, ''), sha1) DO UPDATE SET
                  digest=excluded.digest,
                  gmail_attachment_id=excluded.gmail_attachment_id,
                  filename=excluded.filename,
                  mime_type=excluded.mime_type,
                  size=excluded.size,
                  metadata_json=excluded.metadata_json,
                  path=excluded.path,
                  compression=excluded.compression,
                  updated_at=now()
                """,
                (
                    record["sha1"],
                    digest,
                    record["message_id"],
                    record.get("part_id"),
                    record.get("gmail_attachment_id"),
                    record.get("filename"),
                    record.get("mime_type"),
                    int(record.get("size", 0)),
                    Jsonb(record.get("metadata", {})),
                    str(record["path"]),
                    stored.compression,
                ),
            )
            self._upsert_content_ref_conn(
                conn,
                digest,
                "attachments",
                f"{record['message_id']}:{record.get('part_id') or ''}:{record['sha1']}",
                "digest",
                "attachment",
            )
            metadata_obj = self.objects.put_json(record.get("metadata", {}))
            self._record_object_conn(conn, metadata_obj)
            conn.execute(
                """
                INSERT INTO attachment_metadata_versions(digest, metadata_digest, source)
                VALUES(%s, %s, %s)
                ON CONFLICT(digest, metadata_digest) DO NOTHING
                """,
                (digest, metadata_obj.digest, "attachment_sidecar"),
            )

    def _update_archive_state_conn(self, conn: _PgConnection, message_id: str) -> None:
        conn.execute(
            """
            UPDATE messages
            SET archive_state = CASE
                  WHEN archive_state IN ('tombstoned', 'purge_pending', 'purged') THEN archive_state
                  WHEN raw_json_digest IS NOT NULL AND raw_rfc822_digest IS NOT NULL THEN 'complete'
                  ELSE 'incomplete'
                END,
                archive_error = CASE
                  WHEN raw_json_digest IS NOT NULL AND raw_rfc822_digest IS NOT NULL THEN NULL
                  ELSE 'archive completeness requires raw Gmail JSON and canonical RFC822'
                END,
                archive_checked_at = now(),
                updated_at = now()
            WHERE id = %s
            """,
            (message_id,),
        )

    def list_attachments(self) -> list[dict[str, Any]]:
        """List attachments."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, sha1, digest, message_id, part_id, gmail_attachment_id, filename,
                       mime_type, size, metadata_json, path, compression
                FROM attachments ORDER BY sha1, message_id, part_id
                """
            )
            return [self._attachment_row(row) for row in rows]

    def attachments_for_message(self, message_id: str) -> list[dict[str, Any]]:
        """Attachments for message."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, sha1, digest, message_id, part_id, gmail_attachment_id, filename,
                       mime_type, size, metadata_json, path, compression
                FROM attachments WHERE message_id = %s ORDER BY filename, sha1
                """,
                (message_id,),
            )
            return [self._attachment_row(row, drop_path=True) for row in rows]

    def _attachment_row(self, row: dict[str, Any], *, drop_path: bool = False) -> dict[str, Any]:
        item = dict(row)
        item["metadata"] = item.pop("metadata_json") or {}
        if drop_path:
            item.pop("path", None)
        return item

    def attachment_text_for_message(self, message_id: str, *, include_metadata: bool = True) -> list[dict[str, Any]]:
        """Attachment text for message."""
        attachments = self.attachments_for_message(message_id)
        for attachment in attachments:
            text = _attachment_text_from_metadata(attachment.get("metadata", {}))
            if text:
                attachment["text"] = text
        if not include_metadata:
            for attachment in attachments:
                attachment.pop("metadata", None)
        return attachments

    def attachment_text_search(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Attachment text search."""
        needle = (query or "").lower()
        results: list[dict[str, Any]] = []
        for attachment in self.list_attachments():
            haystack = json.dumps(attachment.get("metadata", {}), sort_keys=True).lower()
            if needle in haystack or needle in (attachment.get("filename") or "").lower():
                results.append(attachment)
            if len(results) >= limit:
                break
        return results

    def get_message(self, message_id: str) -> dict[str, Any]:
        """Get message."""
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM messages WHERE id = %s", (message_id,)).fetchone()
        return self._message_row(row) if row else {}

    def list_messages(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        """List messages."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM messages ORDER BY COALESCE(message_date, updated_at::text) DESC LIMIT %s OFFSET %s",
                (limit, offset),
            ).fetchall()
        return [self._message_row(row) for row in rows]

    def iter_messages(self) -> list[dict[str, Any]]:
        """Iterate over messages."""
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM messages ORDER BY id").fetchall()
        return [self._message_row(row) for row in rows]

    def _message_row(self, row: dict[str, Any]) -> dict[str, Any]:
        data = dict(row)
        data.pop("search_tsv", None)
        data["label_ids"] = list(data.get("label_ids") or [])
        data["labels"] = [self.label_name(label_id) for label_id in data["label_ids"]]
        data["headers"] = data.pop("headers_json") or {}
        data["raw"] = self._stored_json(data.pop("raw_json_digest", None))
        data["text_body"] = self._stored_text(data.pop("text_body_digest", None))
        data["markdown"] = self._stored_text(data.pop("markdown_digest", None))
        data["has_raw_rfc822"] = bool(data.pop("raw_rfc822_digest", None))
        data["hydrated"] = bool(data["hydrated"])
        message_date = data.get("message_date")
        data["message_date_iso"] = _parse_message_date(message_date) if message_date else None
        data["date"] = data["message_date_iso"]
        data["from"] = data.get("sender")
        data["to"] = data.get("recipients")
        assignments = self.message_category_assignments(data["id"])
        if assignments:
            data["category_details"] = assignments
            data["categories"] = [assignment["category"] for assignment in assignments]
        else:
            data["categories"] = categorize_message(data)
            data["category_details"] = [
                {"category": category, "confidence": 1.0, "source": "computed", "reason": "deterministic fallback", "top_terms": []}
                for category in data["categories"]
            ]
        return data

    def refresh_message_search_columns(self, limit: int = DEFAULT_INT) -> dict[str, int]:
        """Refresh message search columns."""
        sql = """
            SELECT id, subject, sender, recipients, message_date, snippet, label_ids, headers_json,
                   text_body_digest, markdown_digest
            FROM messages
            ORDER BY updated_at DESC
        """
        params: tuple[Any, ...] = ()
        if limit:
            sql += " LIMIT %s"
            params = (limit,)
        updated = 0
        skipped = 0
        with self._connect() as conn:
            rows = [dict(row) for row in conn.execute(sql, params)]
            for row in rows:
                fields = _message_search_fields(
                    str(row.get("sender") or ""),
                    str(row.get("recipients") or ""),
                    str(row.get("message_date") or ""),
                )
                search_text = _message_search_text(
                    {
                        "subject": row.get("subject"),
                        "sender": row.get("sender"),
                        "recipients": row.get("recipients"),
                        "snippet": row.get("snippet"),
                        "text_body": self._stored_text(str(row.get("text_body_digest") or "")),
                        "markdown": self._stored_text(str(row.get("markdown_digest") or "")),
                        "headers": row.get("headers_json") or {},
                        "label_ids": list(row.get("label_ids") or []),
                    }
                )
                if not search_text.strip():
                    skipped += 1
                    continue
                conn.execute(
                    """
                    UPDATE messages
                    SET message_ts = %s,
                        sender_addr = %s,
                        sender_domain = %s,
                        recipient_addrs = %s,
                        recipient_domains = %s,
                        search_tsv = to_tsvector('simple', %s),
                        updated_at = updated_at
                    WHERE id = %s
                    """,
                    (
                        fields["message_ts"],
                        fields["sender_addr"],
                        fields["sender_domain"],
                        fields["recipient_addrs"],
                        fields["recipient_domains"],
                        search_text,
                        row["id"],
                    ),
                )
                updated += 1
        return {"updated": updated, "skipped": skipped}

    def compact_message(self, message: dict[str, Any], body_chars: int = 500) -> dict[str, Any]:
        """Compact message."""
        text = " ".join((message.get("text_body") or message.get("snippet") or "").split())
        if len(text) > body_chars:
            text = text[:body_chars].rstrip() + "..."
        return {
            "id": message["id"],
            "thread_id": message.get("thread_id"),
            "subject": message.get("subject"),
            "from": message.get("sender"),
            "to": message.get("recipients"),
            "sender": message.get("sender"),
            "recipients": message.get("recipients"),
            "message_date": message.get("message_date"),
            "message_date_iso": message.get("message_date_iso"),
            "date": message.get("message_date_iso"),
            "snippet": message.get("snippet"),
            "text": text,
            "label_ids": message.get("label_ids", []),
            "labels": message.get("labels", []),
            "categories": message.get("categories", []),
            "hydrated": message.get("hydrated", False),
        }

    def compact_messages(self, messages: list[dict[str, Any]], body_chars: int = 500) -> list[dict[str, Any]]:
        """Compact messages."""
        return [self.compact_message(message, body_chars=body_chars) for message in messages]

    def get_thread(self, thread_id: str, *, compact: bool = False) -> dict[str, Any]:
        """Get thread."""
        with self._connect() as conn:
            thread = conn.execute("SELECT * FROM threads WHERE id = %s", (thread_id,)).fetchone()
            message_rows = conn.execute("SELECT * FROM messages WHERE thread_id = %s", (thread_id,)).fetchall()
        messages = [self._message_row(row) for row in message_rows]
        messages.sort(key=lambda message: message.get("message_date_iso") or message.get("message_date") or "")
        if not thread and not messages:
            return {}
        if compact:
            messages = self.compact_messages(messages, body_chars=500)
        return {"id": thread_id, "snippet": thread["snippet"] if thread else "", "messages": messages}

    def list_threads(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        """List threads."""
        with self._connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT id, snippet, updated_at FROM threads ORDER BY updated_at DESC LIMIT %s OFFSET %s", (limit, offset)
                )
            ]

    def text_search(
        self,
        query: str,
        limit: int = 20,
        after: str = DEFAULT_STR,
        before: str = DEFAULT_STR,
        include_categories: list[str] = DEFAULT_LIST_STR,
        exclude_categories: list[str] = DEFAULT_LIST_STR,
    ) -> list[dict[str, Any]]:
        """Text search."""
        parsed = _parse_local_query(query)
        if after:
            parsed["after"] = after
        if before:
            parsed["before"] = before
        filters = self._text_search_filters(parsed, include_categories, exclude_categories)
        query_sql = sql.SQL(
            """
            SELECT *, {rank_sql} AS search_rank
            FROM messages
            WHERE {where_sql}
            ORDER BY search_rank DESC, message_ts DESC NULLS LAST, updated_at DESC
            LIMIT %s
        """
        ).format(rank_sql=sql.SQL(cast(Any, str(filters["rank_sql"]))), where_sql=_where_sql(filters["where"]))
        query_params = (*filters["params"], limit)
        with self._connect() as conn:
            rows = conn.execute(query_sql, query_params).fetchall()
        messages = [self._message_row(row) for row in rows]
        messages = [
            message for message in messages if self.category_allowed(message.get("categories", []), include_categories, exclude_categories)
        ]
        if filters["after_dt"] is None or filters["before_dt"] is None:
            messages = _filter_by_date(messages, parsed["after"], parsed["before"])
        return messages[:limit]

    def _text_search_filters(self, parsed: dict[str, Any], include_categories: list[str], exclude_categories: list[str]) -> dict[str, Any]:
        where = ["true"]
        params: list[Any] = []
        rank_sql = _apply_fts_filter(where, params, parsed["fts"])
        _apply_address_filters(where, params, parsed["from"], "sender")
        _apply_address_filters(where, params, parsed["to"], "recipients")
        _apply_subject_label_filters(where, params, parsed)
        after_dt, before_dt = _apply_date_filters(where, params, parsed)
        self._apply_category_filters(where, params, include_categories, exclude_categories)
        return {"where": where, "params": params, "rank_sql": rank_sql, "after_dt": after_dt, "before_dt": before_dt}

    def _apply_category_filters(
        self, where: list[str], params: list[Any], include_categories: list[str], exclude_categories: list[str]
    ) -> None:
        _apply_category_filters_sql(where, params, include_categories, exclude_categories, self.default_excluded_categories())

    def address_messages(
        self,
        address: str,
        limit: int = 50,
        include_categories: list[str] = DEFAULT_LIST_STR,
        exclude_categories: list[str] = DEFAULT_LIST_STR,
    ) -> list[dict[str, Any]]:
        """Address messages."""
        parsed_address, parsed_domain = _address_filter_value(address)
        where: list[str] = []
        params: list[Any] = []
        if parsed_address:
            where.append("(sender_addr = %s OR %s = ANY(recipient_addrs) OR sender ILIKE %s OR recipients ILIKE %s)")
            params.extend([parsed_address, parsed_address, f"%{address}%", f"%{address}%"])
        elif parsed_domain:
            where.append("(sender_domain = %s OR %s = ANY(recipient_domains) OR sender ILIKE %s OR recipients ILIKE %s)")
            params.extend([parsed_domain, parsed_domain, f"%{address}%", f"%{address}%"])
        else:
            where.append("(sender ILIKE %s OR recipients ILIKE %s)")
            params.extend([f"%{address}%", f"%{address}%"])
        self._apply_category_filters(where, params, include_categories, exclude_categories)
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                SELECT * FROM messages
                WHERE {where_sql}
                ORDER BY message_ts DESC NULLS LAST, updated_at DESC
                LIMIT %s
                """
                ).format(where_sql=_where_sql(where)),
                (*params, limit),
            ).fetchall()
        messages = [self._message_row(row) for row in rows]
        return [
            message for message in messages if self.category_allowed(message.get("categories", []), include_categories, exclude_categories)
        ][:limit]

    def contacts(self, limit: int = 100, min_messages: int = 1) -> list[dict[str, Any]]:
        """Contacts."""
        contacts: dict[str, dict[str, Any]] = {}
        for message in self.iter_messages():
            for role, raw_value in [("from", message.get("sender") or ""), ("to", message.get("recipients") or "")]:
                for name, raw_address in getaddresses([raw_value]):
                    address = raw_address.lower().strip()
                    if not address:
                        continue
                    contact = contacts.setdefault(
                        address,
                        {
                            "address": address,
                            "display_name": "",
                            "names": set(),
                            "messages": 0,
                            "from_messages": 0,
                            "to_messages": 0,
                            "latest_message_date": None,
                            "latest_message_id": None,
                        },
                    )
                    if name:
                        contact["names"].add(name)
                    contact["messages"] += 1
                    contact[f"{role}_messages"] += 1
                    date = message.get("message_date_iso")
                    if date and (contact["latest_message_date"] is None or date > contact["latest_message_date"]):
                        contact["latest_message_date"] = date
                        contact["latest_message_id"] = message["id"]
        rows: list[dict[str, Any]] = []
        for contact in contacts.values():
            names = sorted(contact.pop("names"))
            contact["names"] = names
            contact["display_name"] = _best_contact_name(names, contact["address"])
            if contact["messages"] >= min_messages:
                rows.append(contact)
        rows.sort(key=lambda item: (-item["messages"], item["address"]))
        return rows[:limit]

    def people(self, limit: int = 100, min_messages: int = 1) -> list[dict[str, Any]]:
        """People."""
        aliases = self.people_aliases()
        people: dict[str, dict[str, Any]] = {}
        for contact in self.contacts(limit=10_000, min_messages=1):
            alias = aliases.get(contact["address"])
            name_key = _person_key(contact["display_name"])
            key = alias["person_key"] if alias else name_key if name_key and "@" not in name_key else contact["address"]
            person = people.setdefault(
                key,
                {
                    "key": key,
                    "name": alias.get("display_name") if alias and alias.get("display_name") else contact["display_name"],
                    "addresses": [],
                    "names": set(),
                    "messages": 0,
                    "latest_message_date": None,
                    "latest_message_id": None,
                },
            )
            person["addresses"].append(contact["address"])
            person["names"].update(contact["names"])
            person["messages"] += contact["messages"]
            if contact["latest_message_date"] and (
                person["latest_message_date"] is None or contact["latest_message_date"] > person["latest_message_date"]
            ):
                person["latest_message_date"] = contact["latest_message_date"]
                person["latest_message_id"] = contact["latest_message_id"]
        rows: list[dict[str, Any]] = []
        for person in people.values():
            person["addresses"] = sorted(set(person["addresses"]))
            person["names"] = sorted(person["names"])
            if person["messages"] >= min_messages:
                rows.append(person)
        rows.sort(key=lambda item: (-item["messages"], item["name"].lower()))
        return rows[:limit]

    def upsert_people_alias(self, address: str, person_key: str, display_name: str = DEFAULT_STR, note: str = DEFAULT_STR) -> None:
        """Upsert people alias."""
        address = address.lower().strip()
        key = _person_key(person_key) or address
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO people_aliases(address, person_key, display_name, note)
                VALUES(%s, %s, %s, %s)
                ON CONFLICT(address) DO UPDATE SET
                  person_key=excluded.person_key,
                  display_name=excluded.display_name,
                  note=excluded.note,
                  updated_at=now()
                """,
                (address, key, display_name, note),
            )

    def people_aliases(self) -> dict[str, dict[str, Any]]:
        """People aliases."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT address, person_key, display_name, note, updated_at FROM people_aliases ORDER BY person_key, address"
            )
            return {row["address"]: dict(row) for row in rows}

    def enqueue_intelligence_job(self, kind: str, target_id: str, payload: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        """Enqueue intelligence job."""
        pg_cache_jobs.enqueue_intelligence_job(self, kind, target_id, payload)

    def enqueue_all_intelligence_jobs(self) -> dict[str, int]:
        """Enqueue all intelligence jobs."""
        return pg_cache_jobs.enqueue_all_intelligence_jobs(self)

    def reclaim_stale_intelligence_jobs(self, stale_after_seconds: int = 900) -> dict[str, int]:
        """Reclaim stale intelligence jobs."""
        return pg_cache_jobs.reclaim_stale_intelligence_jobs(self, stale_after_seconds)

    def claim_next_intelligence_job(self, worker_id: str = DEFAULT_STR, targets: list[tuple[str, str]] = DEFAULT_TARGETS) -> dict[str, Any]:
        """Claim next intelligence job."""
        return pg_cache_jobs.claim_next_intelligence_job(self, worker_id=worker_id, targets=targets)

    def intelligence_target_status(self, targets: list[tuple[str, str]]) -> dict[str, Any]:
        """Return intelligence status for target jobs."""
        return pg_cache_jobs.intelligence_target_status(self, targets)

    def message_backfill_complete(self, message_id: str, *, require_raw: bool = True) -> dict[str, Any]:
        """Return whether a message has all backfill artifacts."""
        return pg_cache_jobs.message_backfill_complete(self, message_id, require_raw=require_raw)

    def complete_intelligence_job(self, job_id: int) -> None:
        """Complete intelligence job."""
        pg_cache_jobs.complete_intelligence_job(self, job_id)

    def fail_intelligence_job(self, job_id: int, error: str) -> None:
        """Fail intelligence job."""
        pg_cache_jobs.fail_intelligence_job(self, job_id, error)

    def intelligence_job_status(self) -> dict[str, int]:
        """Intelligence job status."""
        return pg_cache_jobs.intelligence_job_status(self)

    def list_intelligence_jobs(self, status: str = DEFAULT_STR, limit: int = 50) -> list[dict[str, Any]]:
        """List intelligence jobs."""
        return pg_cache_jobs.list_intelligence_jobs(self, status=status, limit=limit)

    def list_dead_letter_jobs(self, limit: int = 50, *, include_closed: bool = False) -> list[dict[str, Any]]:
        """List dead letter jobs."""
        return pg_cache_jobs.list_dead_letter_jobs(self, limit=limit, include_closed=include_closed)

    def retry_dead_letter_jobs(self, limit: int = DEFAULT_INT) -> dict[str, int]:
        """Retry dead letter jobs."""
        return pg_cache_jobs.retry_dead_letter_jobs(self, limit=limit)

    def record_search_run(
        self,
        search_id: str,
        query: str,
        source: str,
        request: dict[str, Any],
        status: str = "running",
    ) -> None:
        """Record or update a search run."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO search_runs(search_id, query, source, status, request_json)
                VALUES(%s, %s, %s, %s, %s)
                ON CONFLICT(search_id) DO UPDATE SET
                  query=excluded.query,
                  source=excluded.source,
                  status=excluded.status,
                  request_json=excluded.request_json,
                  updated_at=now()
                """,
                (search_id, query, source, status, Jsonb(_json_ready(request or {}))),
            )

    def finish_search_run(
        self,
        search_id: str,
        *,
        status: str,
        source: str,
        live: dict[str, Any],
        message_ids: list[str],
        analysis: dict[str, Any],
        attachment_hydration: dict[str, Any],
        phase_timings: dict[str, Any],
        error: str = DEFAULT_STR,
    ) -> None:
        """Finish a search run."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE search_runs
                SET status=%s,
                    source=%s,
                    live_json=%s,
                    message_ids_json=%s,
                    analysis_json=%s,
                    attachment_hydration_json=%s,
                    phase_timings_json=%s,
                    error=%s,
                    updated_at=now(),
                    completed_at=CASE WHEN %s <> 'running' THEN now() ELSE completed_at END
                WHERE search_id=%s
                """,
                (
                    status,
                    source,
                    Jsonb(_json_ready(live or {})),
                    Jsonb(_json_ready(message_ids)),
                    Jsonb(_json_ready(analysis or {})),
                    Jsonb(_json_ready(attachment_hydration or {})),
                    Jsonb(_json_ready(phase_timings or {})),
                    error,
                    status,
                    search_id,
                ),
            )

    def search_run(self, search_id: str) -> dict[str, Any]:
        """Return a search run."""
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM search_runs WHERE search_id = %s", (search_id,)).fetchone()
        if not row:
            return {}
        item = dict(row)
        item["request"] = item.pop("request_json") or {}
        item["live"] = item.pop("live_json") or {}
        item["message_ids"] = item.pop("message_ids_json") or []
        item["analysis"] = item.pop("analysis_json") or {}
        item["attachment_hydration"] = item.pop("attachment_hydration_json") or {}
        item["phase_timings_ms"] = item.pop("phase_timings_json") or {}
        return item

    def enqueue_deferred_attachment_hydration(self, ref: dict[str, Any], payload: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        """Queue an attachment for later Gmail download and sidecar analysis."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO deferred_attachment_hydration(
                  message_id, thread_id, part_id, gmail_attachment_id, filename, mime_type,
                  size, priority, search_id, payload_json
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(message_id, part_id, gmail_attachment_id) DO UPDATE
                SET status=CASE
                      WHEN deferred_attachment_hydration.status = 'done' THEN deferred_attachment_hydration.status
                      ELSE 'pending'
                    END,
                    priority=GREATEST(deferred_attachment_hydration.priority, excluded.priority),
                    last_error=NULL,
                    locked_by=NULL,
                    locked_at=NULL,
                    search_id=COALESCE(excluded.search_id, deferred_attachment_hydration.search_id),
                    payload_json=excluded.payload_json,
                    updated_at=now()
                """,
                (
                    ref["message_id"],
                    ref.get("thread_id"),
                    ref["part_id"],
                    ref["attachment_id"],
                    ref.get("filename"),
                    ref.get("mime_type"),
                    ref.get("size"),
                    int(ref.get("priority", 100)),
                    ref.get("search_id"),
                    Jsonb(_json_ready(payload or {})),
                ),
            )

    def claim_deferred_attachment_hydration(self, limit: int, worker_id: str) -> list[dict[str, Any]]:
        """Claim deferred attachment hydration jobs."""
        with self._connect() as conn, conn.transaction():
            rows = conn.execute(
                """
                SELECT id, message_id, thread_id, part_id, gmail_attachment_id, filename, mime_type,
                       size, attempts, max_attempts, payload_json, search_id
                FROM deferred_attachment_hydration
                WHERE status = 'pending'
                ORDER BY priority DESC, created_at, id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (limit,),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if ids:
                conn.execute(
                    """
                    UPDATE deferred_attachment_hydration
                    SET status='running',
                        attempts=attempts+1,
                        locked_by=%s,
                        locked_at=now(),
                        updated_at=now()
                    WHERE id = ANY(%s)
                    """,
                    (worker_id, ids),
                )
        return [dict(row) for row in rows]

    def complete_deferred_attachment_hydration(self, job_id: int, sha1: str) -> None:
        """Mark deferred attachment hydration complete."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE deferred_attachment_hydration
                SET status='done',
                    last_error=NULL,
                    locked_by=NULL,
                    locked_at=NULL,
                    completed_at=now(),
                    payload_json=payload_json || %s,
                    updated_at=now()
                WHERE id=%s
                """,
                (Jsonb({"sha1": sha1}), job_id),
            )

    def fail_deferred_attachment_hydration(self, job_id: int, error: str) -> None:
        """Mark deferred attachment hydration failed or retryable."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT attempts, max_attempts FROM deferred_attachment_hydration WHERE id = %s",
                (job_id,),
            ).fetchone()
            if not row:
                return
            status = "dead" if int(row["attempts"] or 0) >= int(row["max_attempts"] or 5) else "pending"
            conn.execute(
                """
                UPDATE deferred_attachment_hydration
                SET status=%s,
                    last_error=%s,
                    locked_by=NULL,
                    locked_at=NULL,
                    updated_at=now()
                WHERE id=%s
                """,
                (status, error[:2000], job_id),
            )

    def deferred_attachment_hydration_status(self, message_ids: list[str] = DEFAULT_LIST_STR) -> dict[str, Any]:
        """Return deferred attachment hydration status."""
        conditions: list[str] = []
        params: list[Any] = []
        if message_ids:
            conditions.append("message_id = ANY(%s)")
            params.append(message_ids)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                    SELECT id, message_id, part_id, gmail_attachment_id, filename, mime_type,
                           status, attempts, last_error, search_id, updated_at
                    FROM deferred_attachment_hydration
                    {where}
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 200
                    """
                ).format(where=sql.SQL(where)),
                tuple(params),
            ).fetchall()
        counts: dict[str, int] = {}
        items = [dict(row) for row in rows]
        for item in items:
            status = str(item["status"])
            counts[status] = counts.get(status, 0) + 1
        return {
            "counts": counts,
            "pending": [item for item in items if item["status"] in {"pending", "running"}],
            "dead": [item for item in items if item["status"] == "dead"],
            "done": [item for item in items if item["status"] == "done"],
            "terminal": not any(item["status"] in {"pending", "running"} for item in items),
        }

    def clear_dead_letter_job(self, dead_letter_id: int) -> dict[str, Any]:
        """Clear dead letter job."""
        return pg_cache_jobs.clear_dead_letter_job(self, dead_letter_id)

    def ensure_default_categories(self) -> None:
        """Ensure default categories."""
        defaults = [
            ("camera_alert", "Camera Alert", "system", True, "UniFi Protect and similar camera/event alert noise"),
            ("machine_notification", "Machine Notification", "system", True, "High-volume automated machine status notifications"),
            ("bulk_status_noise", "Bulk Status Noise", "system", True, "Low-value repeated operational status messages"),
            ("security_admin", "Security/Admin", "system", False, "Security, identity, and admin console alerts"),
            ("financial_statement", "Financial Statement", "system", False, "Banking, broker, statement, and financial account mail"),
            ("dev_activity", "Development Activity", "system", True, "GitHub/GitLab repository activity"),
            ("dev_review", "Development Review", "system", True, "Code review comments and requested reviews"),
            ("dev_ci", "Development CI", "system", False, "CI, pipeline, build, and workflow status"),
            ("call_notice", "Call Notice", "system", True, "Phone call and voicemail notifications"),
            ("mailing_list", "Mailing List", "system", True, "Mailing list conversations"),
            ("newsletter", "Newsletter", "system", False, "Newsletters and periodic digests"),
            ("promotion", "Promotion", "system", False, "Marketing and promotional mail"),
            ("social", "Social", "system", False, "Social network and community notifications"),
            ("personal", "Personal", "system", False, "Personal correspondence"),
            ("work", "Work", "system", False, "Work-related correspondence"),
            ("primary", "Primary", "system", False, "General uncategorized visible mail"),
            ("update", "Update", "system", False, "Gmail Updates category mail"),
            ("news_alert", "News Alert", "system", False, "Google Alerts and similar news alerts"),
            ("admin_alert", "Admin Alert", "system", False, "Administrative alert messages"),
            ("real_estate", "Real Estate", "system", False, "Real estate transaction mail"),
            ("corporate_filing", "Corporate Filing", "system", False, "Corporate registry and filing mail"),
            ("legal", "Legal", "system", False, "Legal, strata, and formal matter mail"),
            ("family", "Family", "system", False, "Family/couple correspondence"),
            ("health_appointment", "Health Appointment", "system", False, "Appointment and health scheduling mail"),
            ("home_services", "Home Services", "system", False, "Household service and vendor mail"),
        ]
        with self._connect() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO categories(id, name, source, default_hidden, description)
                    VALUES(%s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO NOTHING
                    """,
                defaults,
            )

    def upsert_category(
        self,
        category: str,
        name: str = DEFAULT_STR,
        source: str = "manual",
        **options: object,
    ) -> None:
        """Upsert category."""
        default_hidden = bool(options.get("default_hidden", False))
        enabled = bool(options.get("enabled", True))
        description = options.get("description")
        profile = options.get("profile")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO categories(id, name, source, default_hidden, enabled, description, profile_json)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET
                  name=excluded.name,
                  source=excluded.source,
                  default_hidden=excluded.default_hidden,
                  enabled=excluded.enabled,
                  description=excluded.description,
                  profile_json=excluded.profile_json,
                  updated_at=now()
                """,
                (category, name or _title_category(category), source, default_hidden, enabled, description, Jsonb(profile or {})),
            )

    def list_categories(self) -> list[dict[str, Any]]:
        """List categories."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.*, COUNT(mc.message_id) AS messages
                FROM categories c
                LEFT JOIN message_categories mc ON mc.category = c.id
                GROUP BY c.id
                ORDER BY c.default_hidden DESC, c.source, c.id
                """
            )
            return [self._category_row(row) for row in rows]

    def _category_row(self, row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item["default_hidden"] = bool(item["default_hidden"])
        item["enabled"] = bool(item["enabled"])
        item["profile"] = item.pop("profile_json") or {}
        return item

    def default_excluded_categories(self) -> set[str]:
        """Default excluded categories."""
        with self._connect() as conn:
            rows = conn.execute("SELECT id FROM categories WHERE default_hidden = true AND enabled = true")
            return {row["id"] for row in rows} or set(DEFAULT_EXCLUDED_CATEGORIES)

    def replace_message_categories(
        self, message_id: str, assignments: list[dict[str, Any]], sources: tuple[str, ...] = ("system", "learned", "manual_rule")
    ) -> None:
        """Replace message categories."""
        with self._connect() as conn:
            conn.execute("DELETE FROM message_categories WHERE message_id = %s AND source = ANY(%s)", (message_id, list(sources)))
            for assignment in assignments:
                conn.execute(
                    """
                    INSERT INTO categories(id, name, source, default_hidden, enabled)
                    VALUES(%s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO NOTHING
                    """,
                    (
                        assignment["category"],
                        _title_category(assignment["category"]),
                        assignment.get("source", "system"),
                        assignment["category"] in DEFAULT_EXCLUDED_CATEGORIES,
                        assignment.get("enabled", True),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO message_categories(message_id, category, confidence, source, reason, top_terms_json)
                    VALUES(%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(message_id, category) DO UPDATE SET
                      confidence=excluded.confidence,
                      source=excluded.source,
                      reason=excluded.reason,
                      top_terms_json=excluded.top_terms_json,
                      updated_at=now()
                    """,
                    (
                        message_id,
                        assignment["category"],
                        float(assignment.get("confidence", 1.0)),
                        assignment.get("source", "system"),
                        assignment.get("reason"),
                        Jsonb(assignment.get("top_terms", [])),
                    ),
                )
            self._apply_category_overrides_conn(conn, message_id)

    def apply_category(self, message_id: str, category: str, action: str = "include", reason: str = DEFAULT_STR) -> None:
        """Apply category."""
        self.upsert_category(category, source="manual")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO category_overrides(message_id, category, action, reason)
                VALUES(%s, %s, %s, %s)
                ON CONFLICT(message_id, category) DO UPDATE SET action=excluded.action, reason=excluded.reason, updated_at=now()
                """,
                (message_id, category, action, reason),
            )
            self._apply_category_overrides_conn(conn, message_id)

    def _apply_category_overrides_conn(self, conn: _PgConnection, message_id: str) -> None:
        for row in conn.execute("SELECT category, action, reason FROM category_overrides WHERE message_id = %s", (message_id,)):
            if row["action"] == "exclude":
                conn.execute("DELETE FROM message_categories WHERE message_id = %s AND category = %s", (message_id, row["category"]))
                continue
            conn.execute(
                """
                INSERT INTO message_categories(message_id, category, confidence, source, reason, top_terms_json)
                VALUES(%s, %s, 1.0, 'manual', %s, '[]'::jsonb)
                ON CONFLICT(message_id, category) DO UPDATE SET confidence=1.0, source='manual', reason=excluded.reason, updated_at=now()
                """,
                (message_id, row["category"], row["reason"] or row["action"]),
            )

    def upsert_category_rule(self, category: str, rule: dict[str, Any], *, enabled: bool = True) -> int:
        """Upsert category rule."""
        self.upsert_category(category, source="manual", default_hidden=category in DEFAULT_EXCLUDED_CATEGORIES)
        with self._connect() as conn:
            row = conn.execute(
                "INSERT INTO category_rules(category, rule_json, enabled) VALUES(%s, %s, %s) RETURNING id",
                (category, Jsonb(rule), enabled),
            ).fetchone()
            return int(row["id"])

    def list_category_rules(self) -> list[dict[str, Any]]:
        """List category rules."""
        with self._connect() as conn:
            rows = conn.execute("SELECT id, category, rule_json, enabled, created_at, updated_at FROM category_rules ORDER BY category, id")
            result: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["rule"] = item.pop("rule_json") or {}
                item["enabled"] = bool(item["enabled"])
                result.append(item)
            return result

    def message_category_assignments(self, message_id: str) -> list[dict[str, Any]]:
        """Message category assignments."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT category, confidence, source, reason, top_terms_json, updated_at
                FROM message_categories
                WHERE message_id = %s
                ORDER BY confidence DESC, category
                """,
                (message_id,),
            )
            result: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["top_terms"] = item.pop("top_terms_json") or []
                result.append(item)
            return result

    def has_manual_category_assignments(self) -> bool:
        """Return whether manual category examples exist."""
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM message_categories WHERE source = 'manual' LIMIT 1").fetchone()
        return bool(row)

    def store_learned_category_run(self, run: dict[str, Any]) -> int:
        """Store learned category run."""
        with self._connect() as conn:
            row = conn.execute("INSERT INTO learned_category_runs(run_json) VALUES(%s) RETURNING id", (Jsonb(run),)).fetchone()
            return int(row["id"])

    def learned_category_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        """Learned category runs."""
        with self._connect() as conn:
            return [
                {"id": row["id"], "created_at": row["created_at"], "run": row["run_json"]}
                for row in conn.execute("SELECT id, run_json, created_at FROM learned_category_runs ORDER BY id DESC LIMIT %s", (limit,))
            ]

    def category_stats(self, after: str = DEFAULT_STR, before: str = DEFAULT_STR) -> dict[str, Any]:
        """Category stats."""
        messages = _filter_by_date(self.iter_messages(), after, before)
        counts: dict[str, int] = {}
        for message in messages:
            for category in message.get("categories", []):
                counts[category] = counts.get(category, 0) + 1
        return {
            "messages": len(messages),
            "categories": dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))),
            "default_excluded": sorted(self.default_excluded_categories()),
        }

    def upsert_rule(self, name: str, priority: int, rule: dict[str, Any]) -> None:
        """Upsert rule."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO priority_rules(name, rule_json, priority) VALUES(%s, %s, %s)
                ON CONFLICT(name) DO UPDATE SET rule_json=excluded.rule_json, priority=excluded.priority
                """,
                (name, Jsonb(rule), priority),
            )

    def set_state(self, key: str, value: str) -> None:
        """Set state."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sync_state(key, value) VALUES(%s, %s)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=now()
                """,
                (key, value),
            )

    def delete_state(self, key: str) -> None:
        """Delete state."""
        with self._connect() as conn:
            conn.execute("DELETE FROM sync_state WHERE key = %s", (key,))

    def get_state(self, key: str) -> str:
        """Get state."""
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM sync_state WHERE key = %s", (key,)).fetchone()
            return str(row["value"]) if row else ""

    def record_operational_event(
        self,
        event_type: str,
        severity: str,
        component: str,
        subject_id: str = DEFAULT_STR,
        detail: str = DEFAULT_STR,
        metadata: dict[str, Any] = DEFAULT_DICT_ANY,
    ) -> int:
        """Record operational event."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    INSERT INTO operational_events(event_type, severity, component, subject_id, detail, metadata_json)
                    VALUES(%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (event_type, severity, component, subject_id, detail, Jsonb(metadata or {})),
                ).fetchone()
                return int(row["id"]) if row else 0
        except PG_CACHE_EXCEPTIONS as exc:
            self.operational_event_error = repr(exc)
            return 0

    def operational_events(self, limit: int = 100, component: str = DEFAULT_STR, severity: str = DEFAULT_STR) -> list[dict[str, Any]]:
        """Operational events."""
        where: list[str] = []
        params: list[Any] = []
        if component:
            where.append("component = %s")
            params.append(component)
        if severity:
            where.append("severity = %s")
            params.append(severity)
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                SELECT id, event_type, severity, component, subject_id, detail, metadata_json, created_at
                FROM operational_events
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """
                ).format(where_sql=_where_clause_sql(where)),
                (*params, limit),
            )
            return [dict(row) for row in rows]

    def start_sync_run(self, run_kind: str, start_cursor: str = DEFAULT_STR, request: dict[str, Any] = DEFAULT_DICT_ANY) -> int:
        """Start sync run."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    INSERT INTO sync_runs(run_kind, status, start_cursor, request_json)
                    VALUES(%s, 'running', %s, %s)
                    RETURNING id
                    """,
                    (run_kind, start_cursor, Jsonb(request or {})),
                ).fetchone()
                run_id = int(row["id"]) if row else 0
            self.record_operational_event(
                "sync.started",
                "info",
                "sync",
                str(run_id) if run_id else "",
                f"Started {run_kind} sync.",
                {"run_kind": run_kind, "start_cursor": start_cursor},
            )
        except PG_CACHE_EXCEPTIONS as exc:
            self.sync_run_error = repr(exc)
            return 0
        else:
            return run_id

    def finish_sync_run(
        self,
        run_id: int,
        status: str,
        end_cursor: str = DEFAULT_STR,
        result: dict[str, Any] = DEFAULT_DICT_ANY,
        error: str = DEFAULT_STR,
    ) -> None:
        """Finish sync run."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE sync_runs
                    SET status=%s,
                        end_cursor=%s,
                        result_json=%s,
                        error=%s,
                        finished_at=now()
                    WHERE id = %s
                    """,
                    (status, end_cursor, Jsonb(result or {}), error, run_id),
                )
            self.record_operational_event(
                "sync." + status,
                "error" if status == "failed" else "info",
                "sync",
                str(run_id),
                error or f"Finished sync run {run_id}.",
                result or {},
            )
        except PG_CACHE_EXCEPTIONS as exc:
            self.sync_run_error = repr(exc)
            return

    def sync_runs(self, limit: int = 50, run_kind: str = DEFAULT_STR) -> list[dict[str, Any]]:
        """Sync runs."""
        where = "WHERE run_kind = %s" if run_kind else ""
        params: tuple[Any, ...] = (run_kind, limit) if run_kind else (limit,)
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                SELECT id, run_kind, status, start_cursor, end_cursor, request_json, result_json, error, started_at, finished_at
                FROM sync_runs
                {where}
                ORDER BY started_at DESC, id DESC
                LIMIT %s
                """
                ).format(where=sql.SQL(where)),
                params,
            )
            return [dict(row) for row in rows]

    def latest_history_id(self) -> str:
        """Latest history id."""
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(history_id) AS history_id FROM messages").fetchone()
            return str(row["history_id"]) if row and row["history_id"] is not None else ""

    def sync_status(self) -> dict[str, Any]:
        """Sync status."""
        with self._connect() as conn:
            counts = {}
            for table in [
                "content_objects",
                "content_object_refs",
                "labels",
                "threads",
                "messages",
                "attachments",
                "graph_triples",
                "graph_node_profiles",
                "graph_edge_stats",
                "summary_items",
                "embedding_chunks",
                "ingest_issues",
                "dead_letter_jobs",
                "operational_events",
                "sync_runs",
                "retention_policies",
                "imap_mailboxes",
                "imap_message_uids",
                "archive_exports",
                "search_runs",
                "deferred_attachment_hydration",
            ]:
                counts[table] = conn.execute(sql.SQL("SELECT COUNT(*) AS c FROM {}").format(sql.Identifier(table))).fetchone()["c"]
            state = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM sync_state")}
            rows = conn.execute("SELECT message_date FROM messages WHERE message_date IS NOT NULL")
            iso_dates = sorted(_parse_message_date(date) for row in rows if (date := row["message_date"]))
            archive_states = {
                row["archive_state"]: row["count"]
                for row in conn.execute("SELECT archive_state, COUNT(*) AS count FROM messages GROUP BY archive_state")
            }
        return {
            "counts": counts,
            "state": state,
            "intelligence_jobs": self.intelligence_job_status(),
            "deferred_attachment_hydration": self.deferred_attachment_hydration_status(),
            "archive_states": archive_states,
            "date_range": {"earliest": iso_dates[0] if iso_dates else None, "latest": iso_dates[-1] if iso_dates else None},
            "age": self.age_status(),
            "maintenance": self.maintenance_status(),
            "db_pool": self._pool.get_stats(),
        }

    def refresh_archive_states(self, limit: int = DEFAULT_INT) -> dict[str, int]:
        """Refresh archive states."""
        sql = "SELECT id FROM messages ORDER BY updated_at DESC"
        params: tuple[Any, ...] = ()
        if limit:
            sql += " LIMIT %s"
            params = (limit,)
        updated = 0
        with self._connect() as conn:
            for row in conn.execute(sql, params):
                self._update_archive_state_conn(conn, row["id"])
                updated += 1
        return {"updated": updated}

    def archive_status(self, message_id: str = DEFAULT_STR, limit: int = 50) -> dict[str, Any]:
        """Archive status."""
        with self._connect() as conn:
            states = {
                row["archive_state"]: row["count"]
                for row in conn.execute(
                    "SELECT archive_state, COUNT(*) AS count FROM messages GROUP BY archive_state ORDER BY archive_state"
                )
            }
            if message_id:
                rows = conn.execute(
                    """
                    SELECT id, thread_id, subject, archive_state, archive_checked_at, archive_error, raw_json_digest,
                           raw_rfc822_digest, tombstoned_at, purged_at, deletion_policy
                    FROM messages WHERE id = %s
                    """,
                    (message_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, thread_id, subject, archive_state, archive_checked_at, archive_error, raw_json_digest,
                           raw_rfc822_digest, tombstoned_at, purged_at, deletion_policy
                    FROM messages
                    WHERE archive_state != 'complete'
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                ).fetchall()
            messages: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["has_raw_json"] = bool(item.pop("raw_json_digest"))
                item["has_raw_rfc822"] = bool(item.pop("raw_rfc822_digest"))
                messages.append(item)
        return {"states": states, "messages": messages}

    def archive_incomplete_message_ids(self, limit: int = 50) -> list[str]:
        """Archive incomplete message ids."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id FROM messages
                WHERE archive_state = 'incomplete'
                  AND raw_json_digest IS NOT NULL
                  AND raw_rfc822_digest IS NULL
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [row["id"] for row in rows]

    def retention_policies(self) -> list[dict[str, Any]]:
        """Retention policies."""
        with self._connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT id, name, match_kind, match_value, action, enabled, created_at, updated_at
                    FROM retention_policies ORDER BY name
                    """
                )
            ]

    def retention_preview(self, message_id: str) -> dict[str, Any]:
        """Retention preview."""
        message = self.get_message(message_id)
        if not message:
            raise KeyError(message_id)
        labels = set(message.get("label_ids", [])) | set(message.get("labels", []))
        categories = set(message.get("categories", []))
        matched: list[dict[str, Any]] = []
        action = "tombstone"
        for policy in self.retention_policies():
            if not policy.get("enabled"):
                continue
            if policy["match_kind"] == "label" and policy["match_value"] in labels:
                matched.append(policy)
            if policy["match_kind"] == "category" and policy["match_value"] in categories:
                matched.append(policy)
        if any(policy["action"] == "purge" for policy in matched):
            action = "purge"
        return {
            "message_id": message_id,
            "action": action,
            "labels": sorted(labels),
            "categories": sorted(categories),
            "matched_policies": matched,
        }

    def apply_retention_policy(self, message_id: str, source: str = "operator", *, dry_run: bool = True) -> dict[str, Any]:
        """Apply retention policy."""
        preview = self.retention_preview(message_id)
        if dry_run:
            return {"dry_run": True, **preview}
        state = "purge_pending" if preview["action"] == "purge" else "tombstoned"
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE messages
                SET archive_state=%s,
                    deletion_source=%s,
                    deletion_policy=%s,
                    deleted_label_ids=%s,
                    tombstoned_at=CASE WHEN %s = 'tombstoned' THEN now() ELSE tombstoned_at END,
                    archive_checked_at=now(),
                    updated_at=now()
                WHERE id=%s
                """,
                (state, source, preview["action"], Jsonb(preview["labels"]), state, message_id),
            )
        self.record_operational_event(
            "retention.applied", "warning", "archive", message_id, f"Applied retention action {preview['action']}.", preview
        )
        return {"dry_run": False, "archive_state": state, **preview}

    def verify_objects(self, limit: int = DEFAULT_INT) -> dict[str, Any]:
        """Verify objects."""
        return pg_cache_archive.verify_objects(self, PG_CACHE_EXCEPTIONS, limit=limit)

    def export_archive(self, target: str) -> dict[str, Any]:
        """Export archive."""
        return pg_cache_archive.export_archive(self, PG_CACHE_EXCEPTIONS, target)

    def verify_archive_export(self, source: str) -> dict[str, Any]:
        """Verify archive export."""
        return pg_cache_archive.verify_archive_export(source, PG_CACHE_EXCEPTIONS)

    def restore_archive(self, source: str, *, dry_run: bool = True) -> dict[str, Any]:
        """Restore archive."""
        return pg_cache_archive.restore_archive(self, source, dry_run=dry_run)

    def refresh_imap_mailboxes(self) -> dict[str, int]:
        """Refresh imap mailboxes."""
        return pg_cache_imap.refresh_imap_mailboxes(cast(Any, self))

    def imap_mailboxes(self) -> list[dict[str, Any]]:
        """Imap mailboxes."""
        return pg_cache_imap.imap_mailboxes(cast(Any, self))

    def imap_mailbox(self, name: str) -> dict[str, Any]:
        """Imap mailbox."""
        return pg_cache_imap.imap_mailbox(cast(Any, self), name) or {}

    def imap_messages(self, mailbox: str, limit: int = DEFAULT_INT) -> list[dict[str, Any]]:
        """Imap messages."""
        return pg_cache_imap.imap_messages(cast(Any, self), mailbox, limit=limit or 0)

    def imap_message_bytes(self, mailbox: str, uid: int) -> bytes:
        """Imap message bytes."""
        return pg_cache_imap.imap_message_bytes(cast(Any, self), mailbox, uid) or b""

    def imap_status(self) -> dict[str, Any]:
        """Imap status."""
        return pg_cache_imap.imap_status(cast(Any, self))

    def resilience_status(self) -> dict[str, Any]:
        """Resilience status."""
        job_status = self.intelligence_job_status()
        dead_letters = self.list_dead_letter_jobs(limit=10)
        events = self.operational_events(limit=20, severity="error")
        repair = self.repair_cache(dry_run=True)
        degraded: list[str] = []
        if job_status.get("dead", 0) or dead_letters:
            degraded.append("dead_letter_jobs")
        if repair.get("missing_object_files", {}).get("count"):
            degraded.append("missing_object_files")
        if repair.get("corrupt_object_files", {}).get("count"):
            degraded.append("corrupt_object_files")
        if repair.get("stale_running_jobs", {}).get("count"):
            degraded.append("stale_running_jobs")
        if events:
            degraded.append("recent_errors")
        return {
            "ok": not degraded,
            "degraded": degraded,
            "jobs": job_status,
            "dead_letters": dead_letters,
            "recent_errors": events,
            "repair_preview": repair,
            "latest_sync_runs": self.sync_runs(limit=5),
        }

    def repair_cache(self, *, dry_run: bool = True) -> dict[str, Any]:
        """Repair cache."""
        missing: list[dict[str, Any]] = []
        corrupt: list[dict[str, Any]] = []
        stale_count = 0
        with self._connect() as conn:
            for row in conn.execute("SELECT digest, path, compression FROM content_objects ORDER BY created_at"):
                if not Path(row["path"]).exists():
                    missing.append(dict(row))
                    continue
                try:
                    self.objects.get(row["digest"], compression=row["compression"])
                except PG_CACHE_EXCEPTIONS as exc:
                    item = dict(row)
                    item["error"] = repr(exc)
                    corrupt.append(item)
            stale = conn.execute(
                """
                SELECT id FROM intelligence_jobs
                WHERE status='running' AND locked_at < now() - interval '15 minutes'
                ORDER BY locked_at, id
                """
            ).fetchall()
            stale_count = len(stale)
        reclaimed = {"reclaimed": 0} if dry_run else self.reclaim_stale_intelligence_jobs()
        if not dry_run:
            self.refresh_content_object_refs()
            self.record_operational_event(
                "repair.cache",
                "warning" if missing or corrupt else "info",
                "repair",
                "",
                "Ran cache repair.",
                {"missing_object_files": len(missing), "corrupt_object_files": len(corrupt), "reclaimed": reclaimed.get("reclaimed", 0)},
            )
        return {
            "dry_run": dry_run,
            "missing_object_files": {"count": len(missing), "items": missing[:25]},
            "corrupt_object_files": {"count": len(corrupt), "items": corrupt[:25]},
            "stale_running_jobs": {"count": stale_count, **reclaimed},
            "content_refs": "would_refresh" if dry_run else "refreshed",
        }

    def maintenance_status(self) -> dict[str, Any]:
        """Maintenance status."""
        with self._connect() as conn:
            ref_rows = conn.execute("SELECT COUNT(*) AS count FROM content_object_refs").fetchone()["count"]
            orphan_objects = conn.execute(
                """
                SELECT COUNT(*) AS count, COALESCE(SUM(original_size), 0) AS original_size, COALESCE(SUM(stored_size), 0) AS stored_size
                FROM content_objects c
                WHERE NOT EXISTS (SELECT 1 FROM content_object_refs r WHERE r.digest = c.digest)
                """
            ).fetchone()
            missing_files = 0
            for row in conn.execute("SELECT path FROM content_objects"):
                if not Path(row["path"]).exists():
                    missing_files += 1
            dims = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT embedding_dim, COUNT(*) AS chunks
                    FROM embedding_chunks GROUP BY embedding_dim ORDER BY embedding_dim NULLS FIRST
                    """
                )
            ]
            reference_kinds = [
                dict(row)
                for row in conn.execute("SELECT ref_kind, COUNT(*) AS refs FROM content_object_refs GROUP BY ref_kind ORDER BY ref_kind")
            ]
            table_stats = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT relname AS table, n_live_tup, n_dead_tup, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
                    FROM pg_stat_user_tables
                    WHERE schemaname = 'public' AND relname = ANY(%s)
                    ORDER BY relname
                    """,
                    (PROJECT_TABLES,),
                )
            ]
        return {
            "orphan_content_objects": dict(orphan_objects),
            "content_object_refs": {"count": ref_rows, "by_kind": reference_kinds},
            "missing_object_files": missing_files,
            "embedding_dimensions": dims,
            "table_stats": table_stats,
        }

    def prune_orphan_content_objects(self, *, dry_run: bool = True) -> dict[str, Any]:
        """Prune orphan content objects."""
        query = """
            WITH referenced AS (
              SELECT raw_json_digest AS digest FROM messages WHERE raw_json_digest IS NOT NULL
              UNION SELECT text_body_digest FROM messages WHERE text_body_digest IS NOT NULL
              UNION SELECT markdown_digest FROM messages WHERE markdown_digest IS NOT NULL
              UNION SELECT raw_rfc822_digest FROM messages WHERE raw_rfc822_digest IS NOT NULL
              UNION SELECT body_text_digest FROM message_parts WHERE body_text_digest IS NOT NULL
              UNION SELECT digest FROM attachments
              UNION SELECT artifact_digest FROM ingest_issues WHERE artifact_digest IS NOT NULL
            )
            SELECT digest, path, compression, original_size, stored_size
            FROM content_objects c
            WHERE NOT EXISTS (SELECT 1 FROM referenced r WHERE r.digest = c.digest)
            ORDER BY created_at, digest
        """
        with self._connect() as conn:
            rows = [dict(row) for row in conn.execute(query)]
            if not dry_run and rows:
                conn.execute("DELETE FROM content_objects WHERE digest = ANY(%s)", ([row["digest"] for row in rows],))
        if not dry_run:
            for row in rows:
                path = Path(row["path"])
                if path.exists():
                    path.unlink()
        return {
            "dry_run": dry_run,
            "objects": len(rows),
            "original_size": sum(int(row.get("original_size") or 0) for row in rows),
            "stored_size": sum(int(row.get("stored_size") or 0) for row in rows),
            "digests": [row["digest"] for row in rows[:100]],
        }

    def refresh_content_object_refs(self) -> dict[str, int]:
        """Refresh content object refs."""
        inserts = [
            ("messages", "raw_json_digest", "raw_json"),
            ("messages", "text_body_digest", "text_body"),
            ("messages", "markdown_digest", "markdown"),
            ("messages", "raw_rfc822_digest", "raw_rfc822"),
        ]
        with self._connect() as conn:
            conn.execute("DELETE FROM content_object_refs")
            total = 0
            for table, column, kind in inserts:
                result = conn.execute(
                    sql.SQL(
                        """
                    INSERT INTO content_object_refs(digest, ref_table, ref_pk, ref_column, ref_kind)
                    SELECT {column}, %s, id, %s, %s
                    FROM {table}
                    WHERE {column} IS NOT NULL
                    ON CONFLICT DO NOTHING
                    """
                    ).format(column=sql.Identifier(column), table=sql.Identifier(table)),
                    (table, column, kind),
                )
                total += int(result.rowcount or 0)
            result = conn.execute(
                """
                INSERT INTO content_object_refs(digest, ref_table, ref_pk, ref_column, ref_kind)
                SELECT body_text_digest, 'message_parts', message_id || ':' || part_id, 'body_text_digest', 'part_body'
                FROM message_parts
                WHERE body_text_digest IS NOT NULL
                ON CONFLICT DO NOTHING
                """
            )
            total += int(result.rowcount or 0)
            result = conn.execute(
                """
                INSERT INTO content_object_refs(digest, ref_table, ref_pk, ref_column, ref_kind)
                SELECT digest, 'attachments', message_id || ':' || COALESCE(part_id, '') || ':' || sha1, 'digest', 'attachment'
                FROM attachments
                WHERE digest IS NOT NULL
                ON CONFLICT DO NOTHING
                """
            )
            total += int(result.rowcount or 0)
            result = conn.execute(
                """
                INSERT INTO content_object_refs(digest, ref_table, ref_pk, ref_column, ref_kind)
                SELECT artifact_digest, 'ingest_issues', id::text, 'artifact_digest', 'ingest_artifact'
                FROM ingest_issues
                WHERE artifact_digest IS NOT NULL
                ON CONFLICT DO NOTHING
                """
            )
            total += int(result.rowcount or 0)
        return {"refs": total}

    def refresh_summary_items(self) -> dict[str, int]:
        """Refresh summary items."""
        statements = [
            (
                "thread",
                """
                INSERT INTO summary_items(
                    scope_kind, scope_id, title, summary, message_count,
                    latest_message_ts, centroid, centroid_dim, metadata_json, updated_at
                )
                SELECT 'thread',
                       COALESCE(m.thread_id, m.id),
                       COALESCE(MAX(NULLIF(m.subject, '')), COALESCE(m.thread_id, m.id)),
                       COUNT(DISTINCT m.id)::text || ' messages in thread',
                       COUNT(DISTINCT m.id),
                       MAX(m.message_ts),
                       AVG(e.embedding)::vector,
                       MAX(e.embedding_dim),
                       jsonb_build_object('message_ids', jsonb_agg(DISTINCT m.id)),
                       now()
                FROM messages m
                LEFT JOIN embedding_chunks e ON e.message_id = m.id AND e.source_kind = 'message' AND e.embedding IS NOT NULL
                GROUP BY COALESCE(m.thread_id, m.id)
                ON CONFLICT(scope_kind, scope_id) DO UPDATE SET
                  title=excluded.title, summary=excluded.summary, message_count=excluded.message_count,
                  latest_message_ts=excluded.latest_message_ts, centroid=excluded.centroid,
                  centroid_dim=excluded.centroid_dim, metadata_json=excluded.metadata_json, updated_at=now()
                """,
            ),
            (
                "contact",
                """
                INSERT INTO summary_items(
                    scope_kind, scope_id, title, summary, message_count,
                    latest_message_ts, centroid, centroid_dim, metadata_json, updated_at
                )
                SELECT 'contact',
                       m.sender_addr,
                       COALESCE(MAX(NULLIF(m.sender, '')), m.sender_addr),
                       COUNT(DISTINCT m.id)::text || ' messages from ' || m.sender_addr,
                       COUNT(DISTINCT m.id),
                       MAX(m.message_ts),
                       AVG(e.embedding)::vector,
                       MAX(e.embedding_dim),
                       jsonb_build_object('domain', MAX(m.sender_domain), 'message_ids', jsonb_agg(DISTINCT m.id)),
                       now()
                FROM messages m
                LEFT JOIN embedding_chunks e ON e.message_id = m.id AND e.source_kind = 'message' AND e.embedding IS NOT NULL
                WHERE m.sender_addr IS NOT NULL
                GROUP BY m.sender_addr
                ON CONFLICT(scope_kind, scope_id) DO UPDATE SET
                  title=excluded.title, summary=excluded.summary, message_count=excluded.message_count,
                  latest_message_ts=excluded.latest_message_ts, centroid=excluded.centroid,
                  centroid_dim=excluded.centroid_dim, metadata_json=excluded.metadata_json, updated_at=now()
                """,
            ),
            (
                "category",
                """
                INSERT INTO summary_items(
                    scope_kind, scope_id, title, summary, message_count,
                    latest_message_ts, centroid, centroid_dim, metadata_json, updated_at
                )
                SELECT 'category',
                       mc.category,
                       COALESCE(MAX(c.name), mc.category),
                       COUNT(DISTINCT m.id)::text || ' messages in category ' || mc.category,
                       COUNT(DISTINCT m.id),
                       MAX(m.message_ts),
                       AVG(e.embedding)::vector,
                       MAX(e.embedding_dim),
                       jsonb_build_object(
                         'default_hidden', COALESCE(bool_or(c.default_hidden), false),
                         'message_ids', jsonb_agg(DISTINCT m.id)
                       ),
                       now()
                FROM message_categories mc
                JOIN messages m ON m.id = mc.message_id
                LEFT JOIN categories c ON c.id = mc.category
                LEFT JOIN embedding_chunks e ON e.message_id = m.id AND e.source_kind = 'message' AND e.embedding IS NOT NULL
                GROUP BY mc.category
                ON CONFLICT(scope_kind, scope_id) DO UPDATE SET
                  title=excluded.title, summary=excluded.summary, message_count=excluded.message_count,
                  latest_message_ts=excluded.latest_message_ts, centroid=excluded.centroid,
                  centroid_dim=excluded.centroid_dim, metadata_json=excluded.metadata_json, updated_at=now()
                """,
            ),
            (
                "project",
                """
                INSERT INTO summary_items(
                    scope_kind, scope_id, title, summary, message_count,
                    latest_message_ts, centroid, centroid_dim, metadata_json, updated_at
                )
                SELECT 'project',
                       gnp.node,
                       gnp.label,
                       COUNT(DISTINCT m.id)::text || ' messages linked to project ' || gnp.label,
                       COUNT(DISTINCT m.id),
                       MAX(m.message_ts),
                       AVG(e.embedding)::vector,
                       MAX(e.embedding_dim),
                       jsonb_build_object('node', gnp.node, 'message_ids', jsonb_agg(DISTINCT m.id)),
                       now()
                FROM graph_node_profiles gnp
                JOIN graph_triples gt ON (gt.subject = gnp.node OR gt.object = gnp.node) AND gt.source_message_id IS NOT NULL
                JOIN messages m ON m.id = gt.source_message_id
                LEFT JOIN embedding_chunks e ON e.message_id = m.id AND e.source_kind = 'message' AND e.embedding IS NOT NULL
                WHERE gnp.kind = 'projects'
                GROUP BY gnp.node, gnp.label
                ON CONFLICT(scope_kind, scope_id) DO UPDATE SET
                  title=excluded.title, summary=excluded.summary, message_count=excluded.message_count,
                  latest_message_ts=excluded.latest_message_ts, centroid=excluded.centroid,
                  centroid_dim=excluded.centroid_dim, metadata_json=excluded.metadata_json, updated_at=now()
                """,
            ),
        ]
        counts: dict[str, int] = {}
        with self._connect() as conn:
            for scope, sql in statements:
                conn.execute("DELETE FROM summary_items WHERE scope_kind = %s", (scope,))
                result = conn.execute(sql)
                counts[scope] = int(result.rowcount or 0)
        return counts

    def list_summary_items(self, scope_kind: str = DEFAULT_STR, limit: int = 50) -> list[dict[str, Any]]:
        """List summary items."""
        where = ""
        params: list[Any] = []
        if scope_kind:
            where = "WHERE scope_kind = %s"
            params.append(scope_kind)
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                SELECT scope_kind, scope_id, title, summary, message_count, latest_message_ts, centroid_dim, metadata_json, updated_at
                FROM summary_items
                {where}
                ORDER BY message_count DESC, latest_message_ts DESC NULLS LAST, scope_kind, scope_id
                LIMIT %s
                """
                ).format(where=sql.SQL(where)),
                (*params, limit),
            )
            result: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["metadata"] = item.pop("metadata_json") or {}
                result.append(item)
            return result

    def refresh_timeline_views(self) -> dict[str, str]:
        """Refresh timeline views."""
        with self._connect() as conn:
            conn.execute("REFRESH MATERIALIZED VIEW message_timeline_daily")
            conn.execute("REFRESH MATERIALIZED VIEW graph_entity_emergence")
        return {"message_timeline_daily": "refreshed", "graph_entity_emergence": "refreshed"}

    def refresh_materialized_summary_views(self) -> dict[str, str]:
        """Refresh materialized summary views."""
        views = [
            "message_search_summary",
            "thread_summary",
            "contact_summary",
            "category_summary",
            "project_summary",
            "graph_node_summary",
        ]
        with self._connect() as conn:
            for view in views:
                conn.execute(sql.SQL("REFRESH MATERIALIZED VIEW {}").format(sql.Identifier(view)))
        return dict.fromkeys(views, "refreshed")

    def timeline_daily(self, limit: int = 90) -> list[dict[str, Any]]:
        """Timeline daily."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT day, messages, hydrated_messages, senders, threads
                FROM message_timeline_daily
                ORDER BY day DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in rows]

    def emerging_entities(self, limit: int = 50, kind: str = DEFAULT_STR, visibility: str = "user") -> list[dict[str, Any]]:
        """Emerging entities."""
        where = ["visibility = %s"]
        params: list[Any] = [visibility]
        if kind:
            where.append("kind = %s")
            params.append(kind)
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                SELECT node, label, kind, visibility, first_seen, last_seen, messages, triples
                FROM graph_entity_emergence
                WHERE {where_sql}
                ORDER BY first_seen DESC NULLS LAST, messages DESC, node
                LIMIT %s
                """
                ).format(where_sql=_where_sql(where)),
                (*params, limit),
            )
            return [dict(row) for row in rows]

    def analyze_storage_tables(self) -> dict[str, Any]:
        """Analyze storage tables."""
        with self._connect() as conn:
            for table in PROJECT_TABLES:
                conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(table)))
        return {"analyzed": PROJECT_TABLES}

    def storage_diagnostics(self) -> dict[str, Any]:
        """Storage diagnostics."""
        with self._connect() as conn:
            extensions = [dict(row) for row in conn.execute("SELECT extname, extversion FROM pg_extension ORDER BY extname")]
            relations = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT relname AS relation,
                           pg_total_relation_size(relid) AS total_bytes,
                           pg_relation_size(relid) AS heap_bytes,
                           pg_indexes_size(relid) AS index_bytes,
                           n_live_tup,
                           n_dead_tup
                    FROM pg_stat_user_tables
                    WHERE schemaname = 'public'
                    ORDER BY pg_total_relation_size(relid) DESC
                    """
                )
            ]
            indexes = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT schemaname, tablename, indexname,
                           pg_relation_size((schemaname || '.' || indexname)::regclass) AS bytes,
                           indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                    ORDER BY pg_relation_size((schemaname || '.' || indexname)::regclass) DESC
                    """
                )
            ]
            vector_dims = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT source_kind, embedding_dim, COUNT(*) AS chunks
                    FROM embedding_chunks
                    GROUP BY source_kind, embedding_dim
                    ORDER BY source_kind, embedding_dim
                    """
                )
            ]
            matviews = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT matviewname, ispopulated
                    FROM pg_matviews
                    WHERE schemaname = 'public'
                    ORDER BY matviewname
                    """
                )
            ]
            hnsw_indexes = [row for row in indexes if "USING hnsw" in row["indexdef"]]
        return {
            "extensions": extensions,
            "relations": relations,
            "indexes": indexes,
            "hnsw_indexes": hnsw_indexes,
            "vector_dimensions": vector_dims,
            "materialized_views": matviews,
            "maintenance": self.maintenance_status(),
        }

    def record_ingest_issue(
        self,
        message_id: str,
        source: str,
        severity: str,
        detail: str,
        artifact: dict[str, Any] | bytes | str = DEFAULT_INGEST_ARTIFACT,
    ) -> int:
        """Record ingest issue."""
        artifact_digest = ""
        with self._connect() as conn:
            if artifact:
                if isinstance(artifact, bytes):
                    stored = self.objects.put(artifact, media_type="application/octet-stream")
                elif isinstance(artifact, str):
                    stored = self.objects.put_text(artifact)
                else:
                    stored = self.objects.put_json(artifact)
                self._record_object_conn(conn, stored)
                artifact_digest = stored.digest
            row = conn.execute(
                """
                INSERT INTO ingest_issues(message_id, source, severity, detail, artifact_digest)
                VALUES(%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (message_id, source, severity, detail[:4000], artifact_digest),
            ).fetchone()
            if artifact_digest:
                self._upsert_content_ref_conn(conn, artifact_digest, "ingest_issues", str(row["id"]), "artifact_digest", "ingest_artifact")
            return int(row["id"])

    def list_ingest_issues(self, limit: int = 50) -> list[dict[str, Any]]:
        """List ingest issues."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, message_id, source, severity, detail, artifact_digest, created_at
                FROM ingest_issues
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in rows]

    def triples(self) -> list[tuple[str, str, str, str]]:
        """Triples."""
        with self._connect() as conn:
            rows = conn.execute("SELECT subject, predicate, object, source_message_id FROM graph_triples")
            return [(row["subject"], row["predicate"], row["object"], row["source_message_id"]) for row in rows]

    def add_triples(self, triples: list[tuple[str, str, str, str]]) -> None:
        """Add triples."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO graph_triples(subject, predicate, object, source_message_id)
                    VALUES(%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                triples,
            )
        self._mirror_triples_to_age(triples)

    def _mirror_triples_to_age(self, triples: list[tuple[str, str, str, str]]) -> None:
        try:
            with self._connect() as conn:
                conn.execute("SET search_path=ag_catalog, public")
                for subject, predicate, obj, source_message_id in triples[:500]:
                    cypher = (
                        "MERGE (s:Node {id: " + json.dumps(subject) + "}) "
                        "MERGE (o:Node {id: " + json.dumps(obj) + "}) "
                        "MERGE (s)-[:RELATED {predicate: "
                        + json.dumps(predicate)
                        + ", source_message_id: "
                        + json.dumps(source_message_id)
                        + "}]->(o)"
                    )
                    conn.execute(_age_sql(cypher, "v agtype"))
        except PG_CACHE_EXCEPTIONS as exc:
            self.age_mirror_error = repr(exc)
            return

    def delete_graph_triples_for_message(self, message_id: str) -> None:
        """Delete graph triples for message."""
        with self._connect() as conn:
            conn.execute("DELETE FROM graph_triples WHERE source_message_id = %s", (message_id,))

    def delete_graph_triples_for_attachment(self, sha1: str) -> None:
        """Delete graph triples for attachment."""
        attachment = f"gmeow:attachment/{sha1}"
        attachment_message = "gmeow:message/attachment_" + sha1
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM graph_triples WHERE subject = ANY(%s) OR object = ANY(%s)",
                ([attachment, attachment_message], [attachment, attachment_message]),
            )

    def graph_triples_for_message(self, message_id: str) -> list[dict[str, str]]:
        """Graph triples for message."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT subject, predicate, object, source_message_id
                FROM graph_triples
                WHERE source_message_id = %s
                ORDER BY subject, predicate, object
                """,
                (message_id,),
            )
            return [dict(row) for row in rows]

    def refresh_graph_node_profiles(self) -> dict[str, int]:
        """Refresh graph node profiles."""
        with self._connect() as conn:
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    WITH endpoints AS (
                      SELECT subject AS node, predicate, source_message_id FROM graph_triples
                      UNION ALL
                      SELECT object AS node, predicate, source_message_id FROM graph_triples
                    )
                    SELECT node, MIN(predicate) AS predicate, COUNT(*) AS degree,
                           COUNT(DISTINCT source_message_id) AS message_count
                    FROM endpoints
                    GROUP BY node
                    """
                )
            ]
            seen: list[str] = []
            for row in rows:
                node = str(row["node"])
                label = self._graph_node_label(node)
                profile = _graph_node_profile(node, label, str(row.get("predicate") or ""))
                conn.execute(
                    """
                    INSERT INTO graph_node_profiles(
                        node, label, kind, namespace, visibility, role,
                        noise_reason, degree, message_count, updated_at
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT(node) DO UPDATE SET
                      label=excluded.label,
                      kind=excluded.kind,
                      namespace=excluded.namespace,
                      visibility=excluded.visibility,
                      role=excluded.role,
                      noise_reason=excluded.noise_reason,
                      degree=excluded.degree,
                      message_count=excluded.message_count,
                      updated_at=now()
                    """,
                    (
                        node,
                        label,
                        profile["kind"],
                        profile.get("namespace"),
                        profile["visibility"],
                        profile["role"],
                        profile.get("noise_reason"),
                        row["degree"],
                        row["message_count"],
                    ),
                )
                seen.append(node)
            if seen:
                conn.execute("DELETE FROM graph_node_profiles WHERE NOT (node = ANY(%s))", (seen,))
            else:
                conn.execute("DELETE FROM graph_node_profiles")
        return {"profiles": len(seen)}

    def graph_nodes_for_message(
        self, message_id: str, visibility: str = "user", kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR
    ) -> dict[str, list[dict[str, Any]]]:
        """Graph nodes for message."""
        grouped: dict[str, list[dict[str, Any]]] = {}
        seen: set[tuple[str, str, str]] = set()
        for triple in self.graph_triples_for_message(message_id):
            for role, value in [("subject", str(triple["subject"])), ("object", str(triple["object"]))]:
                label = self._graph_node_label(value)
                profile = _graph_node_profile(value, label, triple["predicate"])
                is_current_message = value == f"gmeow:message/{message_id}"
                if not is_current_message and not _profile_allowed(profile, visibility=visibility, kind=kind, namespace=namespace):
                    continue
                node_kind = str(profile["kind"])
                key = (node_kind, value, role)
                if key in seen:
                    continue
                seen.add(key)
                grouped.setdefault(node_kind, []).append(
                    {"id": value, "label": label, "role": role, "predicate": triple["predicate"] or "", "profile": profile}
                )
        return {kind: nodes for kind, nodes in grouped.items() if nodes}

    def graph_neighbors(self, node: str, depth: int = 1, limit: int = 100) -> dict[str, Any]:
        """Graph neighbors."""
        depth = max(1, min(depth, 3))
        seen: set[str] = {node}
        frontier: set[str] = {node}
        edges: list[dict[str, Any]] = []
        with self._connect() as conn:
            for distance in range(1, depth + 1):
                if not frontier or len(edges) >= limit:
                    break
                rows = conn.execute(
                    """
                    SELECT subject, predicate, object, source_message_id
                    FROM graph_triples
                    WHERE subject = ANY(%s) OR object = ANY(%s)
                    ORDER BY source_message_id, predicate
                    LIMIT %s
                    """,
                    (list(frontier), list(frontier), max(1, limit - len(edges))),
                )
                next_frontier: set[str] = set()
                for row in rows:
                    edge = dict(row)
                    edge["distance"] = distance
                    edges.append(edge)
                    for endpoint in [str(row["subject"]), str(row["object"])]:
                        if endpoint not in seen:
                            seen.add(endpoint)
                            next_frontier.add(endpoint)
                frontier = next_frontier
        return {"node": node, "nodes": sorted(seen), "edges": edges}

    def graph_projection(
        self, limit: int = 25, prefix: str = DEFAULT_STR, visibility: str = "user", kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR
    ) -> dict[str, Any]:
        """Graph projection."""
        triples = self.triples()
        if prefix:
            triples = [triple for triple in triples if triple[0].startswith(prefix) or triple[2].startswith(prefix)]
        triples = [
            triple
            for triple in triples
            if _profile_allowed(
                _graph_node_profile(triple[0], self._graph_node_label(triple[0]), triple[1]),
                visibility=visibility,
                kind=kind,
                namespace=namespace,
            )
            or _profile_allowed(
                _graph_node_profile(triple[2], self._graph_node_label(triple[2]), triple[1]),
                visibility=visibility,
                kind=kind,
                namespace=namespace,
            )
        ]
        projection = GraphProjector().summary(cast(Any, triples), limit=limit * 10)
        filtered_top_nodes: list[dict[str, Any]] = []
        for node in projection["top_nodes"]:
            node["label"] = self._graph_node_label(node["id"])
            node["profile"] = _graph_node_profile(node["id"], node["label"])
            if _profile_allowed(node["profile"], visibility=visibility, kind=kind, namespace=namespace):
                filtered_top_nodes.append(node)
            if len(filtered_top_nodes) >= limit:
                break
        projection["top_nodes"] = filtered_top_nodes
        return projection

    def graph_path(self, source: str, target: str, max_depth: int = 4) -> dict[str, Any]:
        """Graph path."""
        result = GraphProjector().shortest_path(cast(Any, self.triples()), source, target, max_depth=max_depth)
        if not result:
            return {"source": source, "target": target, "found": False, "max_depth": max_depth}
        result["found"] = True
        total_weight = 0.0
        for edge in result["path"]:
            edge["from_label"] = self._graph_node_label(edge["from"])
            edge["to_label"] = self._graph_node_label(edge["to"])
            stats = self._edge_stats(edge["from"], edge["predicate"].lstrip("^"), edge["to"])
            if not stats and edge["predicate"].startswith("^"):
                stats = self._edge_stats(edge["to"], edge["predicate"].lstrip("^"), edge["from"])
            if stats:
                edge["weight"] = stats["weight"]
                edge["evidence_count"] = stats["evidence_count"]
                edge["message_count"] = stats["message_count"]
                total_weight += float(stats["weight"])
        result["score"] = round(1.0 / (1.0 + total_weight), 6) if result["path"] else 0.0
        result["total_weight"] = round(total_weight, 6)
        return result

    def refresh_graph_edge_stats(self) -> dict[str, int]:
        """Refresh graph edge stats."""
        with self._connect() as conn:
            conn.execute("DELETE FROM graph_edge_stats")
            result = conn.execute(
                """
                INSERT INTO graph_edge_stats(
                    subject, predicate, object, evidence_count, message_count,
                    first_message_ts, last_message_ts, weight
                )
                SELECT gt.subject,
                       gt.predicate,
                       gt.object,
                       COUNT(*) AS evidence_count,
                       COUNT(DISTINCT gt.source_message_id) AS message_count,
                       MIN(m.message_ts) AS first_message_ts,
                       MAX(m.message_ts) AS last_message_ts,
                       CASE
                         WHEN gt.predicate IN (
                           'gmeow:from', 'gmeow:to', 'gmeow:aboutProject',
                           'https://schema.org/about', 'http://xmlns.com/foaf/0.1/topic'
                         ) THEN 0.35
                         WHEN gt.predicate IN (
                           'gmeow:hasAttachmentEvent', 'gmeow:hasCalendarEvent', 'gmeow:calendarAttendee',
                           'gmeow:calendarOrganizer', 'gmeow:calendarLocation', 'gmeow:calendarStart'
                         ) THEN 0.35
                         WHEN gt.predicate IN (
                           'gmeow:hasAttachmentDocument', 'gmeow:hasDocument', 'gmeow:hasDocumentAuthor',
                           'gmeow:documentTitle', 'gmeow:documentAuthor'
                         ) THEN 0.45
                         WHEN gt.predicate IN (
                           'gmeow:attachmentMentions', 'gmeow:attachmentMentionsCanonical',
                           'gmeow:documentMentions', 'gmeow:calendarMentions'
                         ) THEN 0.55
                         WHEN gt.predicate IN ('gmeow:ocrMentions', 'gmeow:visionMentions') THEN 0.8
                         WHEN gt.predicate IN (
                           'gmeow:mentionsEntity', 'gmeow:hasAttachment', 'gmeow:hasLabel',
                           'gmeow:containsFile', 'gmeow:attachmentContainsFile'
                         ) THEN 0.65
                         WHEN gt.predicate LIKE 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' THEN 1.25
                         ELSE 1.0
                       END / LN(COUNT(*) + 2.0) AS weight
                FROM graph_triples gt
                LEFT JOIN messages m ON m.id = gt.source_message_id
                GROUP BY gt.subject, gt.predicate, gt.object
                """
            )
            return {"edges": int(result.rowcount or 0)}

    def _graph_edge_rows(self, limit_edges: int = 50_000) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT subject, predicate, object, evidence_count, message_count, first_message_ts, last_message_ts, weight
                FROM graph_edge_stats
                ORDER BY weight ASC, message_count DESC
                LIMIT %s
                """,
                (limit_edges,),
            )
            return [dict(row) for row in rows]

    def graph_weighted_path(self, source: str, target: str, max_depth: int = 4, limit_edges: int = 50_000) -> dict[str, Any]:
        """Graph weighted path."""
        result = WeightedGraphProjector().weighted_path(self._graph_edge_rows(limit_edges=limit_edges), source, target, max_depth=max_depth)
        if not result:
            return {"source": source, "target": target, "found": False, "max_depth": max_depth}
        result["found"] = True
        for edge in result["path"]:
            edge["from_label"] = self._graph_node_label(edge["from"])
            edge["to_label"] = self._graph_node_label(edge["to"])
        return result

    def graph_centrality(
        self,
        metric: str = "pagerank",
        limit: int = 25,
        kind: str = DEFAULT_STR,
        namespace: str = DEFAULT_STR,
        visibility: str = "user",
        limit_edges: int = 50_000,
    ) -> list[dict[str, Any]]:
        """Graph centrality."""
        rows = WeightedGraphProjector().centrality(self._graph_edge_rows(limit_edges=limit_edges), metric=metric)
        rows.sort(key=lambda item: (-item["score"], item["node"]))
        results: list[dict[str, Any]] = []
        for item in rows:
            node = str(item["node"])
            score = float(cast(float, item["score"]))
            label = self._graph_node_label(node)
            profile = _graph_node_profile(node, label)
            if not _algorithm_profile_allowed(profile, visibility=visibility, kind=kind, namespace=namespace):
                continue
            results.append({"node": node, "label": label, "score": round(score, 8), "profile": profile})
            if len(results) >= limit:
                break
        return results

    def graph_components(
        self, mode: str = "weak", limit: int = 10, min_size: int = 2, visibility: str = "user", limit_edges: int = 50_000
    ) -> list[dict[str, Any]]:
        """Graph components."""
        edge_rows = self._graph_edge_rows(limit_edges=limit_edges)
        degree: dict[str, int] = {}
        for edge in edge_rows:
            degree[str(edge["subject"])] = degree.get(str(edge["subject"]), 0) + 1
            degree[str(edge["object"])] = degree.get(str(edge["object"]), 0) + 1
        components = WeightedGraphProjector().components(edge_rows, mode=mode)
        results: list[dict[str, Any]] = []
        for component in sorted(components, key=lambda values: (-len(values), sorted(values)[0] if values else "")):
            visible_nodes: list[dict[str, Any]] = []
            for node in sorted(component, key=lambda value: (-degree.get(value, 0), value)):
                label = self._graph_node_label(node)
                profile = _graph_node_profile(node, label)
                if _algorithm_profile_allowed(profile, visibility=visibility):
                    visible_nodes.append({"node": node, "label": label, "degree": degree.get(node, 0), "profile": profile})
            if len(visible_nodes) < min_size:
                continue
            results.append({"mode": mode, "size": len(component), "visible_size": len(visible_nodes), "nodes": visible_nodes[:50]})
            if len(results) >= limit:
                break
        return results

    def graph_related_nodes(
        self,
        node: str,
        **options: object,
    ) -> list[dict[str, Any]]:
        """Graph related nodes."""
        limit = int(cast(int, options.get("limit", 25)))
        raw_max_weight = options.get("max_weight")
        max_weight = float(cast(float, raw_max_weight)) if isinstance(raw_max_weight, (int, float)) else 0.0
        kind = str(options.get("kind") or "")
        namespace = str(options.get("namespace") or "")
        visibility = str(options.get("visibility") or "user")
        limit_edges = int(cast(int, options.get("limit_edges", 50_000)))
        candidates = WeightedGraphProjector().related_nodes(
            self._graph_edge_rows(limit_edges=limit_edges), node, limit=limit * 5, max_weight=max_weight
        )
        results: list[dict[str, Any]] = []
        for item in candidates:
            node_id = str(item["node"])
            label = self._graph_node_label(node_id)
            profile = _graph_node_profile(node_id, label)
            if not _algorithm_profile_allowed(profile, visibility=visibility, kind=kind, namespace=namespace):
                continue
            results.append({**item, "label": label, "profile": profile})
            if len(results) >= limit:
                break
        return results

    def graph_bridges(
        self, limit: int = 25, kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR, visibility: str = "user", limit_edges: int = 50_000
    ) -> list[dict[str, Any]]:
        """Graph bridges."""
        results = self.graph_centrality(
            metric="betweenness", limit=limit, kind=kind, namespace=namespace, visibility=visibility, limit_edges=limit_edges
        )
        for item in results:
            item["broker_score"] = item.pop("score")
        return results

    def graph_cycles(
        self, limit: int = 25, max_cycle_len: int = 8, visibility: str = "user", limit_edges: int = 50_000
    ) -> list[dict[str, Any]]:
        """Graph cycles."""
        cycles = WeightedGraphProjector().cycles(
            self._graph_edge_rows(limit_edges=limit_edges), limit=limit * 5, max_cycle_len=max_cycle_len
        )
        results: list[dict[str, Any]] = []
        for cycle in cycles:
            nodes: list[dict[str, Any]] = []
            for node in cycle:
                label = self._graph_node_label(node)
                profile = _graph_node_profile(node, label)
                if _algorithm_profile_allowed(profile, visibility=visibility):
                    nodes.append({"node": node, "label": label, "profile": profile})
            if len(nodes) < MIN_GRAPH_CYCLE_LENGTH:
                continue
            results.append({"length": len(cycle), "nodes": nodes})
            if len(results) >= limit:
                break
        return results

    def graph_recommended_messages(
        self,
        message_id: str,
        limit: int = 20,
        include_categories: list[str] = DEFAULT_LIST_STR,
        exclude_categories: list[str] = DEFAULT_LIST_STR,
    ) -> list[dict[str, Any]]:
        """Graph recommended messages."""
        important_predicates = (
            "gmeow:from",
            "gmeow:to",
            "gmeow:aboutProject",
            "https://schema.org/about",
            "http://xmlns.com/foaf/0.1/topic",
            "gmeow:mentionsEntity",
            "gmeow:hasLabel",
            "gmeow:hasAttachment",
            "gmeow:hasAttachmentDocument",
            "gmeow:hasDocument",
            "gmeow:hasAttachmentEvent",
            "gmeow:hasCalendarEvent",
            "gmeow:attachmentMentions",
            "gmeow:attachmentMentionsCanonical",
            "gmeow:documentMentions",
            "gmeow:calendarMentions",
        )
        with self._connect() as conn:
            source_nodes = [
                row["object"]
                for row in conn.execute(
                    """
                    SELECT DISTINCT object FROM graph_triples
                    WHERE source_message_id = %s AND predicate = ANY(%s)
                    """,
                    (message_id, list(important_predicates)),
                )
            ]
            if not source_nodes:
                return []
            rows = conn.execute(
                """
                SELECT gt.source_message_id AS message_id,
                       COUNT(*) AS shared_edges,
                       COUNT(DISTINCT gt.object) AS shared_nodes,
                       ARRAY_AGG(DISTINCT gt.object ORDER BY gt.object) AS nodes
                FROM graph_triples gt
                WHERE gt.object = ANY(%s)
                  AND gt.source_message_id IS NOT NULL
                  AND gt.source_message_id != %s
                  AND gt.predicate = ANY(%s)
                GROUP BY gt.source_message_id
                ORDER BY shared_nodes DESC, shared_edges DESC, gt.source_message_id
                LIMIT %s
                """,
                (source_nodes, message_id, list(important_predicates), limit * 5),
            )
            found = [dict(row) for row in rows]
        graph_edges: list[dict[str, Any]] = []
        for row in found:
            weight = 1.0 / (float(row["shared_edges"]) + float(row["shared_nodes"]) + 1.0)
            graph_edges.append(
                {
                    "subject": message_id,
                    "predicate": "gmeow:sharesNeighborhood",
                    "object": row["message_id"],
                    "weight": weight,
                    "evidence_count": row["shared_edges"],
                    "message_count": row["shared_nodes"],
                }
            )
        ranked = WeightedGraphProjector().related_nodes(graph_edges, message_id, limit=limit * 5)
        by_id = {row["message_id"]: row for row in found}
        results: list[dict[str, Any]] = []
        for item in ranked:
            matched = by_id.get(str(item["node"]))
            if not matched:
                continue
            message = self.get_message(str(matched["message_id"]))
            if not message or not self.category_allowed(message.get("categories", []), include_categories, exclude_categories):
                continue
            shared: list[dict[str, Any]] = []
            nodes = cast(list[object], matched.get("nodes") or [])
            for node in nodes[:20]:
                node_id = str(node)
                label = self._graph_node_label(node_id)
                profile = _graph_node_profile(node_id, label)
                if not _algorithm_profile_allowed(profile):
                    continue
                shared.append({"node": node_id, "label": label, "profile": profile})
            results.append(
                {
                    "message_id": matched["message_id"],
                    "score": item["score"],
                    "distance": item["distance"],
                    "shared_edges": matched["shared_edges"],
                    "shared_nodes": matched["shared_nodes"],
                    "shared": shared,
                    "message": self.compact_message(message),
                }
            )
            if len(results) >= limit:
                break
        return results

    def _edge_stats(self, subject: str, predicate: str, obj: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT evidence_count, message_count, weight FROM graph_edge_stats WHERE subject = %s AND predicate = %s AND object = %s",
                (subject, predicate, obj),
            ).fetchone()
            return dict(row) if row else {}

    def graph_ranked_nodes(
        self,
        limit: int = 25,
        kind: str = DEFAULT_STR,
        predicate: str = DEFAULT_STR,
        *,
        include_noise: bool = False,
        namespace: str = DEFAULT_STR,
        visibility: str = "user",
    ) -> list[dict[str, Any]]:
        """Graph ranked nodes."""
        where: list[str] = []
        params: list[Any] = []
        if predicate:
            where.append("predicate = %s")
            params.append(predicate)
        kind_prefixes = _kind_prefixes(kind)
        if kind_prefixes:
            placeholders = ", ".join(["%s"] * len(kind_prefixes))
            where.append(f"(subject LIKE ANY(ARRAY[{placeholders}]) OR object LIKE ANY(ARRAY[{placeholders}]))")
            params.extend(kind_prefixes)
            params.extend(kind_prefixes)
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                WITH endpoints AS (
                  SELECT subject AS node, predicate, source_message_id FROM graph_triples {where_sql}
                  UNION ALL
                  SELECT object AS node, predicate, source_message_id FROM graph_triples {where_sql}
                )
                SELECT node, COUNT(*) AS degree, COUNT(DISTINCT source_message_id) AS messages
                FROM endpoints GROUP BY node ORDER BY messages DESC, degree DESC, node LIMIT %s
                """
                ).format(where_sql=_where_clause_sql(where)),
                (*params, *params, limit * 5),
            )
            results: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                node = str(item["node"])
                item["label"] = self._graph_node_label(node)
                item["profile"] = _graph_node_profile(node, str(item["label"]), predicate)
                if not _profile_allowed(item["profile"], visibility="all" if include_noise else visibility, kind=kind, namespace=namespace):
                    continue
                results.append(item)
                if len(results) >= limit:
                    break
            return results

    def graph_projects(self, limit: int = 25) -> list[dict[str, Any]]:
        """Graph projects."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT p.subject AS project, COUNT(DISTINCT p.source_message_id) AS messages, COUNT(*) AS degree
                FROM graph_triples p
                WHERE p.predicate = %s AND p.object = %s
                GROUP BY p.subject
                ORDER BY messages DESC, degree DESC, project
                LIMIT %s
                """,
                (RDF_TYPE, DOAP + "Project", limit),
            )
            projects: list[dict[str, Any]] = []
            for row in rows:
                project = dict(row)
                project["label"] = self._graph_node_label(project["project"])
                project["repositories"] = [
                    repo["object"]
                    for repo in conn.execute(
                        "SELECT DISTINCT object FROM graph_triples WHERE subject = %s AND predicate IN (%s, %s) ORDER BY object",
                        (project["project"], DOAP + "repository", "https://schema.org/codeRepository"),
                    )
                ]
                projects.append(project)
            return projects

    def ontology_profile(self) -> dict[str, Any]:
        """Ontology profile."""
        counts = {}
        with self._connect() as conn:
            for name, profile in ONTOLOGY_PROFILE.items():
                namespace = profile["namespace"]
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS triples, COUNT(DISTINCT source_message_id) AS messages
                    FROM graph_triples WHERE predicate LIKE %s OR object LIKE %s
                    """,
                    (f"{namespace}%", f"{namespace}%"),
                ).fetchone()
                counts[name] = {**profile, "triples": row["triples"], "messages": row["messages"]}
        return {"ontologies": counts}

    def graph_related_messages(
        self,
        message_id: str,
        limit: int = 20,
        include_categories: list[str] = DEFAULT_LIST_STR,
        exclude_categories: list[str] = DEFAULT_LIST_STR,
    ) -> list[dict[str, Any]]:
        """Graph related messages."""
        with self._connect() as conn:
            nodes = {
                row["object"]
                for row in conn.execute(
                    """
                    SELECT object FROM graph_triples
                    WHERE source_message_id = %s
                      AND predicate IN ('gmeow:from', 'gmeow:to', 'gmeow:mentionsEntity', 'gmeow:hasLabel', 'gmeow:hasAttachment')
                    """,
                    (message_id,),
                )
            }
            if not nodes:
                return []
            rows = conn.execute(
                """
                SELECT source_message_id AS message_id, COUNT(*) AS shared_edges
                FROM graph_triples
                WHERE object = ANY(%s) AND source_message_id IS NOT NULL AND source_message_id != %s
                GROUP BY source_message_id
                ORDER BY shared_edges DESC, source_message_id
                LIMIT %s
                """,
                (list(nodes), message_id, limit),
            )
            found = [dict(row) for row in rows]
        results: list[dict[str, Any]] = []
        for row in found:
            message = self.get_message(row["message_id"])
            if not message or not self.category_allowed(message.get("categories", []), include_categories, exclude_categories):
                continue
            results.append({"message_id": row["message_id"], "shared_edges": row["shared_edges"], "message": self.compact_message(message)})
        return results

    def graph_top_nodes(
        self,
        prefix: str = DEFAULT_STR,
        limit: int = 25,
        *,
        include_noise: bool = False,
        kind: str = DEFAULT_STR,
        namespace: str = DEFAULT_STR,
        visibility: str = "user",
    ) -> list[dict[str, Any]]:
        """Graph top nodes."""
        if kind or namespace or visibility or include_noise:
            return self._graph_top_profiled_nodes(
                prefix=prefix,
                limit=limit,
                include_noise=include_noise,
                kind=kind,
                namespace=namespace,
                visibility=visibility,
            )
        return self._graph_top_triple_nodes(
            prefix=prefix,
            limit=limit,
            include_noise=include_noise,
            kind=kind,
            namespace=namespace,
            visibility=visibility,
        )

    def _graph_top_profiled_nodes(
        self,
        *,
        prefix: str,
        limit: int,
        include_noise: bool,
        kind: str,
        namespace: str,
        visibility: str,
    ) -> list[dict[str, Any]]:
        where, params = self._graph_top_profile_filters(
            prefix=prefix, include_noise=include_noise, kind=kind, namespace=namespace, visibility=visibility
        )
        rows = self._graph_top_profile_rows(where, params, limit)
        if not rows:
            self.refresh_graph_node_profiles()
            rows = self._graph_top_profile_rows(where, params, limit)
        return [self._graph_top_profile_item(row) for row in rows]

    def _graph_top_profile_filters(
        self,
        *,
        prefix: str,
        include_noise: bool,
        kind: str,
        namespace: str,
        visibility: str,
    ) -> tuple[list[str], list[Any]]:
        where: list[str] = []
        params: list[Any] = []
        if prefix:
            where.append("node LIKE %s")
            params.append(f"{prefix}%")
        if kind:
            where.append("kind = %s")
            params.append(_normalize_kind(kind))
        if namespace:
            where.append("namespace = %s")
            params.append(namespace)
        if not include_noise and visibility not in {"all", "any"}:
            where.append("visibility = %s")
            params.append(visibility)
        return where, params

    def _graph_top_profile_rows(self, where: list[str], params: list[Any], limit: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                SELECT node, label, kind, namespace, visibility, role, noise_reason, degree, message_count AS messages
                FROM graph_node_profiles
                {where_sql}
                ORDER BY message_count DESC, degree DESC, node
                LIMIT %s
                """
                ).format(where_sql=_where_clause_sql(where)),
                (*params, limit),
            )
            return [dict(row) for row in rows]

    def _graph_top_profile_item(self, row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item["profile"] = {
            "kind": item.pop("kind"),
            "namespace": item.pop("namespace"),
            "visibility": item.pop("visibility"),
            "role": item.pop("role"),
            "noise_reason": item.pop("noise_reason"),
        }
        return item

    def _graph_top_triple_nodes(
        self,
        *,
        prefix: str,
        limit: int,
        include_noise: bool,
        kind: str,
        namespace: str,
        visibility: str,
    ) -> list[dict[str, Any]]:
        subject_where = "WHERE subject LIKE %s" if prefix else ""
        object_where = "WHERE object LIKE %s" if prefix else ""
        params = [f"{prefix}%"] if prefix else []
        with self._connect() as conn:
            rows = conn.execute(
                sql.SQL(
                    """
                WITH endpoints AS (
                  SELECT subject AS node, source_message_id FROM graph_triples {subject_where}
                  UNION ALL
                  SELECT object AS node, source_message_id FROM graph_triples {object_where}
                )
                SELECT node, COUNT(*) AS degree, COUNT(DISTINCT source_message_id) AS messages
                FROM endpoints
                GROUP BY node
                ORDER BY messages DESC, degree DESC, node
                LIMIT %s
                """
                ).format(subject_where=sql.SQL(subject_where), object_where=sql.SQL(object_where)),
                (*params, *params, limit * 10),
            )
            results: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["label"] = self._graph_node_label(item["node"])
                item["profile"] = _graph_node_profile(item["node"], item["label"])
                if not _profile_allowed(item["profile"], visibility="all" if include_noise else visibility, kind=kind, namespace=namespace):
                    continue
                results.append(item)
                if len(results) >= limit:
                    break
            return results

    def graph_search(
        self,
        term: str,
        limit: int = 50,
        *,
        include_noise: bool = False,
        kind: str = DEFAULT_STR,
        namespace: str = DEFAULT_STR,
        visibility: str = "user",
    ) -> list[dict[str, Any]]:
        """Graph search."""
        like = f"%{term}%"
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT subject, predicate, object, source_message_id
                FROM graph_triples
                WHERE subject ILIKE %s OR predicate ILIKE %s OR object ILIKE %s
                LIMIT %s
                """,
                (like, like, like, limit if include_noise else limit * 25),
            )
            results: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                subject = str(item["subject"])
                obj = str(item["object"])
                predicate = str(item["predicate"] or "")
                subject_label = self._graph_node_label(subject)
                object_label = self._graph_node_label(obj)
                subject_profile = _graph_node_profile(subject, subject_label, predicate)
                object_profile = _graph_node_profile(obj, object_label, predicate)
                item["subject_label"] = subject_label
                item["object_label"] = object_label
                item["subject_profile"] = subject_profile
                item["object_profile"] = object_profile
                search_visibility = "all" if include_noise else visibility
                if not (
                    _profile_allowed(subject_profile, visibility=search_visibility, kind=kind, namespace=namespace)
                    or _profile_allowed(object_profile, visibility=search_visibility, kind=kind, namespace=namespace)
                ):
                    continue
                results.append(item)
                if len(results) >= limit:
                    break
            return results

    def _graph_node_label(self, value: str) -> str:
        if value.startswith("gmeow:label/"):
            return self.label_name(value.rsplit("/", 1)[-1])
        return _graph_node_label(value)

    def category_allowed(self, categories: list[str], include_categories: list[str], exclude_categories: list[str]) -> bool:
        """Category allowed."""
        category_set = set(categories)
        if include_categories:
            return bool(category_set & set(include_categories))
        excluded = set(exclude_categories) if exclude_categories else self.default_excluded_categories()
        return not bool(category_set & excluded)
