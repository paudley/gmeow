# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Archive and object verification helpers for PgCache.

Exposes the cache surface used to materialize archive exports, reconcile object-store contents,
and audit retention runs. The helpers stay close to the durable cache so verification stays in
the same transactional boundary as the data it inspects.
"""

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import zstandard as zstd
from psycopg import sql
from psycopg.types.json import Jsonb

from .pg_cache_helpers import blake3_digest, import_insert_sql

_EXPORT_TABLES = (
    "labels",
    "threads",
    "messages",
    "message_parts",
    "attachments",
    "content_objects",
    "content_object_refs",
    "retention_policies",
    "imap_mailboxes",
    "imap_message_uids",
)
_SKIP_MESSAGE_COLUMNS = {"search_tsv"}


def verify_objects(cache: Any, exceptions: tuple[type[BaseException], ...], limit: int = 0) -> dict[str, Any]:
    """Verify cached content objects."""
    query = "SELECT digest, compression, path FROM content_objects ORDER BY created_at"
    params: tuple[Any, ...] = ()
    if limit:
        query += " LIMIT %s"
        params = (limit,)
    counts = {"ok": 0, "missing": 0, "corrupt": 0, "error": 0}
    with cache.connection() as conn:
        rows = [dict(row) for row in conn.execute(query, params)]
        for row in rows:
            status, detail = _verify_object_row(cache, row, exceptions)
            conn.execute(
                """
                UPDATE content_objects
                SET verification_status=%s, verification_error=%s, verified_at=now()
                WHERE digest=%s
                """,
                (status, detail, row["digest"]),
            )
            counts[status] += 1
    return {"checked": sum(counts.values()), **counts}


def _verify_object_row(cache: Any, row: dict[str, Any], exceptions: tuple[type[BaseException], ...]) -> tuple[str, str]:
    try:
        if not Path(row["path"]).exists():
            return "missing", ""
        cache.objects.get(row["digest"], compression=row["compression"])
    except OSError as exc:
        return "corrupt", repr(exc)
    except exceptions as exc:
        return "error", repr(exc)
    return "ok", ""


def export_archive(cache: Any, exceptions: tuple[type[BaseException], ...], target: str) -> dict[str, Any]:
    """Export an archive manifest and object payloads."""
    target_path = Path(target)
    objects_dir = target_path / "objects"
    target_path.mkdir(parents=True, exist_ok=True)
    objects_dir.mkdir(parents=True, exist_ok=True)
    export_id = 0
    try:
        export_id, tables = _export_open(cache, target_path)
        copied = _export_copy_objects(tables["content_objects"], objects_dir)
        manifest = _export_manifest(tables)
        manifest_path = target_path / "manifest.json"
        manifest_text = json.dumps(manifest, indent=2, sort_keys=True, default=str)
        manifest_path.write_text(manifest_text)
        digest = blake3_digest(manifest_text.encode())
        _export_finalize(cache, export_id, digest, len(tables["content_objects"]), len(tables["messages"]))
        _export_record_event(cache, export_id, target_path, copied)
        return _export_summary(export_id, target_path, manifest_path, digest, tables, copied)
    except exceptions as exc:
        _export_mark_failed(cache, export_id, exc)
        raise


def _export_open(cache: Any, target_path: Path) -> tuple[int, dict[str, list[dict[str, Any]]]]:
    with cache.connection() as conn:
        row = conn.execute("INSERT INTO archive_exports(export_path) VALUES(%s) RETURNING id", (str(target_path),)).fetchone()
        export_id = int(row["id"]) if row else 0
        tables: dict[str, list[dict[str, Any]]] = {}
        for table in _EXPORT_TABLES:
            tables[table] = [dict(item) for item in conn.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))]
    return export_id, tables


def _export_copy_objects(content_objects: list[dict[str, Any]], objects_dir: Path) -> int:
    copied = 0
    for obj in content_objects:
        source = Path(obj["path"])
        relative = Path("blake3") / obj["digest"][:2] / obj["digest"][2:4] / source.name
        destination = objects_dir / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.exists():
            shutil.copy2(source, destination)
            copied += 1
        obj["export_relative_path"] = str(Path("objects") / relative)
    return copied


def _export_manifest(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return {
        "format": "gmeow.archive.v1",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "object_count": len(tables["content_objects"]),
        "message_count": len(tables["messages"]),
        "tables": tables,
    }


def _export_finalize(cache: Any, export_id: int, digest: str, object_count: int, message_count: int) -> None:
    if not export_id:
        return
    with cache.connection() as conn:
        conn.execute(
            """
            UPDATE archive_exports
            SET status='complete', manifest_digest=%s, object_count=%s, message_count=%s, finished_at=now()
            WHERE id=%s
            """,
            (digest, object_count, message_count, export_id),
        )


def _export_record_event(cache: Any, export_id: int, target_path: Path, copied: int) -> None:
    cache.record_operational_event(
        "archive.exported",
        "info",
        "archive",
        str(export_id) if export_id else "",
        "Exported archive manifest.",
        {"target": str(target_path), "objects_copied": copied},
    )


def _export_summary(
    export_id: int, target_path: Path, manifest_path: Path, digest: str, tables: dict[str, list[dict[str, Any]]], copied: int
) -> dict[str, Any]:
    return {
        "id": export_id,
        "target": str(target_path),
        "manifest": str(manifest_path),
        "manifest_digest": digest,
        "objects": len(tables["content_objects"]),
        "objects_copied": copied,
        "messages": len(tables["messages"]),
    }


def _export_mark_failed(cache: Any, export_id: int, exc: BaseException) -> None:
    if not export_id:
        return
    with cache.connection() as conn:
        conn.execute("UPDATE archive_exports SET status='failed', error=%s, finished_at=now() WHERE id=%s", (repr(exc), export_id))


def verify_archive_export(source: str, exceptions: tuple[type[BaseException], ...]) -> dict[str, Any]:
    """Verify an exported archive manifest and object payloads."""
    root = Path(source)
    manifest = json.loads((root / "manifest.json").read_text())
    missing: list[str] = []
    corrupt: list[str] = []
    for obj in manifest.get("tables", {}).get("content_objects", []):
        path = root / obj.get("export_relative_path", "")
        if not path.exists():
            missing.append(obj["digest"])
            continue
        if not _archive_object_valid(path, obj, exceptions):
            corrupt.append(obj["digest"])
    return {
        "source": str(root),
        "objects": len(manifest.get("tables", {}).get("content_objects", [])),
        "messages": len(manifest.get("tables", {}).get("messages", [])),
        "missing": missing,
        "corrupt": corrupt,
        "ok": not missing and not corrupt,
    }


def _archive_object_valid(path: Path, obj: dict[str, Any], exceptions: tuple[type[BaseException], ...]) -> bool:
    try:
        content = path.read_bytes()
        if obj.get("compression") == "zstd":
            content = zstd.ZstdDecompressor().decompress(content)
        return bool(blake3_digest(content) == obj["digest"])
    except exceptions:
        return False


def restore_archive(cache: Any, source: str, *, dry_run: bool = True) -> dict[str, Any]:
    """Restore an exported archive manifest."""
    root = Path(source)
    manifest = json.loads((root / "manifest.json").read_text())
    tables = manifest.get("tables", {})
    if dry_run:
        return {"dry_run": True, "messages": len(tables.get("messages", [])), "objects": len(tables.get("content_objects", []))}
    _restore_copy_objects(cache, tables.get("content_objects", []), root)
    with cache.connection() as conn:
        _restore_content_objects(cache, conn, tables.get("content_objects", []))
        _restore_labels(conn, tables.get("labels", []))
        _restore_threads(conn, tables.get("threads", []))
        _restore_messages(conn, tables.get("messages", []))
        _restore_message_parts(conn, tables.get("message_parts", []))
        _restore_attachments(conn, tables.get("attachments", []))
        cache.refresh_content_object_refs()
    verified = cache.verify_objects()
    cache.record_operational_event("archive.restored", "info", "archive", "", "Restored archive export.", {"source": str(root), **verified})
    return {
        "dry_run": False,
        "messages": len(tables.get("messages", [])),
        "objects": len(tables.get("content_objects", [])),
        "verification": verified,
    }


def _restore_copy_objects(cache: Any, content_objects: list[dict[str, Any]], root: Path) -> None:
    for obj in content_objects:
        exported = root / obj.get("export_relative_path", "")
        destination = cache.objects.path_for_digest(obj["digest"], compression=obj["compression"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        if exported.exists() and not destination.exists():
            shutil.copy2(exported, destination)


def _restore_content_objects(cache: Any, conn: Any, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO content_objects(
                digest, path, media_type, compression, original_size,
                stored_size, verification_status, verified_at, metadata_json
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(digest) DO UPDATE SET
              path=excluded.path,
              media_type=excluded.media_type,
              compression=excluded.compression,
              original_size=excluded.original_size,
              stored_size=excluded.stored_size,
              verification_status=excluded.verification_status,
              verified_at=excluded.verified_at, metadata_json=excluded.metadata_json
            """,
            (
                row["digest"],
                str(cache.objects.path_for_digest(row["digest"], compression=row["compression"])),
                row["media_type"],
                row["compression"],
                row["original_size"],
                row["stored_size"],
                row.get("verification_status", "unverified"),
                row.get("verified_at"),
                Jsonb(row.get("metadata_json") or {}),
            ),
        )


def _restore_labels(conn: Any, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO labels(id, name, type, raw_json)
            VALUES(%s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name,
              type=excluded.type,
              raw_json=excluded.raw_json
            """,
            (row["id"], row["name"], row.get("type"), Jsonb(row.get("raw_json") or {})),
        )


def _restore_threads(conn: Any, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO threads(id, snippet, raw_json, updated_at)
            VALUES(%s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
              snippet=excluded.snippet,
              raw_json=excluded.raw_json,
              updated_at=excluded.updated_at
            """,
            (row["id"], row.get("snippet"), Jsonb(row.get("raw_json") or {}), row.get("updated_at")),
        )


def _restore_messages(conn: Any, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        keys = [key for key in row if key not in _SKIP_MESSAGE_COLUMNS]
        values = tuple(Jsonb(row[key]) if isinstance(row[key], (dict, list)) else row[key] for key in keys)
        columns_sql = sql.SQL(", ").join(sql.Identifier(key) for key in keys)
        placeholders_sql = sql.SQL(", ").join([sql.Placeholder()] * len(keys))
        updates_sql = sql.SQL(", ").join(sql.SQL("{col}=EXCLUDED.{col}").format(col=sql.Identifier(key)) for key in keys if key != "id")
        statement = sql.SQL("INSERT INTO messages({cols}) VALUES({vals}) ON CONFLICT(id) DO UPDATE SET {upd}").format(
            cols=columns_sql, vals=placeholders_sql, upd=updates_sql
        )
        conn.execute(statement, values)


def _restore_message_parts(conn: Any, rows: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM message_parts")
    for row in rows:
        keys = list(row.keys())
        conn.execute(
            import_insert_sql("message_parts", keys, ["message_id", "part_id"]),
            tuple(Jsonb(row[key]) if isinstance(row[key], (dict, list)) else row[key] for key in keys),
        )


def _restore_attachments(conn: Any, rows: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM attachments")
    for row in rows:
        keys = list(row.keys())
        conn.execute(
            import_insert_sql(
                "attachments",
                keys,
                ["message_id", "part_id", "sha1"],
                conflict_target=sql.SQL("message_id, COALESCE(part_id, ''), sha1"),
            ),
            tuple(Jsonb(row[key]) if isinstance(row[key], (dict, list)) else row[key] for key in keys),
        )
