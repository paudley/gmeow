# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Archive and object verification helpers for PgCache."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import zstandard as zstd
from psycopg import sql
from psycopg.types.json import Jsonb

from .pg_cache_helpers import blake3_digest, import_insert_sql


def verify_objects(cache: Any, exceptions: tuple[type[BaseException], ...], limit: int | None = None) -> dict[str, Any]:
    """Verify cached content objects."""
    query = "SELECT digest, compression, path FROM content_objects ORDER BY created_at"
    params: tuple[Any, ...] = ()
    if limit is not None:
        query += " LIMIT %s"
        params = (limit,)
    ok = missing = corrupt = error = 0
    with cache._connect() as conn:
        rows = [dict(row) for row in conn.execute(query, params)]
        for row in rows:
            status = "ok"
            detail = None
            try:
                if not Path(row["path"]).exists():
                    status = "missing"
                else:
                    cache.objects.get(row["digest"], compression=row["compression"])
            except OSError as exc:
                status = "corrupt"
                detail = repr(exc)
            except exceptions as exc:
                status = "error"
                detail = repr(exc)
            conn.execute(
                """
                UPDATE content_objects
                SET verification_status=%s, verification_error=%s, verified_at=now()
                WHERE digest=%s
                """,
                (status, detail, row["digest"]),
            )
            if status == "ok":
                ok += 1
            elif status == "missing":
                missing += 1
            elif status == "corrupt":
                corrupt += 1
            else:
                error += 1
    return {"checked": ok + missing + corrupt + error, "ok": ok, "missing": missing, "corrupt": corrupt, "error": error}


def export_archive(cache: Any, exceptions: tuple[type[BaseException], ...], target: str) -> dict[str, Any]:
    """Export an archive manifest and object payloads."""
    target_path = Path(target)
    objects_dir = target_path / "objects"
    target_path.mkdir(parents=True, exist_ok=True)
    objects_dir.mkdir(parents=True, exist_ok=True)
    export_id: int | None = None
    try:
        with cache._connect() as conn:
            row = conn.execute("INSERT INTO archive_exports(export_path) VALUES(%s) RETURNING id", (str(target_path),)).fetchone()
            export_id = int(row["id"]) if row else None
            tables = {}
            for table in [
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
            ]:
                tables[table] = [dict(item) for item in conn.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))]
        copied = 0
        for obj in tables["content_objects"]:
            source = Path(obj["path"])
            relative = Path("blake3") / obj["digest"][:2] / obj["digest"][2:4] / source.name
            destination = objects_dir / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source.exists():
                shutil.copy2(source, destination)
                copied += 1
            obj["export_relative_path"] = str(Path("objects") / relative)
        manifest = {
            "format": "gmeow.archive.v1",
            "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "object_count": len(tables["content_objects"]),
            "message_count": len(tables["messages"]),
            "tables": tables,
        }
        manifest_path = target_path / "manifest.json"
        manifest_text = json.dumps(manifest, indent=2, sort_keys=True, default=str)
        manifest_path.write_text(manifest_text)
        digest = blake3_digest(manifest_text.encode())
        with cache._connect() as conn:
            if export_id is not None:
                conn.execute(
                    """
                    UPDATE archive_exports
                    SET status='complete', manifest_digest=%s, object_count=%s, message_count=%s, finished_at=now()
                    WHERE id=%s
                    """,
                    (digest, len(tables["content_objects"]), len(tables["messages"]), export_id),
                )
        cache.record_operational_event(
            "archive.exported",
            "info",
            "archive",
            str(export_id) if export_id else None,
            "Exported archive manifest.",
            {"target": str(target_path), "objects_copied": copied},
        )
        return {
            "id": export_id,
            "target": str(target_path),
            "manifest": str(manifest_path),
            "manifest_digest": digest,
            "objects": len(tables["content_objects"]),
            "objects_copied": copied,
            "messages": len(tables["messages"]),
        }
    except exceptions as exc:
        if export_id is not None:
            with cache._connect() as conn:
                conn.execute("UPDATE archive_exports SET status='failed', error=%s, finished_at=now() WHERE id=%s", (repr(exc), export_id))
        raise


def verify_archive_export(source: str, exceptions: tuple[type[BaseException], ...]) -> dict[str, Any]:
    """Verify an exported archive manifest and object payloads."""
    root = Path(source)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    missing = []
    corrupt = []
    for obj in manifest.get("tables", {}).get("content_objects", []):
        path = root / obj.get("export_relative_path", "")
        if not path.exists():
            missing.append(obj["digest"])
            continue
        try:
            content = path.read_bytes()
            if obj.get("compression") == "zstd":
                content = zstd.ZstdDecompressor().decompress(content)
            if blake3_digest(content) != obj["digest"]:
                corrupt.append(obj["digest"])
        except exceptions:
            corrupt.append(obj["digest"])
    return {
        "source": str(root),
        "objects": len(manifest.get("tables", {}).get("content_objects", [])),
        "messages": len(manifest.get("tables", {}).get("messages", [])),
        "missing": missing,
        "corrupt": corrupt,
        "ok": not missing and not corrupt,
    }


def restore_archive(cache: Any, source: str, *, dry_run: bool = True) -> dict[str, Any]:
    """Restore an exported archive manifest."""
    root = Path(source)
    manifest = json.loads((root / "manifest.json").read_text())
    tables = manifest.get("tables", {})
    if dry_run:
        return {"dry_run": True, "messages": len(tables.get("messages", [])), "objects": len(tables.get("content_objects", []))}
    for obj in tables.get("content_objects", []):
        exported = root / obj.get("export_relative_path", "")
        destination = cache.objects.path_for_digest(obj["digest"], compression=obj["compression"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        if exported.exists() and not destination.exists():
            shutil.copy2(exported, destination)
    with cache._connect() as conn:
        for row in tables.get("content_objects", []):
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
        for row in tables.get("labels", []):
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
        for row in tables.get("threads", []):
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
        for row in tables.get("messages", []):
            keys = [key for key in row if key != "search_tsv"]
            values = [row[key] for key in keys]
            placeholders = ", ".join(["%s"] * len(keys))
            columns = ", ".join(keys)
            updates = ", ".join(f"{key}=excluded.{key}" for key in keys if key != "id")
            conn.execute(
                f"INSERT INTO messages({columns}) VALUES({placeholders}) ON CONFLICT(id) DO UPDATE SET {updates}",
                tuple(Jsonb(value) if isinstance(value, (dict, list)) else value for value in values),
            )
        conn.execute("DELETE FROM message_parts")
        for row in tables.get("message_parts", []):
            keys = list(row.keys())
            conn.execute(
                import_insert_sql("message_parts", keys, ["message_id", "part_id"]),
                tuple(Jsonb(row[key]) if isinstance(row[key], (dict, list)) else row[key] for key in keys),
            )
        conn.execute("DELETE FROM attachments")
        for row in tables.get("attachments", []):
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
        cache.refresh_content_object_refs()
    verified = cache.verify_objects()
    cache.record_operational_event("archive.restored", "info", "archive", None, "Restored archive export.", {"source": str(root), **verified})
    return {
        "dry_run": False,
        "messages": len(tables.get("messages", [])),
        "objects": len(tables.get("content_objects", [])),
        "verification": verified,
    }
