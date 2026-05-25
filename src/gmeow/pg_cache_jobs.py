# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Manage intelligence job persistence for the PostgreSQL cache.

The module isolates enqueue, claim, retry, dead-letter, and backfill-completion queries for analysis
jobs. It lets the main cache delegate job workflow details to focused database helpers.
"""

from collections.abc import Iterator
from typing import Any, Protocol, Self, cast

from psycopg import sql
from psycopg.types.json import Jsonb

from .pg_cache_helpers import worker_id as _worker_id

DEFAULT_INT = cast(int, None)
DEFAULT_STR = cast(str, None)
DEFAULT_DICT_ANY = cast(dict[str, Any], None)
DEFAULT_TARGETS = cast(list[tuple[str, str]], None)


class _JobContext(Protocol):
    """Context manager returned by job database helpers."""

    def __enter__(self) -> object:
        """Enter the context manager."""
        ...

    def __exit__(self, *args: object) -> None:
        """Exit the context manager."""
        ...


class _JobResult(Protocol):
    """Database result surface used by job helpers."""

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


class _JobConnection(_JobContext, Protocol):
    """Database connection surface used by job helpers."""

    def __enter__(self) -> Self:
        """Enter the connection context manager."""
        ...

    def execute(self, query: object, params: object = ()) -> _JobResult:
        """Execute a statement."""
        ...

    def transaction(self) -> _JobContext:
        """Open a transaction context."""
        ...


class JobCache(Protocol):
    """Database/cache surface used by intelligence job helpers."""

    def connection(self) -> _JobConnection:
        """Open a row-dict database connection."""
        ...

    def record_operational_event(
        self,
        event_type: str,
        severity: str,
        component: str,
        subject_id: str,
        detail: str,
        metadata: dict[str, Any] = DEFAULT_DICT_ANY,
    ) -> int:
        """Record an operational event."""
        ...

    def reclaim_stale_intelligence_jobs(self, stale_after_seconds: int = 900) -> dict[str, int]:
        """Reclaim stale intelligence jobs."""
        ...

    def attachments_for_message(self, message_id: str) -> list[dict[str, Any]]:
        """Return message attachments."""
        ...

    def intelligence_target_status(self, targets: list[tuple[str, str]]) -> dict[str, Any]:
        """Return intelligence status for targets."""
        ...


def enqueue_intelligence_job(cache: JobCache, kind: str, target_id: str, payload: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
    """Enqueue intelligence job."""
    with cache.connection() as conn:
        conn.execute(
            """
            INSERT INTO intelligence_jobs(kind, target_id, status, next_run_at, payload_json)
            VALUES(%s, %s, 'pending', now(), %s)
            ON CONFLICT(kind, target_id) DO UPDATE
            SET status='pending',
                last_error=NULL,
                locked_by=NULL,
                locked_at=NULL,
                next_run_at=now(),
                dead_lettered_at=NULL,
                payload_json=CASE
                  WHEN excluded.payload_json = '{}'::jsonb THEN intelligence_jobs.payload_json
                  ELSE excluded.payload_json
                END,
                updated_at=now()
            """,
            (kind, target_id, Jsonb(payload or {})),
        )
    cache.record_operational_event("job.enqueued", "info", "intelligence", target_id, f"Enqueued {kind} intelligence job.", {"kind": kind})


def enqueue_all_intelligence_jobs(cache: JobCache) -> dict[str, int]:
    """Enqueue all intelligence jobs."""
    messages = attachments = 0
    with cache.connection() as conn:
        for row in conn.execute("SELECT id FROM messages"):
            conn.execute(
                """
                INSERT INTO intelligence_jobs(kind, target_id, status, next_run_at)
                VALUES('message', %s, 'pending', now())
                ON CONFLICT(kind, target_id) DO UPDATE SET
                  status='pending',
                  next_run_at=now(),
                  dead_lettered_at=NULL,
                  locked_by=NULL,
                  locked_at=NULL
                """,
                (row["id"],),
            )
            messages += 1
        for row in conn.execute("SELECT DISTINCT sha1 FROM attachments"):
            conn.execute(
                """
                INSERT INTO intelligence_jobs(kind, target_id, status, next_run_at)
                VALUES('attachment', %s, 'pending', now())
                ON CONFLICT(kind, target_id) DO UPDATE SET
                  status='pending',
                  next_run_at=now(),
                  dead_lettered_at=NULL,
                  locked_by=NULL,
                  locked_at=NULL
                """,
                (row["sha1"],),
            )
            attachments += 1
    cache.record_operational_event(
        "job.bulk_enqueued",
        "info",
        "intelligence",
        "",
        "Enqueued all intelligence jobs.",
        {"messages": messages, "attachments": attachments},
    )
    return {"messages": messages, "attachments": attachments}


def reclaim_stale_intelligence_jobs(cache: JobCache, stale_after_seconds: int = 900) -> dict[str, int]:
    """Reclaim stale intelligence jobs."""
    with cache.connection() as conn:
        result = conn.execute(
            """
            UPDATE intelligence_jobs
            SET status='pending',
                locked_by=NULL,
                locked_at=NULL,
                next_run_at=now(),
                last_error=COALESCE(last_error, 'reclaimed stale running job'),
                updated_at=now()
            WHERE status='running'
              AND locked_at < now() - (%s || ' seconds')::interval
            """,
            (stale_after_seconds,),
        )
        reclaimed = int(result.rowcount or 0)
    if reclaimed:
        cache.record_operational_event(
            "job.reclaimed",
            "warning",
            "intelligence",
            "",
            "Reclaimed stale running intelligence jobs.",
            {"count": reclaimed, "stale_after_seconds": stale_after_seconds},
        )
    return {"reclaimed": reclaimed}


def claim_next_intelligence_job(cache: JobCache, worker_id: str = DEFAULT_STR, targets: object = DEFAULT_TARGETS) -> dict[str, Any]:
    """Claim next intelligence job."""
    worker_id = worker_id or _worker_id()
    cache.reclaim_stale_intelligence_jobs()
    target_where = sql.SQL("")
    target_params: list[Any] = []
    if isinstance(targets, list):
        typed_targets = cast(list[tuple[str, str]], targets)
        if not typed_targets:
            return {}
        clauses: list[Any] = []
        for kind, target_id in typed_targets:
            clauses.append(sql.SQL("(kind = %s AND target_id = %s)"))
            target_params.extend([kind, target_id])
        target_where = sql.SQL("AND ({})").format(sql.SQL(" OR ").join(clauses))
    with cache.connection() as conn, conn.transaction():
        row = conn.execute(
            sql.SQL(
                """
                SELECT id, kind, target_id, attempts, max_attempts, payload_json
                FROM intelligence_jobs
                WHERE status = 'pending'
                  AND next_run_at <= now()
                  {target_where}
                ORDER BY next_run_at, created_at, id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            ).format(target_where=target_where),
            tuple(target_params),
        ).fetchone()
        if not row:
            return {}
        conn.execute(
            """
            UPDATE intelligence_jobs
            SET status='running',
                attempts=attempts+1,
                locked_by=%s,
                locked_at=now(),
                updated_at=now()
            WHERE id = %s
            """,
            (worker_id, row["id"]),
        )
        item = dict(row)
        item["locked_by"] = worker_id
        return item


def intelligence_target_status(cache: JobCache, targets: list[tuple[str, str]]) -> dict[str, Any]:
    """Return intelligence status for target jobs."""
    if not targets:
        return {"counts": {}, "pending": [], "running": [], "done": [], "dead": [], "missing": [], "terminal": True}
    counts: dict[str, int] = {}
    pending: list[dict[str, str]] = []
    running: list[dict[str, str]] = []
    done: list[dict[str, str]] = []
    dead: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    clauses: list[Any] = []
    params: list[Any] = []
    for kind, target_id in targets:
        clauses.append(sql.SQL("(kind = %s AND target_id = %s)"))
        params.extend([kind, target_id])
    with cache.connection() as conn:
        rows = conn.execute(
            sql.SQL(
                """
                SELECT kind, target_id, status
                FROM intelligence_jobs
                WHERE {target_where}
                """
            ).format(target_where=sql.SQL(" OR ").join(clauses)),
            tuple(params),
        ).fetchall()
    for row in rows:
        kind = str(row["kind"])
        target_id = str(row["target_id"])
        status = str(row["status"])
        item = {"kind": kind, "target_id": target_id, "status": status}
        key = (kind, target_id)
        seen.add(key)
        counts[status] = counts.get(status, 0) + 1
        if status == "done":
            done.append(item)
        elif status == "dead":
            dead.append(item)
        elif status == "running":
            running.append(item)
        else:
            pending.append(item)
    missing = [{"kind": kind, "target_id": target_id, "status": "missing"} for kind, target_id in targets if (kind, target_id) not in seen]
    if missing:
        counts["missing"] = len(missing)
    return {
        "counts": counts,
        "pending": pending,
        "running": running,
        "done": done,
        "dead": dead,
        "missing": missing,
        "terminal": not pending and not running and not missing,
    }


def message_backfill_complete(cache: JobCache, message_id: str, *, require_raw: bool = True) -> dict[str, Any]:
    """Return whether a message has all backfill artifacts."""
    with cache.connection() as conn:
        row = conn.execute("SELECT id, hydrated, raw_rfc822_digest FROM messages WHERE id = %s", (message_id,)).fetchone()
    if not row:
        return {"complete": False, "exists": False, "reasons": ["missing_message"], "targets": []}
    reasons: list[str] = []
    if not row["hydrated"]:
        reasons.append("not_hydrated")
    if require_raw and not row["raw_rfc822_digest"]:
        reasons.append("missing_raw_rfc822")
    targets = [("message", message_id)]
    targets.extend(("attachment", str(attachment["sha1"])) for attachment in cache.attachments_for_message(message_id))
    target_status = cache.intelligence_target_status(targets)
    if target_status["missing"]:
        reasons.append("missing_intelligence_jobs")
    if target_status["pending"] or target_status["running"]:
        reasons.append("intelligence_pending")
    if target_status["dead"]:
        reasons.append("intelligence_dead")
    complete = (
        not reasons and all(item["status"] == "done" for item in target_status["done"]) and len(target_status["done"]) == len(targets)
    )
    return {"complete": complete, "exists": True, "reasons": reasons, "targets": targets, "target_status": target_status}


def complete_intelligence_job(cache: JobCache, job_id: int) -> None:
    """Complete intelligence job."""
    with cache.connection() as conn:
        conn.execute(
            """
            UPDATE intelligence_jobs
            SET status='done',
                last_error=NULL,
                locked_by=NULL,
                locked_at=NULL,
                completed_at=now(),
                updated_at=now()
            WHERE id = %s
            """,
            (job_id,),
        )
    cache.record_operational_event("job.completed", "info", "intelligence", str(job_id), "Completed intelligence job.", {"job_id": job_id})


def fail_intelligence_job(cache: JobCache, job_id: int, error: str) -> None:
    """Fail intelligence job."""
    with cache.connection() as conn:
        row = conn.execute(
            "SELECT id, kind, target_id, attempts, max_attempts, payload_json FROM intelligence_jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
        if not row:
            return
        attempts = int(cast(int, row["attempts"] or 0))
        max_attempts = int(cast(int, row["max_attempts"] or 5))
        if attempts >= max_attempts:
            _dead_letter_job(conn, row, job_id, attempts, error)
            dead = True
        else:
            _schedule_retry(conn, job_id, attempts, error)
            dead = False
    cache.record_operational_event(
        "job.dead_lettered" if dead else "job.retry_scheduled",
        "error" if dead else "warning",
        "intelligence",
        str(job_id),
        error[:2000],
        {"job_id": job_id, "attempts": attempts, "max_attempts": max_attempts},
    )


def _dead_letter_job(conn: _JobConnection, row: dict[str, Any], job_id: int, attempts: int, error: str) -> None:
    conn.execute(
        """
        INSERT INTO dead_letter_jobs(source_table, source_id, kind, target_id, attempts, last_error, payload_json)
        VALUES('intelligence_jobs', %s, %s, %s, %s, %s, %s)
        ON CONFLICT(source_table, source_id) DO UPDATE
        SET attempts=excluded.attempts,
            last_error=excluded.last_error,
            payload_json=excluded.payload_json,
            requeued_at=NULL,
            cleared_at=NULL
        """,
        (job_id, row["kind"], row["target_id"], attempts, error[:2000], Jsonb(row["payload_json"] or {})),
    )
    conn.execute(
        """
        UPDATE intelligence_jobs
        SET status='dead',
            last_error=%s,
            locked_by=NULL,
            locked_at=NULL,
            failed_at=now(),
            dead_lettered_at=now(),
            updated_at=now()
        WHERE id = %s
        """,
        (error[:2000], job_id),
    )


def _schedule_retry(conn: _JobConnection, job_id: int, attempts: int, error: str) -> None:
    delay = min(3600, 2 ** max(0, attempts - 1) * 30)
    conn.execute(
        """
        UPDATE intelligence_jobs
        SET status='pending',
            last_error=%s,
            locked_by=NULL,
            locked_at=NULL,
            next_run_at=now() + (%s || ' seconds')::interval,
            failed_at=now(),
            updated_at=now()
        WHERE id = %s
        """,
        (error[:2000], delay, job_id),
    )


def intelligence_job_status(cache: JobCache) -> dict[str, int]:
    """Intelligence job status."""
    with cache.connection() as conn:
        return {
            str(row["status"]): int(cast(int, row["count"]))
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM intelligence_jobs GROUP BY status")
        }


def list_intelligence_jobs(cache: JobCache, status: str = DEFAULT_STR, limit: int = 50) -> list[dict[str, Any]]:
    """List intelligence jobs."""
    where = "WHERE status = %s" if status else ""
    params: tuple[Any, ...] = (status, limit) if status else (limit,)
    with cache.connection() as conn:
        rows = conn.execute(
            sql.SQL(
                """
            SELECT id, kind, target_id, status, attempts, max_attempts, locked_by, locked_at, next_run_at,
                   last_error, created_at, updated_at, completed_at, failed_at, dead_lettered_at
            FROM intelligence_jobs
            {where}
            ORDER BY updated_at DESC, id DESC
            LIMIT %s
            """
            ).format(where=sql.SQL(where)),
            params,
        )
        return [dict(row) for row in rows]


def list_dead_letter_jobs(cache: JobCache, limit: int = 50, *, include_closed: bool = False) -> list[dict[str, Any]]:
    """List dead letter jobs."""
    where = "" if include_closed else "WHERE requeued_at IS NULL AND cleared_at IS NULL"
    with cache.connection() as conn:
        rows = conn.execute(
            sql.SQL(
                """
            SELECT id, source_table, source_id, kind, target_id, attempts, last_error, payload_json,
                   created_at, requeued_at, cleared_at
            FROM dead_letter_jobs
            {where}
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """
            ).format(where=sql.SQL(where)),
            (limit,),
        )
        return [dict(row) for row in rows]


def retry_dead_letter_jobs(cache: JobCache, limit: int = DEFAULT_INT) -> dict[str, int]:
    """Retry dead letter jobs."""
    with cache.connection() as conn:
        rows = conn.execute(
            """
            SELECT id, source_id FROM dead_letter_jobs
            WHERE requeued_at IS NULL AND cleared_at IS NULL
            ORDER BY created_at, id
            LIMIT %s
            """,
            (limit or 10_000,),
        )
        items = [dict(row) for row in rows]
        if not items:
            return {"requeued": 0}
        ids = [int(cast(int, item["id"])) for item in items]
        source_ids = [int(cast(int, item["source_id"])) for item in items]
        conn.execute(
            """
            UPDATE intelligence_jobs
            SET status='pending',
                locked_by=NULL,
                locked_at=NULL,
                next_run_at=now(),
                dead_lettered_at=NULL,
                last_error=NULL,
                updated_at=now()
            WHERE id = ANY(%s)
            """,
            (source_ids,),
        )
        conn.execute("UPDATE dead_letter_jobs SET requeued_at=now() WHERE id = ANY(%s)", (ids,))
    cache.record_operational_event(
        "dead_letter.requeued", "warning", "intelligence", "", "Requeued dead-letter intelligence jobs.", {"count": len(items)}
    )
    return {"requeued": len(items)}


def clear_dead_letter_job(cache: JobCache, dead_letter_id: int) -> dict[str, Any]:
    """Clear dead letter job."""
    with cache.connection() as conn:
        result = conn.execute(
            "UPDATE dead_letter_jobs SET cleared_at=now() WHERE id = %s AND cleared_at IS NULL RETURNING id", (dead_letter_id,)
        ).fetchone()
    cleared = bool(result)
    if cleared:
        cache.record_operational_event(
            "dead_letter.cleared", "info", "intelligence", str(dead_letter_id), "Cleared dead-letter job.", {"id": dead_letter_id}
        )
    return {"cleared": cleared, "id": dead_letter_id}
