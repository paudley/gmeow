# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Build the FastAPI application for Gmeow.

This module wires configuration, storage, Gmail clients, search indexes, and maintenance services
into the HTTP API. It also mounts the MCP adapter and exposes operational endpoints used by local
operators.
"""

import asyncio
import faulthandler
import logging
import signal
import sys
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from typing import Any, cast
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from .runtime_config import RuntimeConfig
from .gmail import GmailClient, GoogleGmailClient, UserOAuthGmailClient
from .gmail_actions import (
    apply_message_label,
    archive_message,
    remove_message_label,
    set_message_read_state,
    set_message_star_state,
)
from .maintenance import MaintenanceScheduler
from .mcp_server import build_mcp_app
from .object_store import CasAttachmentStore, ObjectStore
from .pg_cache import PgCache
from .protocols import IntelligenceGraph, NoGraph
from .resilience import degraded_status, readiness, startup_self_check
from .semantic_pg import PgSemanticIndex
from .sync import MissingGmailClient, SyncService
from .text_index import TantivyMessageIndex

DEFAULT_FLOAT = cast(float, None)
DEFAULT_INT = cast(int, None)
DEFAULT_LIST_STR = cast(list[str], None)
DEFAULT_STR = cast(str, None)
LOGGER = logging.getLogger(__name__)
SLOW_REQUEST_SECONDS = 10.0


class SearchRequest(BaseModel):
    """Represent SearchRequest data and behavior."""

    query: str
    limit: int = 20
    after: str = DEFAULT_STR
    before: str = DEFAULT_STR
    include_categories: list[str] = DEFAULT_LIST_STR
    exclude_categories: list[str] = DEFAULT_LIST_STR
    compact: bool = True
    body_chars: int = 500
    graph_kind: str = DEFAULT_STR
    graph_namespace: str = DEFAULT_STR
    graph_visibility: str = "user"
    include_noise: bool = False
    source_kind: str = DEFAULT_STR


class GraphNeighborhoodRequest(BaseModel):
    """Represent GraphNeighborhoodRequest data and behavior."""

    node: str
    depth: int = 1
    limit: int = 100


class GraphPathRequest(BaseModel):
    """Represent GraphPathRequest data and behavior."""

    source: str
    target: str
    max_depth: int = 4


class GraphCypherRequest(BaseModel):
    """Represent GraphCypherRequest data and behavior."""

    query: str
    columns: str = "value agtype"
    limit: int = 100


class PruneRequest(BaseModel):
    """Represent PruneRequest data and behavior."""

    dry_run: bool = True


class LabelRequest(BaseModel):
    """Represent LabelRequest data and behavior."""

    label_id: str


class ReadStateRequest(BaseModel):
    """Represent ReadStateRequest data and behavior."""

    read: bool = True


class StarStateRequest(BaseModel):
    """Represent StarStateRequest data and behavior."""

    starred: bool = True


class PeopleAliasRequest(BaseModel):
    """Represent PeopleAliasRequest data and behavior."""

    address: str
    person_key: str
    display_name: str = DEFAULT_STR
    note: str = DEFAULT_STR


class PathRequest(BaseModel):
    """Represent PathRequest data and behavior."""

    path: str
    dry_run: bool = True


class RetentionRequest(BaseModel):
    """Represent RetentionRequest data and behavior."""

    source: str = "operator"
    dry_run: bool = True


def build_services(config: RuntimeConfig) -> tuple[PgCache, CasAttachmentStore, PgSemanticIndex, IntelligenceGraph, SyncService]:
    """Build services."""
    config.ensure_dirs()
    objects = ObjectStore(config.object_store_dir)
    attachments = CasAttachmentStore(objects, analysis_config=config.attachment_analysis)
    text_index = TantivyMessageIndex(config.tantivy_dir)
    postgres_dsn = config.database_dsn()
    cache = PgCache(postgres_dsn, objects, text_index)
    semantic = PgSemanticIndex(
        postgres_dsn, config.embedding_model, config.embedding_endpoint, config.semantic_chunk_size, config.semantic_chunk_overlap
    )
    graph: IntelligenceGraph = NoGraph()
    gmail: GmailClient = MissingGmailClient()
    service_account_info = config.service_account_info()
    user_credentials_info = config.user_credentials_info()
    if config.auth_mode == "service_account" and config.subject and (service_account_info or config.service_account_file.exists()):
        if service_account_info:
            gmail = GoogleGmailClient.from_service_account_info(service_account_info, config.subject)
        else:
            gmail = GoogleGmailClient.from_service_account_file(config.service_account_file, config.subject)
    elif config.auth_mode == "user_oauth" and (user_credentials_info or config.user_credentials_file.exists()):
        if user_credentials_info:
            gmail = UserOAuthGmailClient.from_credentials_info(user_credentials_info)
        else:
            gmail = UserOAuthGmailClient.from_credentials_file(config.user_credentials_file)
    sync = SyncService(config, cache, attachments, semantic, gmail, graph=graph)
    return cache, attachments, semantic, graph, sync


def list_messages_endpoint(request: Request, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    """Messages."""
    cache: PgCache = request.app.state.cache
    return cache.list_messages(limit=limit, offset=offset)


def _install_stack_dump_handler() -> None:
    """Install in-process stack dumps for production hang diagnosis."""
    faulthandler.enable(file=sys.stderr, all_threads=True)
    try:
        faulthandler.register(signal.SIGUSR1, file=sys.stderr, all_threads=True)
    except RuntimeError:
        LOGGER.debug("SIGUSR1 stack dump handler is already registered.")


async def _slow_request_watchdog(request: Request, request_id: str, started: float) -> None:
    await asyncio.sleep(SLOW_REQUEST_SECONDS)
    elapsed = time.monotonic() - started
    LOGGER.warning(
        "Slow request active id=%s method=%s path=%s elapsed=%.3fs",
        request_id,
        request.method,
        request.url.path,
        elapsed,
    )
    faulthandler.dump_traceback(file=sys.stderr, all_threads=True)


def create_app(config: RuntimeConfig) -> FastAPI:
    """Create app."""
    _install_stack_dump_handler()
    cache, attachments, semantic, graph, sync = build_services(config)
    mcp_app = build_mcp_app(cache=cache, sync=sync, attachments=attachments)
    scheduler = MaintenanceScheduler(config.maintenance, cache, sync, attachments, semantic, graph=graph)
    sync.maintenance_scheduler = scheduler
    startup_status = startup_self_check(config, cache, sync, semantic)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        """Lifespan."""
        scheduler.start()
        try:
            if not mcp_app.routes:
                yield
            else:
                async with mcp_app.router.lifespan_context(mcp_app):
                    yield
        finally:
            await scheduler.stop()

    app = FastAPI(title="Gmeow", version="0.1.0", lifespan=lifespan)
    app.state.config = config
    app.state.cache = cache
    app.state.attachments = attachments
    app.state.semantic = semantic
    app.state.graph = graph
    app.state.sync = sync
    app.state.maintenance = scheduler
    app.state.startup_status = startup_status
    app.get("/api/v1/messages")(list_messages_endpoint)

    _register_access_and_health_routes(app, config, cache, sync)
    _register_operations_routes(app, cache)
    _register_archive_routes(app, cache, sync)
    _register_maintenance_routes(app, cache, scheduler)
    _register_summary_routes(app, cache)
    _register_sync_routes(app, cache, sync)
    _register_message_routes(app, cache, sync)
    _register_attachment_routes(app, cache, attachments)
    _register_search_routes(app, cache, sync, semantic)
    _register_graph_routes(app, cache)
    _register_action_people_routes(app, cache, sync)

    app.router.routes.extend(mcp_app.routes)

    return app


def _register_access_and_health_routes(app: FastAPI, config: RuntimeConfig, cache: PgCache, sync: SyncService) -> None:
    @app.middleware("http")
    async def loopback_guard(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """Loopback guard."""
        client_host = request.client.host if request.client else ""
        host_header = request.headers.get("host", "").split(":")[0]
        allowed = {"127.0.0.1", "localhost", "::1"}
        test_clients = {"testclient"}
        if client_host not in allowed | test_clients or host_header not in allowed:
            return Response("Gmeow only serves loopback clients by default.\n", status_code=403)
        request_id = uuid4().hex[:12]
        started = time.monotonic()
        watchdog = asyncio.create_task(_slow_request_watchdog(request, request_id, started))
        try:
            return await call_next(request)
        finally:
            watchdog.cancel()
            with suppress(asyncio.CancelledError):
                await watchdog
            elapsed = time.monotonic() - started
            if elapsed >= SLOW_REQUEST_SECONDS:
                LOGGER.warning(
                    "Slow request completed id=%s method=%s path=%s elapsed=%.3fs",
                    request_id,
                    request.method,
                    request.url.path,
                    elapsed,
                )

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        """Health."""
        return {"ok": True, "subject_configured": bool(config.subject), "gmail_configured": sync.gmail_available()}

    @app.get("/api/v1/health/live")
    def health_live() -> dict[str, Any]:
        """Health live."""
        return {"ok": True}

    @app.get("/api/v1/health/ready")
    def health_ready() -> dict[str, Any]:
        """Health ready."""
        return readiness(cache, app.state.startup_status)

    @app.get("/api/v1/health/degraded")
    def health_degraded() -> dict[str, Any]:
        """Health degraded."""
        return degraded_status(cache, app.state.startup_status)

    _route_refs = (
        loopback_guard,
        health,
        health_live,
        health_ready,
        health_degraded,
    )
    _ = _route_refs

def _register_operations_routes(app: FastAPI, cache: PgCache) -> None:
    @app.get("/api/v1/status/resilience")
    def status_resilience() -> dict[str, Any]:
        """Status resilience."""
        return cache.resilience_status()

    @app.get("/api/v1/ops/events")
    def operational_events(limit: int = 100, component: str = DEFAULT_STR, severity: str = DEFAULT_STR) -> list[dict[str, Any]]:
        """Operational events."""
        return cache.operational_events(limit=limit, component=component, severity=severity)

    @app.get("/api/v1/jobs/intelligence")
    def intelligence_jobs(status: str = DEFAULT_STR, limit: int = 50) -> list[dict[str, Any]]:
        """Intelligence jobs."""
        return cache.list_intelligence_jobs(status=status, limit=limit)

    @app.get("/api/v1/jobs/dead-letter")
    def dead_letter_jobs(limit: int = 50, *, include_closed: bool = False) -> list[dict[str, Any]]:
        """Dead letter jobs."""
        return cache.list_dead_letter_jobs(limit=limit, include_closed=include_closed)

    @app.post("/api/v1/jobs/dead-letter/retry")
    def retry_dead_letter_jobs(limit: int = DEFAULT_INT) -> dict[str, int]:
        """Retry dead letter jobs."""
        return cache.retry_dead_letter_jobs(limit=limit)

    @app.post("/api/v1/jobs/dead-letter/{dead_letter_id}/clear")
    def clear_dead_letter_job(dead_letter_id: int) -> dict[str, Any]:
        """Clear dead letter job."""
        return cache.clear_dead_letter_job(dead_letter_id)

    @app.post("/api/v1/repair/cache")
    def repair_cache(request: PruneRequest) -> dict[str, Any]:
        """Repair cache."""
        return cache.repair_cache(dry_run=request.dry_run)

    _route_refs = (
        status_resilience,
        operational_events,
        intelligence_jobs,
        dead_letter_jobs,
        retry_dead_letter_jobs,
        clear_dead_letter_job,
        repair_cache,
    )
    _ = _route_refs


def _register_archive_routes(app: FastAPI, cache: PgCache, sync: SyncService) -> None:
    @app.get("/api/v1/archive/status")
    def archive_status(message_id: str = DEFAULT_STR, limit: int = 50) -> dict[str, Any]:
        """Archive status."""
        return cache.archive_status(message_id=message_id, limit=limit)

    @app.post("/api/v1/archive/refresh")
    def archive_refresh(limit: int = DEFAULT_INT) -> dict[str, int]:
        """Archive refresh."""
        return cache.refresh_archive_states(limit=limit)

    @app.post("/api/v1/archive/complete")
    def archive_complete(limit: int = 25) -> dict[str, Any]:
        """Archive complete."""
        try:
            return sync.complete_archive(limit=limit)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/archive/verify-objects")
    def archive_verify_objects(limit: int = DEFAULT_INT) -> dict[str, Any]:
        """Archive verify objects."""
        return cache.verify_objects(limit=limit)

    @app.post("/api/v1/archive/export")
    def archive_export(request: PathRequest) -> dict[str, Any]:
        """Archive export."""
        return cache.export_archive(request.path)

    @app.post("/api/v1/archive/verify-export")
    def archive_verify_export(request: PathRequest) -> dict[str, Any]:
        """Archive verify export."""
        return cache.verify_archive_export(request.path)

    @app.post("/api/v1/archive/restore")
    def archive_restore(request: PathRequest) -> dict[str, Any]:
        """Archive restore."""
        return cache.restore_archive(request.path, dry_run=request.dry_run)

    _register_retention_imap_routes(app, cache)

    _route_refs = (
        archive_status,
        archive_refresh,
        archive_complete,
        archive_verify_objects,
        archive_export,
        archive_verify_export,
        archive_restore,
    )
    _ = _route_refs


def _register_retention_imap_routes(app: FastAPI, cache: PgCache) -> None:

    @app.get("/api/v1/retention/policies")
    def retention_policies() -> list[dict[str, Any]]:
        """Retention policies."""
        return cache.retention_policies()

    @app.get("/api/v1/messages/{message_id}/retention")
    def retention_preview(message_id: str) -> dict[str, Any]:
        """Retention preview."""
        return cache.retention_preview(message_id)

    @app.post("/api/v1/messages/{message_id}/retention")
    def retention_apply(message_id: str, request: RetentionRequest) -> dict[str, Any]:
        """Retention apply."""
        return cache.apply_retention_policy(message_id, source=request.source, dry_run=request.dry_run)

    @app.get("/api/v1/imap/status")
    def imap_status() -> dict[str, Any]:
        """Imap status."""
        return cache.imap_status()

    @app.post("/api/v1/imap/refresh")
    def imap_refresh() -> dict[str, int]:
        """Imap refresh."""
        return cache.refresh_imap_mailboxes()

    _route_refs = (
        retention_policies,
        retention_preview,
        retention_apply,
        imap_status,
        imap_refresh,
    )
    _ = _route_refs


def _register_maintenance_routes(app: FastAPI, cache: PgCache, scheduler: MaintenanceScheduler) -> None:
    @app.get("/api/v1/sync/status")
    def sync_status() -> dict[str, Any]:
        """Sync status."""
        return cache.sync_status()

    @app.get("/api/v1/ingest/issues")
    def ingest_issues(limit: int = 50) -> list[dict[str, Any]]:
        """Ingest issues."""
        return cache.list_ingest_issues(limit=limit)

    @app.get("/api/v1/maintenance/status")
    def maintenance_status() -> dict[str, Any]:
        """Maintenance status."""
        return {**cache.maintenance_status(), "timed_tasks": scheduler.status()}

    @app.get("/api/v1/maintenance/timed")
    def maintenance_timed_status() -> dict[str, Any]:
        """Maintenance timed status."""
        return scheduler.status()

    @app.post("/api/v1/maintenance/timed/{task_name}/run")
    async def maintenance_timed_run(task_name: str) -> dict[str, Any]:
        """Maintenance timed run."""
        try:
            return await scheduler.run_now(task_name)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Unknown timed maintenance task") from exc

    @app.get("/api/v1/maintenance/storage-diagnostics")
    def maintenance_storage_diagnostics() -> dict[str, Any]:
        """Maintenance storage diagnostics."""
        return cache.storage_diagnostics()

    @app.post("/api/v1/maintenance/prune-orphans")
    def maintenance_prune_orphans(request: PruneRequest) -> dict[str, Any]:
        """Maintenance prune orphans."""
        return cache.prune_orphan_content_objects(dry_run=request.dry_run)

    @app.post("/api/v1/maintenance/analyze")
    def maintenance_analyze() -> dict[str, Any]:
        """Maintenance analyze."""
        return cache.analyze_storage_tables()

    _register_maintenance_refresh_routes(app, cache)

    _route_refs = (
        sync_status,
        ingest_issues,
        maintenance_status,
        maintenance_timed_status,
        maintenance_timed_run,
        maintenance_storage_diagnostics,
        maintenance_prune_orphans,
        maintenance_analyze,
    )
    _ = _route_refs


def _register_maintenance_refresh_routes(app: FastAPI, cache: PgCache) -> None:

    @app.post("/api/v1/maintenance/refresh-message-search")
    def maintenance_refresh_message_search(limit: int = DEFAULT_INT) -> dict[str, Any]:
        """Maintenance refresh message search."""
        return cache.refresh_message_search_columns(limit=limit)

    @app.post("/api/v1/maintenance/refresh-graph-profiles")
    def maintenance_refresh_graph_profiles() -> dict[str, Any]:
        """Maintenance refresh graph profiles."""
        return cache.refresh_graph_node_profiles()

    @app.post("/api/v1/maintenance/refresh-content-refs")
    def maintenance_refresh_content_refs() -> dict[str, Any]:
        """Maintenance refresh content refs."""
        return cache.refresh_content_object_refs()

    @app.post("/api/v1/maintenance/refresh-summaries")
    def maintenance_refresh_summaries() -> dict[str, int]:
        """Maintenance refresh summaries."""
        return cache.refresh_summary_items()

    @app.post("/api/v1/maintenance/refresh-graph-edges")
    def maintenance_refresh_graph_edges() -> dict[str, int]:
        """Maintenance refresh graph edges."""
        return cache.refresh_graph_edge_stats()

    @app.post("/api/v1/maintenance/refresh-timelines")
    def maintenance_refresh_timelines() -> dict[str, str]:
        """Maintenance refresh timelines."""
        return cache.refresh_timeline_views()

    @app.post("/api/v1/maintenance/refresh-summary-views")
    def maintenance_refresh_summary_views() -> dict[str, str]:
        """Maintenance refresh summary views."""
        return cache.refresh_materialized_summary_views()

    _route_refs = (
        maintenance_refresh_message_search,
        maintenance_refresh_graph_profiles,
        maintenance_refresh_content_refs,
        maintenance_refresh_summaries,
        maintenance_refresh_graph_edges,
        maintenance_refresh_timelines,
        maintenance_refresh_summary_views,
    )
    _ = _route_refs


def _register_summary_routes(app: FastAPI, cache: PgCache) -> None:
    @app.get("/api/v1/summaries")
    def summaries(scope_kind: str = DEFAULT_STR, limit: int = 50) -> list[dict[str, Any]]:
        """Summaries."""
        return cache.list_summary_items(scope_kind=scope_kind, limit=limit)

    @app.get("/api/v1/timeline/daily")
    def timeline_daily(limit: int = 90) -> list[dict[str, Any]]:
        """Timeline daily."""
        return cache.timeline_daily(limit=limit)

    @app.get("/api/v1/entities/emerging")
    def emerging_entities(limit: int = 50, kind: str = DEFAULT_STR, visibility: str = "user") -> list[dict[str, Any]]:
        """Emerging entities."""
        return cache.emerging_entities(limit=limit, kind=kind, visibility=visibility)

    _route_refs = (
        summaries,
        timeline_daily,
        emerging_entities,
    )
    _ = _route_refs


def _register_sync_routes(app: FastAPI, cache: PgCache, sync: SyncService) -> None:
    @app.post("/api/v1/sync/run")
    def sync_run(limit_per_rule: int = 100) -> dict[str, Any]:
        """Sync run."""
        try:
            return sync.sync_priority(limit_per_rule=limit_per_rule)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/sync/history")
    def sync_history(limit: int = 500) -> dict[str, Any]:
        """Sync history."""
        try:
            return sync.sync_history(limit=limit)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/sync/backfill")
    def sync_backfill(batch_size: int = 50, *, validate: bool = False, max_empty_windows: int = 120) -> dict[str, Any]:
        """Sync backfill."""
        try:
            return sync.backfill_batch(batch_size=batch_size, validate=validate, max_empty_windows=max_empty_windows)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/v1/labels")
    def labels() -> list[dict[str, Any]]:
        """Labels."""
        return cache.list_labels()

    _route_refs = (
        sync_run,
        sync_history,
        sync_backfill,
        labels,
    )
    _ = _route_refs


def _register_message_routes(app: FastAPI, cache: PgCache, sync: SyncService) -> None:
    @app.get("/api/v1/threads")
    def threads(limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        """Threads."""
        return cache.list_threads(limit=limit, offset=offset)

    @app.get("/api/v1/threads/{thread_id}")
    def thread(thread_id: str) -> dict[str, Any]:
        """Thread."""
        value = cache.get_thread(thread_id)
        if not value:
            raise HTTPException(status_code=404, detail="Thread not found")
        return value

    @app.get("/api/v1/messages/{message_id}")
    def message(message_id: str) -> dict[str, Any]:
        """Message."""
        value = cache.get_message(message_id)
        if not value:
            raise HTTPException(status_code=404, detail="Message not found")
        return value

    @app.get("/api/v1/messages/{message_id}/raw")
    def raw_message(message_id: str) -> dict[str, Any]:
        """Raw message."""
        value = cache.get_message(message_id)
        if not value:
            raise HTTPException(status_code=404, detail="Message not found")
        return cast(dict[str, Any], value["raw"])

    _register_raw_message_routes(app, cache, sync)

    _route_refs = (
        threads,
        thread,
        message,
        raw_message,
    )
    _ = _route_refs


def _register_raw_message_routes(app: FastAPI, cache: PgCache, sync: SyncService) -> None:

    @app.get("/api/v1/messages/{message_id}/raw/rfc822")
    def raw_rfc822_message(message_id: str) -> Response:
        """Raw rfc822 message."""
        try:
            raw = sync.hydrate_raw_rfc822(message_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Message raw source not found") from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return Response(raw, media_type="message/rfc822")

    @app.get("/api/v1/messages/{message_id}/markdown")
    def markdown_message(message_id: str) -> Response:
        """Markdown message."""
        value = cache.get_message(message_id)
        if not value:
            raise HTTPException(status_code=404, detail="Message not found")
        return Response(value.get("markdown") or "", media_type="text/markdown")

    _route_refs = (
        raw_rfc822_message,
        markdown_message,
    )
    _ = _route_refs


def _register_attachment_routes(app: FastAPI, cache: PgCache, attachments: CasAttachmentStore) -> None:
    @app.get("/api/v1/attachments/{sha1}", response_model=None)
    def attachment(sha1: str) -> Response | FileResponse:
        """Attachment."""
        try:
            if hasattr(attachments, "get_bytes"):
                metadata = attachments.read_metadata(sha1)
                return Response(
                    attachments.get_bytes(sha1),
                    media_type=metadata.get("media_type")
                    or metadata.get("source", {}).get("gmail", {}).get("mime_type")
                    or "application/octet-stream",
                )
            path = attachments.get(sha1)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Attachment not found") from exc
        return FileResponse(path)

    @app.get("/api/v1/attachments/{sha1}/metadata")
    def attachment_metadata(sha1: str) -> dict[str, Any]:
        """Attachment metadata."""
        return attachments.read_metadata(sha1)

    @app.get("/api/v1/messages/{message_id}/attachments")
    def message_attachments(message_id: str, *, include_metadata: bool = True) -> list[dict[str, Any]]:
        """Message attachments."""
        return cache.attachment_text_for_message(message_id, include_metadata=include_metadata)

    _route_refs = (
        attachment,
        attachment_metadata,
        message_attachments,
    )
    _ = _route_refs


def _register_search_routes(app: FastAPI, cache: PgCache, sync: SyncService, semantic: PgSemanticIndex) -> None:
    @app.get("/api/v1/search/analysis-status")
    def search_analysis_status(message_ids: str) -> dict[str, Any]:
        """Search analysis status."""
        ids = [message_id.strip() for message_id in message_ids.split(",") if message_id.strip()]
        status = sync.analysis_status_for_messages(ids)
        status["messages"] = [message for message_id in ids if (message := cache.get_message(message_id))]
        return status

    @app.get("/api/v1/search/{search_id}/status")
    def search_status(search_id: str) -> dict[str, Any]:
        """Search status."""
        status = sync.search_status(search_id)
        if not status:
            raise HTTPException(status_code=404, detail="Search not found.")
        return status

    @app.post("/api/v1/search/text")
    def search_text(request: SearchRequest) -> list[dict[str, Any]]:
        """Search text."""
        messages = sync.search(
            request.query,
            limit=request.limit,
            after=request.after,
            before=request.before,
            include_categories=request.include_categories,
            exclude_categories=request.exclude_categories,
        )["messages"]
        if request.compact:
            return cache.compact_messages(messages, body_chars=request.body_chars)
        return cast(list[dict[str, Any]], messages)

    @app.post("/api/v1/search/attachments/text")
    def search_attachments_text(request: SearchRequest) -> list[dict[str, Any]]:
        """Search attachments text."""
        return cache.attachment_text_search(request.query, limit=request.limit)

    @app.post("/api/v1/search/semantic")
    def search_semantic(request: SearchRequest) -> list[dict[str, Any]]:
        """Search semantic."""
        try:
            return semantic.search(request.query, limit=request.limit, source_kind=request.source_kind)
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Semantic index unavailable: {exc}") from exc

    @app.post("/api/v1/search/hybrid")
    def search_hybrid(request: SearchRequest) -> dict[str, Any]:
        """Search hybrid."""
        result = sync.hybrid_search(
            request.query,
            limit=request.limit,
            after=request.after,
            before=request.before,
            include_categories=request.include_categories,
            exclude_categories=request.exclude_categories,
        )
        if request.compact:
            result["messages"] = cache.compact_messages(result["messages"], body_chars=request.body_chars)
        return result

    @app.post("/api/v1/search/graph")
    def search_graph(request: SearchRequest) -> list[dict[str, Any]]:
        """Search graph."""
        return cache.graph_search(
            request.query,
            limit=request.limit,
            include_noise=request.include_noise,
            kind=request.graph_kind,
            namespace=request.graph_namespace,
            visibility=request.graph_visibility,
        )

    _route_refs = (
        search_analysis_status,
        search_status,
        search_text,
        search_attachments_text,
        search_semantic,
        search_hybrid,
        search_graph,
    )
    _ = _route_refs


def _register_graph_routes(app: FastAPI, cache: PgCache) -> None:
    @app.post("/api/v1/graph/neighborhood")
    def graph_neighborhood(request: GraphNeighborhoodRequest) -> dict[str, Any]:
        """Graph neighborhood."""
        return cache.graph_neighbors(request.node, depth=request.depth, limit=request.limit)

    @app.get("/api/v1/graph/projection")
    def graph_projection(
        limit: int = 25, prefix: str = DEFAULT_STR, visibility: str = "user", kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR
    ) -> dict[str, Any]:
        """Graph projection."""
        return cache.graph_projection(limit=limit, prefix=prefix, visibility=visibility, kind=kind, namespace=namespace)

    @app.post("/api/v1/graph/path")
    def graph_path(request: GraphPathRequest) -> dict[str, Any]:
        """Graph path."""
        return cache.graph_path(request.source, request.target, max_depth=request.max_depth)

    @app.post("/api/v1/graph/weighted-path")
    def graph_weighted_path(request: GraphPathRequest) -> dict[str, Any]:
        """Graph weighted path."""
        return cache.graph_weighted_path(request.source, request.target, max_depth=request.max_depth)

    @app.get("/api/v1/graph/rank")
    def graph_rank(
        limit: int = 25,
        kind: str = DEFAULT_STR,
        predicate: str = DEFAULT_STR,
        namespace: str = DEFAULT_STR,
        visibility: str = "user",
        *,
        include_noise: bool = False,
    ) -> list[dict[str, Any]]:
        """Graph rank."""
        return cache.graph_ranked_nodes(
            limit=limit, kind=kind, predicate=predicate, namespace=namespace, visibility=visibility, include_noise=include_noise
        )

    @app.get("/api/v1/graph/centrality")
    def graph_centrality(
        metric: str = "pagerank", limit: int = 25, kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR, visibility: str = "user"
    ) -> list[dict[str, Any]]:
        """Graph centrality."""
        try:
            return cache.graph_centrality(metric=metric, limit=limit, kind=kind, namespace=namespace, visibility=visibility)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    _register_graph_discovery_routes(app, cache)

    _route_refs = (
        graph_neighborhood,
        graph_projection,
        graph_path,
        graph_weighted_path,
        graph_rank,
        graph_centrality,
    )
    _ = _route_refs


def _register_graph_discovery_routes(app: FastAPI, cache: PgCache) -> None:
    @app.get("/api/v1/graph/components")
    def graph_components(mode: str = "weak", limit: int = 10, min_size: int = 2, visibility: str = "user") -> list[dict[str, Any]]:
        """Graph components."""
        return cache.graph_components(mode=mode, limit=limit, min_size=min_size, visibility=visibility)

    @app.get("/api/v1/graph/related-nodes")
    def graph_related_nodes(
        node: str,
        limit: int = 25,
        max_weight: float = DEFAULT_FLOAT,
        kind: str = DEFAULT_STR,
        namespace: str = DEFAULT_STR,
        visibility: str = "user",
    ) -> list[dict[str, Any]]:
        """Graph related nodes."""
        return cache.graph_related_nodes(
            node=node, limit=limit, max_weight=max_weight, kind=kind, namespace=namespace, visibility=visibility
        )

    @app.get("/api/v1/graph/bridges")
    def graph_bridges(
        limit: int = 25, kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR, visibility: str = "user"
    ) -> list[dict[str, Any]]:
        """Graph bridges."""
        return cache.graph_bridges(limit=limit, kind=kind, namespace=namespace, visibility=visibility)

    @app.get("/api/v1/graph/cycles")
    def graph_cycles(limit: int = 25, max_cycle_len: int = 8, visibility: str = "user") -> list[dict[str, Any]]:
        """Graph cycles."""
        return cache.graph_cycles(limit=limit, max_cycle_len=max_cycle_len, visibility=visibility)

    @app.get("/api/v1/graph/projects")
    def graph_projects(limit: int = 25) -> list[dict[str, Any]]:
        """Graph projects."""
        return cache.graph_projects(limit=limit)

    @app.get("/api/v1/graph/ontologies")
    def graph_ontologies() -> dict[str, Any]:
        """Graph ontologies."""
        return cache.ontology_profile()

    @app.get("/api/v1/graph/age/status")
    def graph_age_status() -> dict[str, Any]:
        """Graph age status."""
        return cache.age_status()

    _register_graph_message_routes(app, cache)

    _route_refs = (
        graph_components,
        graph_related_nodes,
        graph_bridges,
        graph_cycles,
        graph_projects,
        graph_ontologies,
        graph_age_status,
    )
    _ = _route_refs


def _register_graph_message_routes(app: FastAPI, cache: PgCache) -> None:

    @app.post("/api/v1/graph/age/cypher")
    def graph_age_cypher(request: GraphCypherRequest) -> list[dict[str, Any]]:
        """Graph age cypher."""
        try:
            return cache.age_cypher(request.query, columns=request.columns, limit=request.limit)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/graph/messages/{message_id}/related")
    def graph_related_messages(message_id: str, limit: int = 20) -> list[dict[str, Any]]:
        """Graph related messages."""
        return cache.graph_related_messages(message_id, limit=limit)

    @app.get("/api/v1/graph/messages/{message_id}/recommendations")
    def graph_recommended_messages(message_id: str, limit: int = 20) -> list[dict[str, Any]]:
        """Graph recommended messages."""
        return cache.graph_recommended_messages(message_id, limit=limit)

    @app.get("/api/v1/graph/messages/{message_id}/nodes")
    def graph_message_nodes(
        message_id: str, visibility: str = "user", kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR
    ) -> dict[str, Any]:
        """Graph message nodes."""
        return cache.graph_nodes_for_message(message_id, visibility=visibility, kind=kind, namespace=namespace)

    @app.get("/api/v1/graph/top")
    def graph_top(
        prefix: str = DEFAULT_STR,
        limit: int = 25,
        *,
        include_noise: bool = False,
        kind: str = DEFAULT_STR,
        namespace: str = DEFAULT_STR,
        visibility: str = "user",
    ) -> list[dict[str, Any]]:
        """Graph top."""
        return cache.graph_top_nodes(
            prefix=prefix, limit=limit, include_noise=include_noise, kind=kind, namespace=namespace, visibility=visibility
        )

    _route_refs = (
        graph_age_cypher,
        graph_related_messages,
        graph_recommended_messages,
        graph_message_nodes,
        graph_top,
    )
    _ = _route_refs


def _register_action_people_routes(app: FastAPI, cache: PgCache, sync: SyncService) -> None:
    @app.post("/api/v1/messages/{message_id}/labels")
    def apply_label(message_id: str, request: LabelRequest) -> dict[str, Any]:
        """Apply label."""
        label_id = request.label_id
        result = apply_message_label(sync, message_id, label_id)
        return dict(result)

    @app.delete("/api/v1/messages/{message_id}/labels/{label_id}")
    def remove_label(message_id: str, label_id: str) -> dict[str, Any]:
        """Remove label."""
        result = remove_message_label(sync, message_id, label_id)
        return dict(result)

    @app.post("/api/v1/messages/{message_id}/archive")
    def archive(message_id: str) -> dict[str, Any]:
        """Archive."""
        result = archive_message(sync, message_id)
        return dict(result)

    @app.post("/api/v1/messages/{message_id}/read-state")
    def read_state(message_id: str, request: ReadStateRequest) -> dict[str, Any]:
        """Read state."""
        read = request.read
        result = set_message_read_state(sync, message_id, read=read)
        return dict(result)

    @app.post("/api/v1/messages/{message_id}/star-state")
    def star_state(message_id: str, request: StarStateRequest) -> dict[str, Any]:
        """Star state."""
        starred = request.starred
        result = set_message_star_state(sync, message_id, starred=starred)
        return dict(result)

    @app.get("/api/v1/contacts")
    def contacts(limit: int = 100, min_messages: int = 1) -> list[dict[str, Any]]:
        """Contacts."""
        return cache.contacts(limit=limit, min_messages=min_messages)

    @app.get("/api/v1/people")
    def people(limit: int = 100, min_messages: int = 1) -> list[dict[str, Any]]:
        """People."""
        return cache.people(limit=limit, min_messages=min_messages)

    @app.post("/api/v1/people/aliases")
    def people_alias(request: PeopleAliasRequest) -> dict[str, Any]:
        """People alias."""
        cache.upsert_people_alias(request.address, request.person_key, display_name=request.display_name, note=request.note)
        return {"aliases": list(cache.people_aliases().values())}

    _route_refs = (
        apply_label,
        remove_label,
        archive,
        read_state,
        star_state,
        contacts,
        people,
        people_alias,
    )
    _ = _route_refs
