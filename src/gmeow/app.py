# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide app functionality for Gmeow."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from .config import GmeowConfig
from .gmail import GoogleGmailClient, UserOAuthGmailClient
from .maintenance import MaintenanceScheduler
from .mcp_server import build_mcp_app
from .object_store import CasAttachmentStore, ObjectStore
from .pg_cache import PgCache
from .resilience import degraded_status, readiness, startup_self_check
from .semantic_pg import PgSemanticIndex
from .sync import SyncService
from .text_index import TantivyMessageIndex


class SearchRequest(BaseModel):
    """Represent SearchRequest data and behavior."""

    query: str
    limit: int = 20
    after: str | None = None
    before: str | None = None
    include_categories: list[str] | None = None
    exclude_categories: list[str] | None = None
    compact: bool = True
    body_chars: int = 500
    graph_kind: str | None = None
    graph_namespace: str | None = None
    graph_visibility: str = "user"
    include_noise: bool = False
    source_kind: str | None = None


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
    display_name: str | None = None
    note: str | None = None


class PathRequest(BaseModel):
    """Represent PathRequest data and behavior."""

    path: str
    dry_run: bool = True


class RetentionRequest(BaseModel):
    """Represent RetentionRequest data and behavior."""

    source: str = "operator"
    dry_run: bool = True


def build_services(config: GmeowConfig) -> tuple[PgCache, CasAttachmentStore, PgSemanticIndex, None, SyncService]:
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
    graph = None
    gmail = None
    service_account_info = config.service_account_info()
    user_credentials_info = config.user_credentials_info()
    if (
        config.auth_mode == "service_account"
        and config.subject
        and (service_account_info is not None or config.service_account_file.exists())
    ):
        gmail = GoogleGmailClient(
            config.service_account_file if service_account_info is None else None, config.subject, service_account_info=service_account_info
        )
    elif config.auth_mode == "user_oauth" and (user_credentials_info is not None or config.user_credentials_file.exists()):
        gmail = UserOAuthGmailClient(
            config.user_credentials_file if user_credentials_info is None else None, credentials_info=user_credentials_info
        )
    sync = SyncService(config, cache, attachments, semantic, gmail, graph=graph)
    return cache, attachments, semantic, graph, sync


def list_messages_endpoint(request: Request, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    """Messages."""
    cache: PgCache = request.app.state.cache
    return cache.list_messages(limit=limit, offset=offset)


def create_app(config: GmeowConfig | None = None) -> FastAPI:
    """Create app."""
    config = config or GmeowConfig.load()
    cache, attachments, semantic, graph, sync = build_services(config)
    mcp_app = build_mcp_app(cache=cache, sync=sync, attachments=attachments)
    scheduler = MaintenanceScheduler(config.maintenance, cache, sync, attachments, semantic, graph=graph)
    sync.maintenance_scheduler = scheduler
    startup_status = startup_self_check(config, cache, sync, semantic)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        """Lifespan."""
        scheduler.start()
        try:
            if mcp_app is None:
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

    @app.middleware("http")
    async def loopback_guard(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """Loopback guard."""
        client_host = request.client.host if request.client else ""
        host_header = request.headers.get("host", "").split(":")[0]
        allowed = {"127.0.0.1", "localhost", "::1"}
        test_clients = {"testclient"}
        if client_host not in allowed | test_clients or host_header not in allowed:
            return Response("Gmeow only serves loopback clients by default.\n", status_code=403)
        return await call_next(request)

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        """Health."""
        return {"ok": True, "subject_configured": bool(config.subject), "gmail_configured": sync.gmail is not None}

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

    @app.get("/api/v1/status/resilience")
    def status_resilience() -> dict[str, Any]:
        """Status resilience."""
        return cache.resilience_status()

    @app.get("/api/v1/ops/events")
    def operational_events(limit: int = 100, component: str | None = None, severity: str | None = None) -> list[dict[str, Any]]:
        """Operational events."""
        return cache.operational_events(limit=limit, component=component, severity=severity)

    @app.get("/api/v1/jobs/intelligence")
    def intelligence_jobs(status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Intelligence jobs."""
        return cache.list_intelligence_jobs(status=status, limit=limit)

    @app.get("/api/v1/jobs/dead-letter")
    def dead_letter_jobs(limit: int = 50, *, include_closed: bool = False) -> list[dict[str, Any]]:
        """Dead letter jobs."""
        return cache.list_dead_letter_jobs(limit=limit, include_closed=include_closed)

    @app.post("/api/v1/jobs/dead-letter/retry")
    def retry_dead_letter_jobs(limit: int | None = None) -> dict[str, int]:
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

    @app.get("/api/v1/archive/status")
    def archive_status(message_id: str | None = None, limit: int = 50) -> dict[str, Any]:
        """Archive status."""
        return cache.archive_status(message_id=message_id, limit=limit)

    @app.post("/api/v1/archive/refresh")
    def archive_refresh(limit: int | None = None) -> dict[str, int]:
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
    def archive_verify_objects(limit: int | None = None) -> dict[str, Any]:
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

    @app.post("/api/v1/maintenance/refresh-message-search")
    def maintenance_refresh_message_search(limit: int | None = None) -> dict[str, Any]:
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

    @app.get("/api/v1/summaries")
    def summaries(scope_kind: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Summaries."""
        return cache.list_summary_items(scope_kind=scope_kind, limit=limit)

    @app.get("/api/v1/timeline/daily")
    def timeline_daily(limit: int = 90) -> list[dict[str, Any]]:
        """Timeline daily."""
        return cache.timeline_daily(limit=limit)

    @app.get("/api/v1/entities/emerging")
    def emerging_entities(limit: int = 50, kind: str | None = None, visibility: str = "user") -> list[dict[str, Any]]:
        """Emerging entities."""
        return cache.emerging_entities(limit=limit, kind=kind, visibility=visibility)

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

    @app.get("/api/v1/labels")
    def labels() -> list[dict[str, Any]]:
        """Labels."""
        return cache.list_labels()

    @app.get("/api/v1/threads")
    def threads(limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        """Threads."""
        return cache.list_threads(limit=limit, offset=offset)

    @app.get("/api/v1/threads/{thread_id}")
    def thread(thread_id: str) -> dict[str, Any]:
        """Thread."""
        value = cache.get_thread(thread_id)
        if value is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        return value

    @app.get("/api/v1/messages/{message_id}")
    def message(message_id: str) -> dict[str, Any]:
        """Message."""
        value = cache.get_message(message_id)
        if value is None:
            raise HTTPException(status_code=404, detail="Message not found")
        return value

    @app.get("/api/v1/messages/{message_id}/raw")
    def raw_message(message_id: str) -> dict[str, Any]:
        """Raw message."""
        value = cache.get_message(message_id)
        if value is None:
            raise HTTPException(status_code=404, detail="Message not found")
        return value["raw"]

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
        if value is None:
            raise HTTPException(status_code=404, detail="Message not found")
        return Response(value.get("markdown") or "", media_type="text/markdown")

    @app.get("/api/v1/attachments/{sha1}")
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
        return messages

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

    @app.post("/api/v1/graph/neighborhood")
    def graph_neighborhood(request: GraphNeighborhoodRequest) -> dict[str, Any]:
        """Graph neighborhood."""
        return cache.graph_neighbors(request.node, depth=request.depth, limit=request.limit)

    @app.get("/api/v1/graph/projection")
    def graph_projection(
        limit: int = 25, prefix: str | None = None, visibility: str = "user", kind: str | None = None, namespace: str | None = None
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
        kind: str | None = None,
        predicate: str | None = None,
        namespace: str | None = None,
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
        metric: str = "pagerank", limit: int = 25, kind: str | None = None, namespace: str | None = None, visibility: str = "user"
    ) -> list[dict[str, Any]]:
        """Graph centrality."""
        try:
            return cache.graph_centrality(metric=metric, limit=limit, kind=kind, namespace=namespace, visibility=visibility)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/graph/components")
    def graph_components(mode: str = "weak", limit: int = 10, min_size: int = 2, visibility: str = "user") -> list[dict[str, Any]]:
        """Graph components."""
        return cache.graph_components(mode=mode, limit=limit, min_size=min_size, visibility=visibility)

    @app.get("/api/v1/graph/related-nodes")
    def graph_related_nodes(
        node: str,
        limit: int = 25,
        max_weight: float | None = None,
        kind: str | None = None,
        namespace: str | None = None,
        visibility: str = "user",
    ) -> list[dict[str, Any]]:
        """Graph related nodes."""
        return cache.graph_related_nodes(
            node=node, limit=limit, max_weight=max_weight, kind=kind, namespace=namespace, visibility=visibility
        )

    @app.get("/api/v1/graph/bridges")
    def graph_bridges(
        limit: int = 25, kind: str | None = None, namespace: str | None = None, visibility: str = "user"
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
        message_id: str, visibility: str = "user", kind: str | None = None, namespace: str | None = None
    ) -> dict[str, Any]:
        """Graph message nodes."""
        return cache.graph_nodes_for_message(message_id, visibility=visibility, kind=kind, namespace=namespace)

    @app.get("/api/v1/graph/top")
    def graph_top(
        prefix: str | None = None,
        limit: int = 25,
        *,
        include_noise: bool = False,
        kind: str | None = None,
        namespace: str | None = None,
        visibility: str = "user",
    ) -> list[dict[str, Any]]:
        """Graph top."""
        return cache.graph_top_nodes(
            prefix=prefix, limit=limit, include_noise=include_noise, kind=kind, namespace=namespace, visibility=visibility
        )

    @app.post("/api/v1/messages/{message_id}/labels")
    def apply_label(message_id: str, request: LabelRequest) -> dict[str, Any]:
        """Apply label."""
        return sync.apply_label(message_id, request.label_id)

    @app.delete("/api/v1/messages/{message_id}/labels/{label_id}")
    def remove_label(message_id: str, label_id: str) -> dict[str, Any]:
        """Remove label."""
        return sync.remove_label(message_id, label_id)

    @app.post("/api/v1/messages/{message_id}/archive")
    def archive(message_id: str) -> dict[str, Any]:
        """Archive."""
        return sync.archive(message_id)

    @app.post("/api/v1/messages/{message_id}/read-state")
    def read_state(message_id: str, request: ReadStateRequest) -> dict[str, Any]:
        """Read state."""
        return sync.mark_read(message_id, read=request.read)

    @app.post("/api/v1/messages/{message_id}/star-state")
    def star_state(message_id: str, request: StarStateRequest) -> dict[str, Any]:
        """Star state."""
        return sync.star(message_id, starred=request.starred)

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

    if mcp_app is not None:
        app.router.routes.extend(mcp_app.routes)

    return app


def app_from_config_path(config_path: str | Path = "config.toml") -> FastAPI:
    """App from config path."""
    return create_app(GmeowConfig.load(config_path))
