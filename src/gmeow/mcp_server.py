# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from .categories import CategoryEngine
from .sync import SyncService
from .toon import dumps as toon_dumps


def build_mcp_app(cache: Any, sync: SyncService, attachments: Any) -> Any | None:
    try:
        from mcp.server.fastmcp import FastMCP
    except Exception:
        return None

    mcp = FastMCP("gmeow")

    @mcp.resource("gmail://labels")
    def labels() -> str:
        return json.dumps(cache.list_labels(), indent=2)

    @mcp.resource("gmail://threads/{thread_id}")
    def thread(thread_id: str) -> str:
        return json.dumps(enriched_thread(cache, thread_id), indent=2)

    @mcp.resource("gmail://messages/{message_id}")
    def message(message_id: str) -> str:
        return json.dumps(enriched_message(cache, message_id), indent=2)

    @mcp.resource("gmail://attachments/{sha1}/metadata")
    def attachment_metadata(sha1: str) -> str:
        return json.dumps(attachments.read_metadata(sha1), indent=2)

    @mcp.tool()
    def gmail_text_search(
        query: str,
        limit: int = 20,
        after: str | None = None,
        before: str | None = None,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
        compact: bool = True,
        body_chars: int = 500,
        format: str = "toon",
    ) -> Any:
        result = sync.search(
            query,
            limit=limit,
            after=after,
            before=before,
            include_categories=include_categories,
            exclude_categories=exclude_categories,
        )
        results = result["messages"]
        if compact:
            results = cache.compact_messages(results, body_chars=body_chars)
        result["messages"] = results
        return _format(result, format)

    @mcp.tool()
    def gmail_attachment_text_search(query: str, limit: int = 20, format: str = "toon") -> Any:
        return _format(cache.attachment_text_search(query, limit=limit), format)

    @mcp.tool()
    def gmail_semantic_search(
        query: str,
        limit: int = 10,
        after: str | None = None,
        before: str | None = None,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
        source_kind: str | None = None,
        compact: bool = True,
        format: str = "toon",
    ) -> Any:
        needs_filter = after or before or include_categories is not None or exclude_categories is not None
        results = sync.semantic.search(query, limit=limit * 4 if needs_filter else limit, source_kind=source_kind)
        filtered = []
        for result in results:
            message_id = (result.get("metadata") or {}).get("message_id") or str(result.get("id", "")).split(":", 1)[0]
            message = cache.get_message(message_id)
            if not message:
                continue
            date = message.get("message_date_iso")
            if after and (not date or date < after):
                continue
            if before and (not date or date > before):
                continue
            if not cache.category_allowed(message.get("categories", []), include_categories, exclude_categories):
                continue
            result["message"] = cache.compact_message(message) if compact else message
            filtered.append(result)
        results = filtered[:limit] if needs_filter else results
        return _format(results, format)

    @mcp.tool()
    def gmail_hybrid_search(
        query: str,
        limit: int = 10,
        after: str | None = None,
        before: str | None = None,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
        compact: bool = True,
        body_chars: int = 500,
        format: str = "toon",
    ) -> Any:
        result = sync.hybrid_search(
            query,
            limit=limit,
            after=after,
            before=before,
            include_categories=include_categories,
            exclude_categories=exclude_categories,
        )
        if compact:
            result["messages"] = cache.compact_messages(result["messages"], body_chars=body_chars)
        return _format(result, format)

    @mcp.tool()
    def gmail_graph_search(
        query: str,
        limit: int = 50,
        include_noise: bool = False,
        kind: str | None = None,
        namespace: str | None = None,
        visibility: str = "user",
        format: str = "toon",
    ) -> Any:
        return _format(cache.graph_search(query, limit=limit, include_noise=include_noise, kind=kind, namespace=namespace, visibility=visibility), format)

    @mcp.tool()
    def gmail_graph_neighborhood(node: str, depth: int = 1, limit: int = 100, format: str = "toon") -> Any:
        return _format(cache.graph_neighbors(node, depth=depth, limit=limit), format)

    @mcp.tool()
    def gmail_graph_related_messages(
        message_id: str,
        limit: int = 20,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
        format: str = "toon",
    ) -> Any:
        return _format(
            cache.graph_related_messages(message_id, limit=limit, include_categories=include_categories, exclude_categories=exclude_categories),
            format,
        )

    @mcp.tool()
    def gmail_graph_top_nodes(
        prefix: str | None = None,
        limit: int = 25,
        include_noise: bool = False,
        kind: str | None = None,
        namespace: str | None = None,
        visibility: str = "user",
        format: str = "toon",
    ) -> Any:
        return _format(cache.graph_top_nodes(prefix=prefix, limit=limit, include_noise=include_noise, kind=kind, namespace=namespace, visibility=visibility), format)

    @mcp.tool()
    def gmail_graph_projection(limit: int = 25, prefix: str | None = None, visibility: str = "user", kind: str | None = None, namespace: str | None = None, format: str = "toon") -> Any:
        return _format(cache.graph_projection(limit=limit, prefix=prefix, visibility=visibility, kind=kind, namespace=namespace), format)

    @mcp.tool()
    def gmail_graph_path(source: str, target: str, max_depth: int = 4, format: str = "toon") -> Any:
        return _format(cache.graph_path(source, target, max_depth=max_depth), format)

    @mcp.tool()
    def gmail_graph_weighted_path(source: str, target: str, max_depth: int = 4, format: str = "toon") -> Any:
        return _format(cache.graph_weighted_path(source, target, max_depth=max_depth), format)

    @mcp.tool()
    def gmail_graph_rank(
        limit: int = 25,
        kind: str | None = None,
        predicate: str | None = None,
        namespace: str | None = None,
        visibility: str = "user",
        include_noise: bool = False,
        format: str = "toon",
    ) -> Any:
        return _format(cache.graph_ranked_nodes(limit=limit, kind=kind, predicate=predicate, namespace=namespace, visibility=visibility, include_noise=include_noise), format)

    @mcp.tool()
    def gmail_graph_centrality(metric: str = "pagerank", limit: int = 25, kind: str | None = None, namespace: str | None = None, visibility: str = "user", format: str = "toon") -> Any:
        return _format(cache.graph_centrality(metric=metric, limit=limit, kind=kind, namespace=namespace, visibility=visibility), format)

    @mcp.tool()
    def gmail_graph_components(mode: str = "weak", limit: int = 10, min_size: int = 2, visibility: str = "user", format: str = "toon") -> Any:
        return _format(cache.graph_components(mode=mode, limit=limit, min_size=min_size, visibility=visibility), format)

    @mcp.tool()
    def gmail_graph_related_nodes(
        node: str,
        limit: int = 25,
        max_weight: float | None = None,
        kind: str | None = None,
        namespace: str | None = None,
        visibility: str = "user",
        format: str = "toon",
    ) -> Any:
        return _format(cache.graph_related_nodes(node=node, limit=limit, max_weight=max_weight, kind=kind, namespace=namespace, visibility=visibility), format)

    @mcp.tool()
    def gmail_graph_bridges(limit: int = 25, kind: str | None = None, namespace: str | None = None, visibility: str = "user", format: str = "toon") -> Any:
        return _format(cache.graph_bridges(limit=limit, kind=kind, namespace=namespace, visibility=visibility), format)

    @mcp.tool()
    def gmail_graph_cycles(limit: int = 25, max_cycle_len: int = 8, visibility: str = "user", format: str = "toon") -> Any:
        return _format(cache.graph_cycles(limit=limit, max_cycle_len=max_cycle_len, visibility=visibility), format)

    @mcp.tool()
    def gmail_graph_recommended_messages(
        message_id: str,
        limit: int = 20,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
        format: str = "toon",
    ) -> Any:
        return _format(cache.graph_recommended_messages(message_id, limit=limit, include_categories=include_categories, exclude_categories=exclude_categories), format)

    @mcp.tool()
    def gmail_graph_projects(limit: int = 25, format: str = "toon") -> Any:
        return _format(cache.graph_projects(limit=limit), format)

    @mcp.tool()
    def gmail_graph_ontologies(format: str = "toon") -> Any:
        return _format(cache.ontology_profile(), format)

    @mcp.tool()
    def gmail_graph_age_status(format: str = "toon") -> Any:
        return _format(cache.age_status(), format)

    @mcp.tool()
    def gmail_graph_age_cypher(query: str, columns: str = "value agtype", limit: int = 100, format: str = "toon") -> Any:
        return _format(cache.age_cypher(query, columns=columns, limit=limit), format)

    @mcp.tool()
    def gmail_get_message(message_id: str, format: str = "toon") -> Any:
        return _format(enriched_message(cache, message_id), format)

    @mcp.tool()
    def gmail_get_thread(thread_id: str, compact: bool = True, format: str = "toon") -> Any:
        return _format(enriched_thread(cache, thread_id, compact=compact), format)

    @mcp.tool()
    def gmail_address_messages(
        address: str,
        limit: int = 50,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
        compact: bool = True,
        body_chars: int = 500,
        format: str = "toon",
    ) -> Any:
        results = cache.address_messages(address, limit=limit, include_categories=include_categories, exclude_categories=exclude_categories)
        if compact:
            results = cache.compact_messages(results, body_chars=body_chars)
        return _format(results, format)

    @mcp.tool()
    def gmail_get_attachments(message_id: str, include_metadata: bool = True, format: str = "toon") -> Any:
        return _format(cache.attachment_text_for_message(message_id, include_metadata=include_metadata), format)

    @mcp.tool()
    def gmail_contacts(limit: int = 100, min_messages: int = 1, format: str = "toon") -> Any:
        return _format(cache.contacts(limit=limit, min_messages=min_messages), format)

    @mcp.tool()
    def gmail_people(limit: int = 100, min_messages: int = 1, format: str = "toon") -> Any:
        return _format(cache.people(limit=limit, min_messages=min_messages), format)

    @mcp.tool()
    def gmail_set_people_alias(address: str, person_key: str, display_name: str | None = None, note: str | None = None, format: str = "toon") -> Any:
        cache.upsert_people_alias(address, person_key, display_name=display_name, note=note)
        return _format({"aliases": list(cache.people_aliases().values()), "people": cache.people()}, format)

    @mcp.tool()
    def gmail_status(format: str = "toon") -> Any:
        status = cache.sync_status()
        status["semantic_index"] = {"available": sync.semantic.available()}
        return _format(status, format)

    @mcp.tool()
    def gmail_summaries(scope_kind: str | None = None, limit: int = 50, format: str = "toon") -> Any:
        return _format(cache.list_summary_items(scope_kind=scope_kind, limit=limit), format)

    @mcp.tool()
    def gmail_timeline_daily(limit: int = 90, format: str = "toon") -> Any:
        return _format(cache.timeline_daily(limit=limit), format)

    @mcp.tool()
    def gmail_emerging_entities(limit: int = 50, kind: str | None = None, visibility: str = "user", format: str = "toon") -> Any:
        return _format(cache.emerging_entities(limit=limit, kind=kind, visibility=visibility), format)

    @mcp.tool()
    def gmail_maintenance_status(format: str = "toon") -> Any:
        status = cache.maintenance_status()
        scheduler = getattr(sync, "maintenance_scheduler", None)
        if scheduler is not None:
            status["timed_tasks"] = scheduler.status()
        return _format(status, format)

    @mcp.tool()
    def gmail_resilience_status(format: str = "toon") -> Any:
        return _format(cache.resilience_status(), format)

    @mcp.tool()
    def gmail_operational_events(limit: int = 50, component: str | None = None, severity: str | None = None, format: str = "toon") -> Any:
        return _format(cache.operational_events(limit=limit, component=component, severity=severity), format)

    @mcp.tool()
    def gmail_intelligence_jobs(status: str | None = None, limit: int = 50, format: str = "toon") -> Any:
        return _format(cache.list_intelligence_jobs(status=status, limit=limit), format)

    @mcp.tool()
    def gmail_dead_letter_jobs(limit: int = 50, include_closed: bool = False, format: str = "toon") -> Any:
        return _format(cache.list_dead_letter_jobs(limit=limit, include_closed=include_closed), format)

    @mcp.tool()
    def gmail_retry_dead_letter_jobs(limit: int | None = None, format: str = "toon") -> Any:
        return _format(cache.retry_dead_letter_jobs(limit=limit), format)

    @mcp.tool()
    def gmail_repair_cache(dry_run: bool = True, format: str = "toon") -> Any:
        return _format(cache.repair_cache(dry_run=dry_run), format)

    @mcp.tool()
    def gmail_archive_status(message_id: str | None = None, limit: int = 50, format: str = "toon") -> Any:
        return _format(cache.archive_status(message_id=message_id, limit=limit), format)

    @mcp.tool()
    def gmail_refresh_archive_states(limit: int | None = None, format: str = "toon") -> Any:
        return _format(cache.refresh_archive_states(limit=limit), format)

    @mcp.tool()
    def gmail_complete_archive(limit: int = 25, format: str = "toon") -> Any:
        return _format(sync.complete_archive(limit=limit), format)

    @mcp.tool()
    def gmail_verify_objects(limit: int | None = None, format: str = "toon") -> Any:
        return _format(cache.verify_objects(limit=limit), format)

    @mcp.tool()
    def gmail_retention_policies(format: str = "toon") -> Any:
        return _format(cache.retention_policies(), format)

    @mcp.tool()
    def gmail_retention_preview(message_id: str, format: str = "toon") -> Any:
        return _format(cache.retention_preview(message_id), format)

    @mcp.tool()
    def gmail_apply_retention_policy(message_id: str, source: str = "agent", dry_run: bool = True, format: str = "toon") -> Any:
        return _format(cache.apply_retention_policy(message_id, source=source, dry_run=dry_run), format)

    @mcp.tool()
    def gmail_export_archive(path: str, format: str = "toon") -> Any:
        return _format(cache.export_archive(path), format)

    @mcp.tool()
    def gmail_verify_archive(path: str, format: str = "toon") -> Any:
        return _format(cache.verify_archive_export(path), format)

    @mcp.tool()
    def gmail_restore_archive(path: str, dry_run: bool = True, format: str = "toon") -> Any:
        return _format(cache.restore_archive(path, dry_run=dry_run), format)

    @mcp.tool()
    def gmail_imap_status(format: str = "toon") -> Any:
        return _format(cache.imap_status(), format)

    @mcp.tool()
    def gmail_refresh_imap(format: str = "toon") -> Any:
        return _format(cache.refresh_imap_mailboxes(), format)

    @mcp.tool()
    def gmail_storage_diagnostics(format: str = "toon") -> Any:
        return _format(cache.storage_diagnostics(), format)

    @mcp.tool()
    def gmail_prune_orphan_objects(dry_run: bool = True, format: str = "toon") -> Any:
        return _format(cache.prune_orphan_content_objects(dry_run=dry_run), format)

    @mcp.tool()
    def gmail_categories(format: str = "toon") -> Any:
        return _format({"categories": cache.list_categories(), "rules": cache.list_category_rules(), "default_excluded": sorted(cache.default_excluded_categories())}, format)

    @mcp.tool()
    def gmail_category_stats(after: str | None = None, before: str | None = None, format: str = "toon") -> Any:
        return _format(cache.category_stats(after=after, before=before), format)

    @mcp.tool()
    def gmail_seed_categories(format: str = "toon") -> Any:
        return _format(CategoryEngine(cache).seed_initial_categories(), format)

    @mcp.tool()
    def gmail_discover_categories(since_hours: int = 48, limit: int | None = None, format: str = "toon") -> Any:
        return _format(CategoryEngine(cache).discover(since_hours=since_hours, limit=limit), format)

    @mcp.tool()
    def gmail_recategorize(since_hours: int | None = None, limit: int | None = None, format: str = "toon") -> Any:
        return _format(CategoryEngine(cache).recategorize(since_hours=since_hours, limit=limit), format)

    @mcp.tool()
    def gmail_apply_category(message_id: str, category: str, action: str = "include", reason: str | None = None, format: str = "toon") -> Any:
        cache.apply_category(message_id, category, action=action, reason=reason)
        return _format(cache.get_message(message_id), format)

    @mcp.tool()
    def gmail_set_category_rule(category: str, rule: dict[str, Any], enabled: bool = True, format: str = "toon") -> Any:
        rule_id = cache.upsert_category_rule(category, rule, enabled=enabled)
        return _format({"id": rule_id, "category": category, "rule": rule, "enabled": enabled}, format)

    @mcp.tool()
    def gmail_enable_learned_category(learned_id: str, category: str | None = None, format: str = "toon") -> Any:
        return _format(CategoryEngine(cache).enable_learned_category(learned_id, category=category), format)

    @mcp.tool()
    def gmail_help(format: str = "toon") -> Any:
        help_data = {
            "text_search": {
                "syntax": "Plain terms plus Gmail-style from:, to:, subject:, label:, after:, before:. Date params are also accepted.",
                "examples": ["from:owner@example.com invoice", 'subject:"Apollo Invoice" after:2026-05-01', "to:user@example.com attachment"],
                "categories": "camera_alert is excluded by default. Pass include_categories:['camera_alert'] to search those operational alerts.",
                "archive": "Unbounded searches use live Gmail archive search, hydrate/cache returned messages, and enqueue intelligence processing. Add after:/before: or after/before params for time-bound cache-only searches.",
            },
            "categories": ["primary", "camera_alert", "news_alert", "admin_alert", "promotion", "update"],
            "category_tools": [
                "gmail_categories",
                "gmail_category_stats",
                "gmail_seed_categories",
                "gmail_discover_categories",
                "gmail_recategorize",
                "gmail_apply_category",
                "gmail_set_category_rule",
                "gmail_enable_learned_category",
            ],
            "thread": "gmail_get_thread defaults to compact=true. Use compact=false for full cached message bodies.",
            "graph": {
                "node_profile": "Graph results include profile.kind, profile.namespace, profile.visibility, profile.role, and profile.noise_reason.",
                "visibility": ["user", "structural", "noise", "all"],
                "kinds": ["addresses", "entities", "orgs", "handles", "projects", "labels", "attachments", "urls", "literals", "ontology_classes"],
                "namespaces": ["gmeow", "foaf", "sioc", "schemaorg", "skos", "doap", "prov_o", "rdf", "rdfs"],
                "rustworkx_tools": [
                    "gmail_graph_centrality",
                    "gmail_graph_components",
                    "gmail_graph_related_nodes",
                    "gmail_graph_bridges",
                    "gmail_graph_cycles",
                    "gmail_graph_recommended_messages",
                ],
                "defaults": "Discovery tools default to visibility='user'. Use visibility='structural' for ontology/classes or include_noise=true to retain noise while still honoring explicit kind/namespace filters.",
            },
            "semantic_search": {
                "source_kind": "Optional source_kind filter, currently useful values are 'message' and 'attachment'. These align with scoped pgvector HNSW indexes.",
            },
            "hybrid_search": {
                "tool": "gmail_hybrid_search",
                "ranking": "Combines lexical search, message semantic search, graph hits, default category visibility, and recency into one ranked message list.",
            },
            "resilience": {
                "tools": ["gmail_resilience_status", "gmail_operational_events", "gmail_intelligence_jobs", "gmail_dead_letter_jobs", "gmail_retry_dead_letter_jobs", "gmail_repair_cache"],
                "health": "REST exposes /api/v1/health/live, /api/v1/health/ready, /api/v1/health/degraded, and /api/v1/status/resilience.",
            },
            "archive": {
                "completeness": "Archive-complete messages require both Gmail full JSON and canonical raw RFC822 bytes.",
                "retention": "Deletion handling defaults to tombstone retention, with configured purge labels/categories moving messages to purge_pending.",
                "tools": ["gmail_archive_status", "gmail_complete_archive", "gmail_verify_objects", "gmail_retention_preview", "gmail_apply_retention_policy", "gmail_export_archive", "gmail_verify_archive", "gmail_restore_archive", "gmail_imap_status"],
            },
            "formats": ["json", "toon"],
            "sync_priority": {
                "limit_per_rule": "Maximum Gmail search results to hydrate for each configured priority rule.",
                "rules": [asdict(rule) for rule in sync.config.priority_rules],
            },
        }
        return _format(help_data, format)

    @mcp.tool()
    def gmail_sync_priority(limit_per_rule: int = 100) -> dict[str, Any]:
        return sync.sync_priority(limit_per_rule=limit_per_rule)

    @mcp.tool()
    def gmail_sync_history(limit: int = 500) -> dict[str, Any]:
        return sync.sync_history(limit=limit)

    @mcp.tool()
    def gmail_apply_label(message_id: str, label_id: str) -> dict[str, Any]:
        return sync.apply_label(message_id, label_id)

    @mcp.tool()
    def gmail_archive(message_id: str) -> dict[str, Any]:
        return sync.archive(message_id)

    @mcp.tool()
    def gmail_mark_read(message_id: str, read: bool = True) -> dict[str, Any]:
        return sync.mark_read(message_id, read=read)

    @mcp.tool()
    def gmail_star(message_id: str, starred: bool = True) -> dict[str, Any]:
        return sync.star(message_id, starred=starred)

    @mcp.tool()
    def gmail_raw_rfc822(message_id: str) -> str:
        return sync.hydrate_raw_rfc822(message_id).decode("utf-8", errors="replace")

    if hasattr(mcp, "streamable_http_app"):
        return mcp.streamable_http_app()
    if hasattr(mcp, "sse_app"):
        return mcp.sse_app()
    return None


def enriched_message(cache: Cache, message_id: str) -> dict[str, Any] | None:
    message = cache.get_message(message_id)
    if message is None:
        return None
    message["graph"] = cache.graph_triples_for_message(message_id)
    message["graph_nodes"] = cache.graph_nodes_for_message(message_id)
    message["attachment_sidecars"] = cache.attachments_for_message(message_id)
    return message


def enriched_thread(cache: Cache, thread_id: str, compact: bool = False) -> dict[str, Any] | None:
    thread = cache.get_thread(thread_id, compact=compact)
    if thread is None:
        return None
    if compact:
        return thread
    thread["messages"] = [enriched_message(cache, message["id"]) or message for message in thread["messages"]]
    return thread


def _format(value: Any, format: str) -> Any:
    if format.lower() == "toon":
        return toon_dumps(value)
    return value
