# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide the command line interface for Gmeow.

The module maps operator commands to the same application services used by the HTTP server. It
includes sync, backfill, archive, maintenance, and worker entry points for local administration.
"""

import argparse
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, cast

import uvicorn
from google.auth.exceptions import RefreshError

from .app import create_app
from .categories import CategoryEngine
from .config import GmeowConfig
from .imap_server import run_imap_server
from .intelligence import IntelligenceWorker
from .provision import build_gcloud_provision_plan

CommandHandler = Callable[[GmeowConfig, argparse.Namespace], None]


class _Subcommands(Protocol):
    """Parser collection returned by argparse subparsers."""

    def add_parser(self, name: str) -> argparse.ArgumentParser:
        """Create a named subcommand parser."""
        ...


class _AppState(Protocol):
    """Application state used by CLI commands."""

    cache: Any
    sync: Any
    attachments: Any
    semantic: Any
    graph: Any


class _AppLike(Protocol):
    """Application object used by CLI commands."""

    state: _AppState


def main() -> None:
    """Run the Gmeow command-line interface."""
    parser = _build_parser()
    args = parser.parse_args()
    config = GmeowConfig.load(args.config)

    handler = COMMAND_HANDLERS.get(args.command)
    if handler is None:
        parser.print_help()
        return
    handler(config, args)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gmeow")
    parser.add_argument("--config", default="config.toml")
    sub = cast(_Subcommands, parser.add_subparsers(dest="command"))
    _add_server_commands(sub)
    _add_sync_commands(sub)
    _add_archive_commands(sub)
    _add_maintenance_commands(sub)
    _add_category_commands(sub)
    _add_people_provision_commands(sub)
    return parser


def _add_server_commands(sub: _Subcommands) -> None:
    serve = sub.add_parser("serve")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve_imap = sub.add_parser("serve-imap")
    serve_imap.add_argument("--host")
    serve_imap.add_argument("--port", type=int)


def _add_sync_commands(sub: _Subcommands) -> None:
    sub.add_parser("sync")
    history_sync = sub.add_parser("sync-history")
    history_sync.add_argument("--limit", type=int, default=500)
    backfill = sub.add_parser("backfill")
    backfill.add_argument("--batch-size", type=int, default=50)
    backfill.add_argument("--validate", action="store_true")
    backfill.add_argument("--max-empty-windows", type=int, default=120)
    sub.add_parser("status")
    sub.add_parser("maintenance-status")
    sub.add_parser("resilience-status")
    events = sub.add_parser("ops-events")
    events.add_argument("--limit", type=int, default=50)
    events.add_argument("--component")
    events.add_argument("--severity")
    jobs = sub.add_parser("jobs")
    jobs.add_argument("--status")
    jobs.add_argument("--limit", type=int, default=50)
    dead = sub.add_parser("dead-letter")
    dead.add_argument("--limit", type=int, default=50)
    dead.add_argument("--include-closed", action="store_true")
    retry_dead = sub.add_parser("retry-dead")
    retry_dead.add_argument("--limit", type=int)
    repair = sub.add_parser("repair-cache")
    repair.add_argument("--apply", action="store_true")


def _add_archive_commands(sub: _Subcommands) -> None:
    archive_status = sub.add_parser("archive-status")
    archive_status.add_argument("--message-id")
    archive_status.add_argument("--limit", type=int, default=50)
    archive_refresh = sub.add_parser("refresh-archive-states")
    archive_refresh.add_argument("--limit", type=int)
    complete_archive = sub.add_parser("complete-archive")
    complete_archive.add_argument("--limit", type=int, default=25)
    verify_objects = sub.add_parser("verify-objects")
    verify_objects.add_argument("--limit", type=int)
    export_archive = sub.add_parser("export-archive")
    export_archive.add_argument("target")
    verify_archive = sub.add_parser("verify-archive")
    verify_archive.add_argument("source")
    restore_archive = sub.add_parser("restore-archive")
    restore_archive.add_argument("source")
    restore_archive.add_argument("--apply", action="store_true")
    retention = sub.add_parser("apply-retention-policy")
    retention.add_argument("message_id")
    retention.add_argument("--source", default="operator")
    retention.add_argument("--apply", action="store_true")
    sub.add_parser("retention-policies")
    sub.add_parser("imap-status")
    sub.add_parser("refresh-imap")
    sub.add_parser("storage-diagnostics")
    prune = sub.add_parser("prune-orphan-objects")
    prune.add_argument("--apply", action="store_true")


def _add_maintenance_commands(sub: _Subcommands) -> None:
    sub.add_parser("analyze-storage")
    sub.add_parser("doctor")
    sub.add_parser("refresh-attachment-sidecars")
    refresh_search = sub.add_parser("refresh-message-search")
    refresh_search.add_argument("--limit", type=int)
    sub.add_parser("refresh-graph-profiles")
    sub.add_parser("refresh-content-refs")
    sub.add_parser("refresh-summaries")
    sub.add_parser("refresh-graph-edges")
    sub.add_parser("refresh-timelines")
    sub.add_parser("refresh-summary-views")
    sub.add_parser("rebuild-intelligence")
    sub.add_parser("enqueue-intelligence")
    worker = sub.add_parser("run-intelligence-worker")
    worker.add_argument("--limit", type=int)
    worker.add_argument("--watch", action="store_true")
    worker.add_argument("--sleep-seconds", type=float, default=5.0)


def _add_category_commands(sub: _Subcommands) -> None:
    discover = sub.add_parser("discover-categories")
    discover.add_argument("--since-hours", type=int, default=48)
    discover.add_argument("--limit", type=int)
    recategorize = sub.add_parser("recategorize")
    recategorize.add_argument("--since-hours", type=int)
    recategorize.add_argument("--limit", type=int)
    category_stats = sub.add_parser("category-stats")
    category_stats.add_argument("--after")
    category_stats.add_argument("--before")
    sub.add_parser("seed-categories")


def _add_people_provision_commands(sub: _Subcommands) -> None:
    alias = sub.add_parser("set-people-alias")
    alias.add_argument("address")
    alias.add_argument("person_key")
    alias.add_argument("--display-name")
    alias.add_argument("--note")
    provision = sub.add_parser("provision-plan")
    provision.add_argument("project_id")
    provision.add_argument("--service-account-name", default="gmeow-gmail")
    provision.add_argument("--key-file", default="data/secrets/service-account.json")


def _print_json(value: object) -> None:
    print(json.dumps(value, indent=2, default=str))


def _with_app(config: GmeowConfig, action: Callable[[_AppLike], object]) -> None:
    app = cast(_AppLike, create_app(config))
    try:
        _print_json(action(app))
    finally:
        app.state.cache.close()


def _serve(config: GmeowConfig, args: argparse.Namespace) -> None:
    if args.host:
        config.host = args.host
    if args.port:
        config.port = args.port
    uvicorn.run(create_app(config), host=config.host, port=config.port)


def _serve_imap(config: GmeowConfig, args: argparse.Namespace) -> None:
    if args.host:
        config.imap.host = args.host
    if args.port:
        config.imap.port = args.port
    run_imap_server(config)


def _sync(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, lambda app: app.state.sync.sync_priority())


def _sync_history(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, lambda app: app.state.sync.sync_history(limit=args.limit))


def _backfill(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(
        config,
        lambda app: app.state.sync.backfill_batch(
            batch_size=args.batch_size,
            validate=args.validate,
            max_empty_windows=args.max_empty_windows,
        ),
    )


def _cache_call(name: str, **kwargs: object) -> Callable[[_AppLike], object]:
    return lambda app: getattr(app.state.cache, name)(**kwargs)


def _sync_call(name: str, **kwargs: object) -> Callable[[_AppLike], object]:
    return lambda app: getattr(app.state.sync, name)(**kwargs)


def _status(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("sync_status"))


def _maintenance_status(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("maintenance_status"))


def _resilience_status(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("resilience_status"))


def _ops_events(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("operational_events", limit=args.limit, component=args.component, severity=args.severity))


def _jobs(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("list_intelligence_jobs", status=args.status, limit=args.limit))


def _dead_letter(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("list_dead_letter_jobs", limit=args.limit, include_closed=args.include_closed))


def _retry_dead(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("retry_dead_letter_jobs", limit=args.limit))


def _repair_cache(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("repair_cache", dry_run=not args.apply))


def _archive_status(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("archive_status", message_id=args.message_id, limit=args.limit))


def _refresh_archive_states(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_archive_states", limit=args.limit))


def _complete_archive(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _sync_call("complete_archive", limit=args.limit))


def _verify_objects(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("verify_objects", limit=args.limit))


def _export_archive(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, lambda app: app.state.cache.export_archive(args.target))


def _verify_archive(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, lambda app: app.state.cache.verify_archive_export(args.source))


def _restore_archive(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, lambda app: app.state.cache.restore_archive(args.source, dry_run=not args.apply))


def _apply_retention_policy(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, lambda app: app.state.cache.apply_retention_policy(args.message_id, source=args.source, dry_run=not args.apply))


def _retention_policies(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("retention_policies"))


def _imap_status(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("imap_status"))


def _refresh_imap(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_imap_mailboxes"))


def _storage_diagnostics(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("storage_diagnostics"))


def _prune_orphan_objects(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("prune_orphan_content_objects", dry_run=not args.apply))


def _analyze_storage(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("analyze_storage_tables"))


def _refresh_attachment_sidecars(config: GmeowConfig, _args: argparse.Namespace) -> None:
    app = cast(_AppLike, create_app(config))
    try:
        _print_json({"refreshed": _refresh_sidecars(app)})
    finally:
        app.state.cache.close()


def _refresh_sidecars(app: _AppLike) -> int:
    refreshed = 0
    for attachment in app.state.cache.list_attachments():
        source_metadata = _attachment_source_metadata(app, attachment)
        stored = app.state.attachments.refresh_sidecar(attachment["sha1"], source_metadata=source_metadata)
        attachment.update(
            {
                "metadata": stored.metadata,
                "digest": stored.digest,
                "compression": stored.compression,
                "path": str(stored.path),
            }
        )
        app.state.cache.add_attachment(attachment)
        app.state.cache.enqueue_intelligence_job("attachment", attachment["sha1"])
        refreshed += 1
    return refreshed


def _attachment_source_metadata(app: _AppLike, attachment: dict[str, object]) -> dict[str, object]:
    message_id = str(attachment["message_id"])
    message = cast(dict[str, object], app.state.cache.get_message(message_id) or {})
    return {
        "gmail": {
            "message_id": attachment["message_id"],
            "part_id": attachment.get("part_id"),
            "attachment_id": attachment.get("gmail_attachment_id"),
            "filename": attachment.get("filename"),
            "mime_type": attachment.get("mime_type"),
            "size": attachment.get("size"),
        },
        "message": {
            "thread_id": message.get("thread_id"),
            "subject": message.get("subject"),
            "from": message.get("sender"),
            "to": message.get("recipients"),
            "date": message.get("message_date"),
            "labels": message.get("label_ids", []),
        },
    }


def _refresh_message_search(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_message_search_columns", limit=args.limit))


def _refresh_graph_profiles(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_graph_node_profiles"))


def _refresh_content_refs(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_content_object_refs"))


def _refresh_summaries(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_summary_items"))


def _refresh_graph_edges(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_graph_edge_stats"))


def _refresh_timelines(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_timeline_views"))


def _refresh_summary_views(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("refresh_materialized_summary_views"))


def _rebuild_intelligence(config: GmeowConfig, _args: argparse.Namespace) -> None:
    app = cast(_AppLike, create_app(config))
    worker_service = IntelligenceWorker(app.state.cache, app.state.semantic, app.state.graph)
    enqueued = worker_service.enqueue_all()
    result = worker_service.run_until_empty()
    if app.state.graph is not None:
        app.state.graph.close()
    app.state.cache.close()
    _print_json({"enqueued": enqueued, **result})


def _enqueue_intelligence(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("enqueue_all_intelligence_jobs"))


def _run_intelligence_worker(config: GmeowConfig, args: argparse.Namespace) -> None:
    app = cast(_AppLike, create_app(config))
    worker_service = IntelligenceWorker(app.state.cache, app.state.semantic, app.state.graph)
    try:
        _run_worker_loop(worker_service, args)
    finally:
        if app.state.graph is not None:
            app.state.graph.close()
        app.state.cache.close()


def _run_worker_loop(worker_service: IntelligenceWorker, args: argparse.Namespace) -> None:
    if not args.watch:
        _print_json(worker_service.run_until_empty(limit=args.limit))
        return
    while True:
        result = worker_service.run_until_empty(limit=args.limit)
        print(json.dumps(result, indent=2), flush=True)
        if result["processed"] == 0 and result["failed"] == 0:
            time.sleep(args.sleep_seconds)


def _discover_categories(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, lambda app: CategoryEngine(app.state.cache).discover(since_hours=args.since_hours, limit=args.limit))


def _recategorize(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, lambda app: CategoryEngine(app.state.cache).recategorize(since_hours=args.since_hours, limit=args.limit))


def _category_stats(config: GmeowConfig, args: argparse.Namespace) -> None:
    _with_app(config, _cache_call("category_stats", after=args.after, before=args.before))


def _seed_categories(config: GmeowConfig, _args: argparse.Namespace) -> None:
    _with_app(config, lambda app: CategoryEngine(app.state.cache).seed_initial_categories())


def _doctor(config: GmeowConfig, _args: argparse.Namespace) -> None:
    run_doctor(config)


def _set_people_alias(config: GmeowConfig, args: argparse.Namespace) -> None:
    def action(app: _AppLike) -> dict[str, object]:
        app.state.cache.upsert_people_alias(args.address, args.person_key, display_name=args.display_name, note=args.note)
        return {"aliases": list(app.state.cache.people_aliases().values())}

    _with_app(config, action)


def _provision_plan(_config: GmeowConfig, args: argparse.Namespace) -> None:
    plan = build_gcloud_provision_plan(args.project_id, args.service_account_name, Path(args.key_file))
    print(plan.shell_script())
    print("\nAdmin Console steps:")
    for step in plan.admin_steps:
        print(f"- {step}")


COMMAND_HANDLERS: dict[str, CommandHandler] = {
    "serve": _serve,
    "serve-imap": _serve_imap,
    "sync": _sync,
    "sync-history": _sync_history,
    "backfill": _backfill,
    "status": _status,
    "maintenance-status": _maintenance_status,
    "resilience-status": _resilience_status,
    "ops-events": _ops_events,
    "jobs": _jobs,
    "dead-letter": _dead_letter,
    "retry-dead": _retry_dead,
    "repair-cache": _repair_cache,
    "archive-status": _archive_status,
    "refresh-archive-states": _refresh_archive_states,
    "complete-archive": _complete_archive,
    "verify-objects": _verify_objects,
    "export-archive": _export_archive,
    "verify-archive": _verify_archive,
    "restore-archive": _restore_archive,
    "apply-retention-policy": _apply_retention_policy,
    "retention-policies": _retention_policies,
    "imap-status": _imap_status,
    "refresh-imap": _refresh_imap,
    "storage-diagnostics": _storage_diagnostics,
    "prune-orphan-objects": _prune_orphan_objects,
    "analyze-storage": _analyze_storage,
    "doctor": _doctor,
    "refresh-attachment-sidecars": _refresh_attachment_sidecars,
    "refresh-message-search": _refresh_message_search,
    "refresh-graph-profiles": _refresh_graph_profiles,
    "refresh-content-refs": _refresh_content_refs,
    "refresh-summaries": _refresh_summaries,
    "refresh-graph-edges": _refresh_graph_edges,
    "refresh-timelines": _refresh_timelines,
    "refresh-summary-views": _refresh_summary_views,
    "rebuild-intelligence": _rebuild_intelligence,
    "enqueue-intelligence": _enqueue_intelligence,
    "run-intelligence-worker": _run_intelligence_worker,
    "discover-categories": _discover_categories,
    "recategorize": _recategorize,
    "category-stats": _category_stats,
    "seed-categories": _seed_categories,
    "set-people-alias": _set_people_alias,
    "provision-plan": _provision_plan,
}


def run_doctor(config: GmeowConfig) -> None:
    """Run doctor."""
    print(f"auth_mode: {config.auth_mode}")
    print(f"subject: {config.subject or '(none)'}")
    print(f"sops_secrets_file: {config.secrets.file or '(none)'} exists={bool(config.secrets.file and config.secrets.file.exists())}")
    print(f"service_account_configured: {bool(config.service_account_info() or config.service_account_file.exists())}")
    print(f"user_credentials_configured: {bool(config.user_credentials_info() or config.user_credentials_file.exists())}")
    try:
        app = cast(_AppLike, create_app(config))
        labels: list[dict[str, object]] = (
            cast(list[dict[str, object]], app.state.sync.gmail.list_labels()) if app.state.sync.gmail_available() else []
        )
    except RefreshError as exc:
        print("gmail_access: failed")
        print(f"error: {exc}")
        if config.auth_mode == "service_account":
            client_id = service_account_client_id(config)
            print("required_admin_console_grant:")
            print("  url: https://admin.google.com/ac/owl/domainwidedelegation")
            print(f"  client_id: {client_id or '(unknown; run gcloud iam service-accounts describe)'}")
            print("  scopes: https://www.googleapis.com/auth/gmail.modify")
        elif config.auth_mode == "user_oauth":
            print("required_local_oauth_command:")
            print(
                "  gcloud auth application-default login --scopes=https://www.googleapis.com/auth/gmail.modify,https://www.googleapis.com/auth/cloud-platform"
            )
        raise SystemExit(1) from exc
    except Exception as exc:
        print("gmail_access: failed")
        print(f"error: {exc}")
        raise SystemExit(1) from exc
    print("gmail_access: ok")
    print(f"labels_visible: {len(labels)}")


def service_account_client_id(config: GmeowConfig) -> str:
    """Service account client id."""
    info = config.service_account_info()
    if info:
        return str(info.get("client_id") or "")
    path = config.service_account_file
    if not path.exists():
        return ""
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        msg = f"Invalid service account JSON in {path}"
        raise ValueError(msg) from exc
    return str(data.get("client_id") or "")
