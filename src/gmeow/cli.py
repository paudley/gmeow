# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from google.auth.exceptions import RefreshError

import uvicorn

from .app import create_app
from .categories import CategoryEngine
from .config import GmeowConfig
from .imap_server import run_imap_server
from .intelligence import IntelligenceWorker
from .provision import build_gcloud_provision_plan


def main() -> None:
    parser = argparse.ArgumentParser(prog="gmeow")
    parser.add_argument("--config", default="config.toml")
    sub = parser.add_subparsers(dest="command")

    serve = sub.add_parser("serve")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve_imap = sub.add_parser("serve-imap")
    serve_imap.add_argument("--host")
    serve_imap.add_argument("--port", type=int)

    sub.add_parser("sync")
    history_sync = sub.add_parser("sync-history")
    history_sync.add_argument("--limit", type=int, default=500)
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

    alias = sub.add_parser("set-people-alias")
    alias.add_argument("address")
    alias.add_argument("person_key")
    alias.add_argument("--display-name")
    alias.add_argument("--note")

    provision = sub.add_parser("provision-plan")
    provision.add_argument("project_id")
    provision.add_argument("--service-account-name", default="gmeow-gmail")
    provision.add_argument("--key-file", default="data/secrets/service-account.json")

    args = parser.parse_args()
    config = GmeowConfig.load(args.config)

    if args.command == "serve":
        if args.host:
            config.host = args.host
        if args.port:
            config.port = args.port
        app = create_app(config)
        uvicorn.run(app, host=config.host, port=config.port)
        return

    if args.command == "serve-imap":
        if args.host:
            config.imap.host = args.host
        if args.port:
            config.imap.port = args.port
        run_imap_server(config)
        return

    if args.command == "sync":
        app = create_app(config)
        print(json.dumps(app.state.sync.sync_priority(), indent=2))
        return

    if args.command == "sync-history":
        app = create_app(config)
        print(json.dumps(app.state.sync.sync_history(limit=args.limit), indent=2))
        return

    if args.command == "status":
        app = create_app(config)
        print(json.dumps(app.state.cache.sync_status(), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "maintenance-status":
        app = create_app(config)
        print(json.dumps(app.state.cache.maintenance_status(), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "resilience-status":
        app = create_app(config)
        print(json.dumps(app.state.cache.resilience_status(), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "ops-events":
        app = create_app(config)
        print(json.dumps(app.state.cache.operational_events(limit=args.limit, component=args.component, severity=args.severity), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "jobs":
        app = create_app(config)
        print(json.dumps(app.state.cache.list_intelligence_jobs(status=args.status, limit=args.limit), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "dead-letter":
        app = create_app(config)
        print(json.dumps(app.state.cache.list_dead_letter_jobs(limit=args.limit, include_closed=args.include_closed), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "retry-dead":
        app = create_app(config)
        print(json.dumps(app.state.cache.retry_dead_letter_jobs(limit=args.limit), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "repair-cache":
        app = create_app(config)
        print(json.dumps(app.state.cache.repair_cache(dry_run=not args.apply), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "archive-status":
        app = create_app(config)
        print(json.dumps(app.state.cache.archive_status(message_id=args.message_id, limit=args.limit), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "refresh-archive-states":
        app = create_app(config)
        print(json.dumps(app.state.cache.refresh_archive_states(limit=args.limit), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "complete-archive":
        app = create_app(config)
        print(json.dumps(app.state.sync.complete_archive(limit=args.limit), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "verify-objects":
        app = create_app(config)
        print(json.dumps(app.state.cache.verify_objects(limit=args.limit), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "export-archive":
        app = create_app(config)
        print(json.dumps(app.state.cache.export_archive(args.target), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "verify-archive":
        app = create_app(config)
        print(json.dumps(app.state.cache.verify_archive_export(args.source), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "restore-archive":
        app = create_app(config)
        print(json.dumps(app.state.cache.restore_archive(args.source, dry_run=not args.apply), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "apply-retention-policy":
        app = create_app(config)
        print(json.dumps(app.state.cache.apply_retention_policy(args.message_id, source=args.source, dry_run=not args.apply), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "retention-policies":
        app = create_app(config)
        print(json.dumps(app.state.cache.retention_policies(), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "imap-status":
        app = create_app(config)
        print(json.dumps(app.state.cache.imap_status(), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "refresh-imap":
        app = create_app(config)
        print(json.dumps(app.state.cache.refresh_imap_mailboxes(), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "storage-diagnostics":
        app = create_app(config)
        print(json.dumps(app.state.cache.storage_diagnostics(), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "prune-orphan-objects":
        app = create_app(config)
        print(json.dumps(app.state.cache.prune_orphan_content_objects(dry_run=not args.apply), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "analyze-storage":
        app = create_app(config)
        print(json.dumps(app.state.cache.analyze_storage_tables(), indent=2, default=str))
        app.state.cache.close()
        return

    if args.command == "doctor":
        run_doctor(config)
        return

    if args.command == "refresh-attachment-sidecars":
        app = create_app(config)
        refreshed = 0
        for attachment in app.state.cache.list_attachments():
            message = app.state.cache.get_message(attachment["message_id"]) or {}
            source_metadata = {
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
            stored = app.state.attachments.refresh_sidecar(attachment["sha1"], source_metadata=source_metadata)
            attachment["metadata"] = stored.metadata
            attachment["digest"] = stored.digest
            attachment["compression"] = stored.compression
            attachment["path"] = str(stored.path)
            app.state.cache.add_attachment(attachment)
            app.state.cache.enqueue_intelligence_job("attachment", attachment["sha1"])
            refreshed += 1
        print(json.dumps({"refreshed": refreshed}, indent=2))
        return

    if args.command == "refresh-message-search":
        app = create_app(config)
        try:
            print(json.dumps(app.state.cache.refresh_message_search_columns(limit=args.limit), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "refresh-graph-profiles":
        app = create_app(config)
        try:
            print(json.dumps(app.state.cache.refresh_graph_node_profiles(), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "refresh-content-refs":
        app = create_app(config)
        try:
            print(json.dumps(app.state.cache.refresh_content_object_refs(), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "refresh-summaries":
        app = create_app(config)
        try:
            print(json.dumps(app.state.cache.refresh_summary_items(), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "refresh-graph-edges":
        app = create_app(config)
        try:
            print(json.dumps(app.state.cache.refresh_graph_edge_stats(), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "refresh-timelines":
        app = create_app(config)
        try:
            print(json.dumps(app.state.cache.refresh_timeline_views(), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "refresh-summary-views":
        app = create_app(config)
        try:
            print(json.dumps(app.state.cache.refresh_materialized_summary_views(), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "rebuild-intelligence":
        app = create_app(config)
        worker_service = IntelligenceWorker(app.state.cache, app.state.semantic, app.state.graph)
        enqueued = worker_service.enqueue_all()
        result = worker_service.run_until_empty()
        if app.state.graph is not None:
            app.state.graph.close()
        app.state.cache.close()
        print(json.dumps({"enqueued": enqueued, **result}, indent=2))
        return

    if args.command == "enqueue-intelligence":
        app = create_app(config)
        print(json.dumps(app.state.cache.enqueue_all_intelligence_jobs(), indent=2))
        app.state.cache.close()
        return

    if args.command == "run-intelligence-worker":
        app = create_app(config)
        worker_service = IntelligenceWorker(app.state.cache, app.state.semantic, app.state.graph)
        try:
            if args.watch:
                while True:
                    result = worker_service.run_until_empty(limit=args.limit)
                    print(json.dumps(result, indent=2), flush=True)
                    if result["processed"] == 0 and result["failed"] == 0:
                        time.sleep(args.sleep_seconds)
            else:
                print(json.dumps(worker_service.run_until_empty(limit=args.limit), indent=2))
        finally:
            if app.state.graph is not None:
                app.state.graph.close()
            app.state.cache.close()
        return

    if args.command == "discover-categories":
        app = create_app(config)
        try:
            print(json.dumps(CategoryEngine(app.state.cache).discover(since_hours=args.since_hours, limit=args.limit), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "recategorize":
        app = create_app(config)
        try:
            print(json.dumps(CategoryEngine(app.state.cache).recategorize(since_hours=args.since_hours, limit=args.limit), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "category-stats":
        app = create_app(config)
        try:
            print(json.dumps(app.state.cache.category_stats(after=args.after, before=args.before), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "seed-categories":
        app = create_app(config)
        try:
            print(json.dumps(CategoryEngine(app.state.cache).seed_initial_categories(), indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "set-people-alias":
        app = create_app(config)
        try:
            app.state.cache.upsert_people_alias(args.address, args.person_key, display_name=args.display_name, note=args.note)
            print(json.dumps({"aliases": list(app.state.cache.people_aliases().values())}, indent=2, default=str))
        finally:
            app.state.cache.close()
        return

    if args.command == "provision-plan":
        plan = build_gcloud_provision_plan(args.project_id, args.service_account_name, Path(args.key_file))
        print(plan.shell_script())
        print("\nAdmin Console steps:")
        for step in plan.admin_steps:
            print(f"- {step}")
        return

    parser.print_help()


def run_doctor(config: GmeowConfig) -> None:
    print(f"auth_mode: {config.auth_mode}")
    print(f"subject: {config.subject or '(none)'}")
    print(f"sops_secrets_file: {config.secrets.file or '(none)'} exists={bool(config.secrets.file and config.secrets.file.exists())}")
    print(f"service_account_configured: {bool(config.service_account_info() or config.service_account_file.exists())}")
    print(f"user_credentials_configured: {bool(config.user_credentials_info() or config.user_credentials_file.exists())}")
    try:
        app = create_app(config)
        labels = app.state.sync.gmail.list_labels() if app.state.sync.gmail else []
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
            print("  gcloud auth application-default login --scopes=https://www.googleapis.com/auth/gmail.modify,https://www.googleapis.com/auth/cloud-platform")
        raise SystemExit(1) from exc
    except Exception as exc:
        print("gmail_access: failed")
        print(f"error: {exc}")
        raise SystemExit(1) from exc
    print("gmail_access: ok")
    print(f"labels_visible: {len(labels)}")


def service_account_client_id(config: GmeowConfig) -> str | None:
    info = config.service_account_info()
    if info:
        return info.get("client_id")
    path = config.service_account_file
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return None
    return data.get("client_id")
