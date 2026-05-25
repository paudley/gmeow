# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Exercise core Gmeow cache and sync behavior.

These tests validate parsing, storage, graph, category, archive, intelligence, and backfill
workflows against PostgreSQL fixtures. They serve as executable specifications for correctness
across the mailbox ingestion pipeline."""

import base64
import json
import os
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

import psycopg
from psycopg import sql
import pytest

from gmeow.cache import categorize_message
from gmeow.categories import CategoryEngine, deterministic_assignments, message_document
from gmeow.runtime_config import RuntimeConfig, MaintenanceConfig, PriorityRule
from gmeow.graph import DOAP, FOAF, RDF_TYPE, SCHEMA, extract_attachment_sidecar_triples, extract_triples
from gmeow.intelligence import IntelligenceWorker
from gmeow.kg import spacy_entities
from gmeow.maintenance import MaintenanceScheduler
from gmeow.markdown import message_to_markdown
from gmeow.mcp_server import _format, enriched_message, enriched_thread
from gmeow.parser import parse_gmail_message
from gmeow.pg_cache import PgCache
from gmeow.semantic import chunk_text
from gmeow.sync import SyncService, message_matches_rule, should_search_gmail
from gmeow.text_index import TantivyMessageIndex
from gmeow.toon import dumps as toon_dumps

DEFAULT_LIST_STR = cast(list[str], None)
DEFAULT_INT = cast(int, None)
DEFAULT_STR = cast(str, None)
DEFAULT_DICT_ANY = cast(dict[str, Any], None)
DEFAULT_FAKE_GMAIL_METADATA = cast(dict[str, dict[str, Any]], None)

try:
    from gmeow.object_store import CasAttachmentStore, ObjectStore
except ModuleNotFoundError:
    CasAttachmentStore = cast(Any, None)
    ObjectStore = cast(Any, None)
DEFAULT_FAKE_GMAIL_PAGES = cast(dict[tuple[str, str], dict[str, Any]], None)

TEST_DSN = os.environ.get("GMEOW_TEST_POSTGRES_DSN", "")


def test_maintenance_backfill_defaults_off() -> None:
    config = MaintenanceConfig.from_dict({})

    assert config.backfill_enabled is False
    assert config.backfill_seconds == 5


def test_maintenance_zero_intervals_are_disabled() -> None:
    config = MaintenanceConfig.from_dict(
        {
            "sync_history_seconds": 0,
            "sync_priority_seconds": None,
            "intelligence_seconds": 30,
            "attachment_sidecars_seconds": False,
        }
    )

    assert config.sync_history_seconds is DEFAULT_INT
    assert config.sync_priority_seconds is DEFAULT_INT
    assert config.intelligence_seconds == 30
    assert config.attachment_sidecars_seconds is DEFAULT_INT


def test_maintenance_sync_history_lets_sync_service_resolve_missing_cursor() -> None:
    class Cache:
        def get_state(self, _key: str) -> str:
            return ""

    class Sync:
        called = False

        def gmail_available(self) -> bool:
            return True

        def sync_history(self, limit: int = 500) -> dict[str, Any]:
            self.called = True
            return {"limit": limit, "source": "fallback"}

    sync = Sync()
    scheduler = MaintenanceScheduler(MaintenanceConfig(sync_history_limit=12), cast(Any, Cache()), sync, cast(Any, object()), object())

    assert scheduler._sync_history() == {"limit": 12, "source": "fallback"}
    assert sync.called is True


@pytest.fixture(autouse=True)
def clean_pg_test_rows() -> Iterator[None]:
    cleanup_pg()
    yield
    cleanup_pg()


def cleanup_pg() -> None:
    if not TEST_DSN:
        return
    conn: Any
    tables = [
        "deferred_attachment_hydration",
        "search_runs",
        "embedding_chunks",
        "ingest_issues",
        "sync_runs",
        "dead_letter_jobs",
        "intelligence_jobs",
        "graph_triples",
        "graph_node_profiles",
        "graph_edge_stats",
        "summary_items",
        "attachments",
        "attachment_metadata_versions",
        "archive_exports",
        "content_object_refs",
        "message_categories",
        "category_overrides",
        "category_rules",
        "learned_category_runs",
        "message_parts",
        "messages",
        "threads",
        "labels",
        "priority_rules",
        "sync_state",
        "content_objects",
    ]
    with psycopg.connect(TEST_DSN) as conn:
        existing = [
            table
            for table in tables
            if conn.execute("SELECT to_regclass(%s) AS table_name", (table,)).fetchone()[0]
        ]
        if existing:
            conn.execute(
                sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
                    sql.SQL(", ").join(sql.Identifier(table) for table in existing)
                )
            )
    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        conn.execute("SET search_path=ag_catalog, public")
        try:
            conn.execute("SELECT drop_graph('gmeow_graph', true)")
        except psycopg.Error as exc:
            if "graph" not in str(exc).lower() or "does not exist" not in str(exc).lower():
                raise
        conn.execute("SELECT create_graph('gmeow_graph')")


def make_cache(tmp_path: Path) -> PgCache:
    if not TEST_DSN:
        pytest.skip("GMEOW_TEST_POSTGRES_DSN is required for Postgres integration tests")
    if ObjectStore is None:
        pytest.skip("Python object_store was retired in Go FILESTORE Phase 01")
    cache = PgCache(TEST_DSN, ObjectStore(tmp_path / "objects"), TantivyMessageIndex(tmp_path / "tantivy"))
    cache.ensure_default_categories()
    return cache


def make_attachments(tmp_path: Path) -> Any:
    if ObjectStore is None or CasAttachmentStore is None:
        pytest.skip("Python object_store was retired in Go FILESTORE Phase 01")
    return CasAttachmentStore(ObjectStore(tmp_path / "objects"))


def b64(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def gmail_fixture() -> dict[str, Any]:
    return {
        "id": "m1",
        "threadId": "t1",
        "historyId": "100",
        "labelIds": ["INBOX", "IMPORTANT"],
        "snippet": "Please review the Apollo invoice",
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": [
                {"name": "Subject", "value": "Apollo Invoice"},
                {"name": "From", "value": "Alice <alice@example.com>"},
                {"name": "To", "value": "Bob <bob@example.com>"},
                {"name": "Date", "value": "Fri, 22 May 2026 12:00:00 -0600"},
                {"name": "Message-ID", "value": "<m1@example.com>"},
                {"name": "Authentication-Results", "value": "mx.example.com; spf=pass dkim=pass dmarc=pass"},
                {"name": "Reply-To", "value": "Alice Replies <reply@example.com>"},
                {"name": "List-ID", "value": "Example List <example.example.com>"},
            ],
            "parts": [
                {
                    "partId": "1",
                    "mimeType": "text/plain",
                    "body": {"data": b64("Please review Apollo invoice. Action required by Monday."), "size": 56},
                },
                {
                    "partId": "2",
                    "mimeType": "application/pdf",
                    "filename": "invoice.pdf",
                    "body": {"attachmentId": "a1", "size": 12},
                },
            ],
        },
    }


def gmail_fixture_two() -> dict[str, Any]:
    value = gmail_fixture()
    value["id"] = "m2"
    value["threadId"] = "t1"
    value["snippet"] = "Zeus followup"
    for header in value["payload"]["headers"]:
        if header["name"] == "Subject":
            header["value"] = "Zeus Followup"
        if header["name"] == "From":
            header["value"] = "Carol <carol@example.com>"
        if header["name"] == "Date":
            header["value"] = "Sat, 23 May 2026 12:00:00 -0600"
    value["payload"]["parts"][0]["body"]["data"] = b64("Zeus followup with " + "long body " * 200)
    return value


def gmail_fixture_variant(message_id: str, subject: str, sender: str, date_header: str, body: str) -> dict[str, Any]:
    value = gmail_fixture()
    value["id"] = message_id
    value["threadId"] = f"t-{message_id}"
    value["snippet"] = body
    for header in value["payload"]["headers"]:
        if header["name"] == "Subject":
            header["value"] = subject
        if header["name"] == "From":
            header["value"] = sender
        if header["name"] == "Date":
            header["value"] = date_header
    value["payload"]["parts"][0]["body"]["data"] = b64(body)
    return value


def set_fixture_header(raw: dict[str, Any], name: str, value: str) -> dict[str, Any]:
    for header in raw["payload"]["headers"]:
        if header["name"] == name:
            header["value"] = value
            break
    return raw


def upsert_fixture_message(cache: PgCache, raw: dict[str, Any]) -> None:
    parsed = parse_gmail_message(raw)
    cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
    cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)


def unifi_fixture() -> dict[str, Any]:
    value = gmail_fixture()
    value["id"] = "u1"
    value["threadId"] = "tu1"
    value["snippet"] = "G4 Instant: 1 Smart Detection Date: Wednesday, May 20th 2026"
    value["labelIds"] = ["CATEGORY_UPDATES"]
    for header in value["payload"]["headers"]:
        if header["name"] == "Subject":
            header["value"] = "example-camera-system's G4 Instant has recorded an animal."
        if header["name"] == "From":
            header["value"] = '"UniFi OS, example-camera-system" <no-reply@notifications.ui.com>'
        if header["name"] == "Date":
            header["value"] = "Wed, 20 May 2026 06:51:04 -0600"
    value["payload"]["parts"][0]["body"]["data"] = b64("G4 Instant: 1 Smart Detection Link: https://unifi.ui.com/protect/events/event/123")
    return value


def ringcentral_fixture() -> dict[str, Any]:
    value = gmail_fixture()
    value["id"] = "r1"
    value["threadId"] = "tr1"
    value["snippet"] = "New Call Dear Pat Example, You have a new call from Jane Example."
    value["labelIds"] = ["CATEGORY_UPDATES"]
    for header in value["payload"]["headers"]:
        if header["name"] == "Subject":
            header["value"] = "New Call from Jane Example (555) 010-1234"
        if header["name"] == "From":
            header["value"] = "RingCentral <notify@ringcentral.com>"
    value["payload"]["parts"][0]["body"]["data"] = b64("New Call Received Status Missed RingCentral phone notice")
    return value


def github_fixture(
    message_id: str = "g1", subject: str = "Re: [example-org/example-project] Harden/core managed git e2e (PR #163)"
) -> dict[str, Any]:
    value = gmail_fixture()
    value["id"] = message_id
    value["threadId"] = "tg1"
    value["snippet"] = "@Copilot commented on this pull request. Pull request overview reviewed changed files."
    value["labelIds"] = ["CATEGORY_FORUMS"]
    for header in value["payload"]["headers"]:
        if header["name"] == "Subject":
            header["value"] = subject
        if header["name"] == "From":
            header["value"] = "Copilot <notifications@github.com>"
        if header["name"] == "To":
            header["value"] = '"example-org/example-project" <coding-ethos@noreply.github.com>'
    value["payload"]["parts"][0]["body"]["data"] = b64("Pull request review commented changed files workflow run CI")
    return value


def test_priority_rule_query() -> None:
    rule = PriorityRule(
        name="critical",
        gmail_query="is:important",
        labels=["INBOX"],
        from_domains=["example.com"],
        newer_than_days=7,
    )
    assert rule.to_gmail_query() == "(is:important) label:INBOX from:example.com newer_than:7d"


def test_parse_markdown_and_graph() -> None:
    parsed = parse_gmail_message(gmail_fixture())
    assert parsed.subject == "Apollo Invoice"
    assert "Action required" in parsed.text
    markdown = message_to_markdown(parsed, ["abc123"])
    assert "# Apollo Invoice" in markdown
    assert "SPF `pass`" in markdown
    assert "## Critical Facts" in markdown
    assert "Reply-To" in markdown
    triples = extract_triples(parsed, ["abc123"])
    assert ("gmeow:message/m1", "gmeow:hasAttachment", "gmeow:attachment/abc123", "m1") in triples
    assert ("gmeow:message/m1", RDF_TYPE, SCHEMA + "EmailMessage", "m1") in triples
    assert ("gmeow:address/alice@example.com", FOAF + "mbox", "mailto:alice@example.com", "m1") in triples
    assert any(predicate == "gmeow:hasTask" for _, predicate, _, _ in triples)
    assert any(predicate == "gmeow:mentions/org" for _, predicate, _, _ in triples)


def test_doap_project_graph_for_dev_notifications() -> None:
    parsed = parse_gmail_message(github_fixture())
    triples = extract_triples(parsed)
    assert any(predicate == RDF_TYPE and obj == DOAP + "Project" for _, predicate, obj, _ in triples)
    assert any(
        predicate == DOAP + "repository" and obj == "https://github.com/example-org/example-project" for _, predicate, obj, _ in triples
    )
    assert any(predicate == SCHEMA + "about" and obj.startswith("gmeow:project/") for _, predicate, obj, _ in triples)


def test_spacy_ner_and_sidecar_kg() -> None:
    entities = spacy_entities("Pat Example met OpenAI in Edmonton on May 22, 2026.")
    assert any(entity["kind"] == "person" and "Pat" in entity["text"] for entity in entities)
    assert any(entity["kind"] == "org" and entity["text"] == "OpenAI" for entity in entities)
    html_entities = spacy_entities(
        "<!DOCTYPE html><html><head><style>.gmail_quote{font-family:Arial}</style></head>"
        "<body>Pat Example met OpenAI in Edmonton.</body></html>"
    )
    assert any(entity["kind"] == "person" and "Pat" in entity["text"] for entity in html_entities)
    assert any(entity["kind"] == "org" and entity["text"] == "OpenAI" for entity in html_entities)
    assert not any(entity["text"].lower() in {"doctype", "html", "style"} for entity in html_entities)
    triples = extract_attachment_sidecar_triples(
        "abc123",
        {
            "source": {
                "gmail": {
                    "message_id": "m1",
                    "filename": "invoice.pdf",
                    "mime_type": "application/pdf",
                    "size": 12,
                },
                "message": {"subject": "Apollo Invoice", "from": "alice@example.com"},
            },
            "exiftool": {"tags": {"File:FileType": "PDF", "PDF:Title": "Quarterly Statement"}},
        },
    )
    assert ("gmeow:attachment/abc123", "gmeow:fileType", "PDF", "m1") in triples
    assert ("gmeow:attachment/abc123", "gmeow:documentTitle", "Quarterly Statement", "m1") in triples
    assert any(predicate == "gmeow:mentions/org" for _, predicate, _, _ in triples)


def test_cache_offline_message_search(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    cache.upsert_label({"id": "INBOX", "name": "Inbox", "type": "system"})
    parsed = parse_gmail_message(gmail_fixture())
    markdown = message_to_markdown(parsed)
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=markdown, hydrated=True)
    cache.add_triples(extract_triples(parsed))
    assert cache.get_message("m1")["subject"] == "Apollo Invoice"
    assert cache.get_message("m1")["message_date_iso"] == "2026-05-22T18:00:00Z"
    assert cache.get_message("m1")["labels"] == ["Inbox", "IMPORTANT"]
    assert cache.get_thread("t1")["messages"][0]["id"] == "m1"
    assert cache.text_search("Apollo")[0]["id"] == "m1"
    assert cache.text_search("from:alice subject:Invoice after:2026-05-01")[0]["id"] == "m1"
    assert cache.text_search("Apollo before:2026-05-23")[0]["id"] == "m1"
    assert cache.text_search("Apollo after:2026-05-23") == []
    assert cache.address_messages("alice@example.com")[0]["id"] == "m1"
    assert cache.graph_search("Apollo")
    cache.close()


def test_compact_search_results_and_thread_order(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    first = parse_gmail_message(gmail_fixture())
    second = parse_gmail_message(gmail_fixture_two())
    cache.upsert_thread({"id": "t1", "snippet": "thread"})
    cache.upsert_message(second, gmail_fixture_two(), markdown=message_to_markdown(second), hydrated=True)
    cache.upsert_message(first, gmail_fixture(), markdown=message_to_markdown(first), hydrated=True)
    compact = cache.compact_messages(cache.text_search("followup"), body_chars=80)
    assert compact[0]["id"] == "m2"
    assert len(compact[0]["text"]) <= 83
    assert "raw" not in compact[0]
    thread = cache.get_thread("t1", compact=True)
    assert [message["id"] for message in thread["messages"]] == ["m1", "m2"]
    cache.close()


def test_message_categories_exclude_camera_alerts_by_default(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    raw = unifi_fixture()
    parsed = parse_gmail_message(raw)
    cache.upsert_label({"id": "CATEGORY_UPDATES", "name": "Updates", "type": "system"})
    cache.upsert_thread({"id": "tu1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)
    message = cache.get_message("u1")
    assert "camera_alert" in categorize_message(message)
    assert cache.text_search("UniFi") == []
    assert cache.text_search("UniFi", include_categories=["camera_alert"])[0]["id"] == "u1"
    assert cache.address_messages("notifications.ui.com") == []
    assert cache.address_messages("notifications.ui.com", include_categories=["camera_alert"])[0]["id"] == "u1"
    cache.close()


def test_text_search_applies_include_categories_before_limit(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    older = gmail_fixture_variant(
        "categorized",
        "Limit Needle",
        "Older <shared@example.com>",
        "Fri, 22 May 2026 12:00:00 -0600",
        "Limit needle categorized body",
    )
    newer = gmail_fixture_variant(
        "uncategorized",
        "Limit Needle",
        "Newer <shared@example.com>",
        "Sat, 23 May 2026 12:00:00 -0600",
        "Limit needle uncategorized body",
    )
    for raw in [older, newer]:
        parsed = parse_gmail_message(raw)
        cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
        cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)
    cache.apply_category("categorized", "needle_category", reason="test")

    results = cache.text_search("Limit Needle", limit=1, include_categories=["needle_category"])

    assert [message["id"] for message in results] == ["categorized"]
    cache.close()


def test_address_messages_applies_include_categories_before_limit(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    older = gmail_fixture_variant(
        "categorized",
        "Address Needle",
        "Shared <shared@example.com>",
        "Fri, 22 May 2026 12:00:00 -0600",
        "Older categorized address message",
    )
    newer = gmail_fixture_variant(
        "uncategorized",
        "Address Needle",
        "Shared <shared@example.com>",
        "Sat, 23 May 2026 12:00:00 -0600",
        "Newer uncategorized address message",
    )
    for raw in [older, newer]:
        parsed = parse_gmail_message(raw)
        cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
        cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)
    cache.apply_category("categorized", "address_category", reason="test")

    results = cache.address_messages("shared@example.com", limit=1, include_categories=["address_category"])

    assert [message["id"] for message in results] == ["categorized"]
    cache.close()


def test_category_engine_persists_system_and_manual_categories(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    for raw in [unifi_fixture(), ringcentral_fixture(), github_fixture()]:
        parsed = parse_gmail_message(raw)
        cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
        cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)
    result = CategoryEngine(cache).recategorize()
    assert result["messages"] == 3
    assert "camera_alert" in cache.get_message("u1")["categories"]
    assert "call_notice" in cache.get_message("r1")["categories"]
    assert "dev_activity" in cache.get_message("g1")["categories"]
    assert cache.text_search("UniFi") == []
    assert cache.text_search("UniFi", include_categories=["camera_alert"])[0]["id"] == "u1"
    cache.apply_category("u1", "personal", reason="manual test")
    assert "personal" in cache.get_message("u1")["categories"]
    cache.apply_category("u1", "camera_alert", action="exclude", reason="manual test")
    assert "camera_alert" not in cache.get_message("u1")["categories"]
    cache.close()


def test_seed_initial_categories_adds_manual_rules(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    seeded = CategoryEngine(cache).seed_initial_categories()
    assert seeded["rules_inserted"] > 0
    assert any(rule["category"] == "real_estate" for rule in cache.list_category_rules())
    assert any(category["id"] == "corporate_filing" for category in cache.list_categories())
    assert {"call_notice", "dev_activity", "dev_review", "mailing_list"} <= cache.default_excluded_categories()
    raw = gmail_fixture()
    raw["id"] = "real1"
    for header in raw["payload"]["headers"]:
        if header["name"] == "Subject":
            header["value"] = "FINAL SUBJECT REMOVAL- 123 Example Street"
        if header["name"] == "From":
            header["value"] = '"Example Agent" <agent@example-realestate.test>'
    parsed = parse_gmail_message(raw)
    cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
    cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)
    CategoryEngine(cache).recategorize()
    assert "real_estate" in cache.get_message("real1")["categories"]
    cache.close()


def test_category_discovery_uses_sklearn_clusters(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    raws = [github_fixture("g1"), github_fixture("g2", "Re: [example-org/example-project] Add output surface report (PR #162)")]
    raws += [unifi_fixture(), ringcentral_fixture()]
    for raw in raws:
        parsed = parse_gmail_message(raw)
        cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
        cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)
    run = CategoryEngine(cache).discover(since_hours=24 * 365 * 10)
    assert run["messages"] == 4
    assert run["clusters"]
    assert cache.learned_category_runs()[0]["run"]["clusters"]
    learned_id = run["clusters"][0]["id"]
    enabled = CategoryEngine(cache).enable_learned_category(learned_id)
    assert enabled["messages"] >= 2
    cache.close()


def test_category_text_preparation_drops_noise() -> None:
    message = parse_gmail_message(unifi_fixture())
    cache_message = {
        "sender": message.sender,
        "subject": message.subject,
        "snippet": message.snippet,
        "text_body": message.text,
        "labels": ["Updates"],
        "label_ids": ["CATEGORY_UPDATES"],
    }
    doc = message_document(cache_message)
    assert "unifi" in doc
    assert "https" not in doc
    assert "123" not in doc
    assert any(item["category"] == "camera_alert" for item in deterministic_assignments(cache_message))


def test_priority_rule_post_filter() -> None:
    parsed = parse_gmail_message(gmail_fixture())
    assert message_matches_rule(
        parsed,
        PriorityRule(
            name="pdf_invoice",
            attachment_mime=["application/pdf"],
            attachment_filename_contains=["invoice"],
            header_contains={"subject": "Apollo"},
        ),
    )
    assert not message_matches_rule(
        parsed,
        PriorityRule(name="image_only", attachment_mime=["image/"], attachment_filename_contains=["photo"]),
    )


def test_attachment_sidecar_text_search(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    cache.add_attachment(
        {
            "sha1": "abc123",
            "message_id": "m1",
            "part_id": "2",
            "gmail_attachment_id": "a1",
            "filename": "invoice.pdf",
            "mime_type": "application/pdf",
            "size": 12,
            "metadata": {
                "source": {"message": {"subject": "Apollo Invoice"}},
                "exiftool": {"tags": {"PDF:Title": "Quarterly Statement"}},
            },
            "path": str(tmp_path / "invoice.pdf"),
        }
    )
    results = cache.attachment_text_search("Quarterly")
    assert results[0]["sha1"] == "abc123"
    assert results[0]["metadata"]["exiftool"]["tags"]["PDF:Title"] == "Quarterly Statement"
    cache.add_attachment(
        {
            "sha1": "def456",
            "message_id": "m2",
            "part_id": "2",
            "gmail_attachment_id": "a2",
            "filename": "scan.png",
            "mime_type": "image/png",
            "size": 12,
            "metadata": {"image_description": "A scanned permit approval letter."},
            "path": str(tmp_path / "scan.png"),
        }
    )
    message_attachments = cache.attachment_text_for_message("m2")
    assert message_attachments[0]["text"] == "A scanned permit approval letter."
    assert "metadata" not in cache.attachment_text_for_message("m2", include_metadata=False)[0]
    cache.close()


def test_mcp_message_enrichment_omits_attachment_paths(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(gmail_fixture())
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    cache.add_triples(extract_triples(parsed, ["abc123"]))
    cache.add_attachment(
        {
            "sha1": "abc123",
            "message_id": "m1",
            "part_id": "2",
            "gmail_attachment_id": "a1",
            "filename": "invoice.pdf",
            "mime_type": "application/pdf",
            "size": 12,
            "metadata": {"source": {"message": {"subject": "Apollo Invoice"}}},
            "path": str(tmp_path / "invoice.pdf"),
        }
    )
    message = enriched_message(cache, "m1")
    assert message is not None
    assert message["graph"]
    assert message["graph_nodes"]["messages"][0]["id"] == "gmeow:message/m1"
    assert message["graph_nodes"]["attachments"][0]["id"] == "gmeow:attachment/abc123"
    assert message["attachment_sidecars"][0]["metadata"]["source"]["message"]["subject"] == "Apollo Invoice"
    assert "path" not in message["attachment_sidecars"][0]
    thread = enriched_thread(cache, "t1")
    assert thread["messages"][0]["attachment_sidecars"][0]["sha1"] == "abc123"
    compact_thread = enriched_thread(cache, "t1", compact=True)
    assert compact_thread["messages"][0]["from"] == "Alice <alice@example.com>"
    assert compact_thread["messages"][0]["date"] == "2026-05-22T18:00:00Z"
    assert "raw" not in compact_thread["messages"][0]
    cache.close()


def test_graph_discovery_helpers(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(gmail_fixture())
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    other_raw = gmail_fixture()
    other_raw["id"] = "m2"
    other_raw["threadId"] = "t2"
    other = parse_gmail_message(other_raw)
    cache.upsert_thread({"id": "t2", "snippet": other.snippet})
    cache.upsert_message(other, other_raw, markdown=message_to_markdown(other), hydrated=True)
    cache.add_triples(
        [
            ("gmeow:message/m1", "gmeow:mentionsEntity", "gmeow:entity/Apollo", "m1"),
            ("gmeow:message/m2", "gmeow:mentionsEntity", "gmeow:entity/Apollo", "m2"),
            ("gmeow:message/m2", "gmeow:mentionsEntity", "gmeow:entity/Zeus", "m2"),
        ]
    )
    neighborhood = cache.graph_neighbors("gmeow:entity/Apollo")
    assert "gmeow:message/m1" in neighborhood["nodes"]
    assert cache.graph_related_messages("m1")[0]["message_id"] == "m2"
    top = cache.graph_top_nodes(prefix="gmeow:entity/")
    assert top[0]["node"] == "gmeow:entity/Apollo"
    assert top[0]["messages"] == 2
    nodes = cache.graph_nodes_for_message("m1")
    assert nodes["entities"][0]["label"] == "Apollo"
    projection = cache.graph_projection(limit=2)
    assert projection["nodes"] >= 3
    assert projection["edges"] >= 3
    assert projection["top_nodes"]
    path = cache.graph_path("gmeow:message/m1", "gmeow:entity/Apollo")
    assert path["found"] is True
    ranked = cache.graph_ranked_nodes(kind="entity", limit=2)
    assert ranked[0]["node"] == "gmeow:entity/Apollo"
    cache.add_triples([("gmeow:message/m1", "gmeow:mentionsEntity", "gmeow:entity/DOCTYPE", "m1")])
    cache.add_triples([("gmeow:message/m1", "gmeow:mentions/cardinal", "gmeow:cardinal/360", "m1")])
    assert all(node["label"] != "DOCTYPE" for node in cache.graph_top_nodes(prefix="gmeow:entity/"))
    assert all(node["label"] != "360" for node in cache.graph_top_nodes())
    cache.close()


def test_project_ranking_and_ontology_profile(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(github_fixture())
    cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
    cache.upsert_message(parsed, github_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    cache.add_triples(extract_triples(parsed))
    projects = cache.graph_projects()
    assert projects
    assert projects[0]["project"] == "gmeow:project/example-org_example-project"
    assert projects[0]["repositories"] == ["https://github.com/example-org/example-project"]
    profile = cache.ontology_profile()
    assert profile["ontologies"]["doap"]["triples"] > 0
    cache.close()


def test_age_status_and_read_only_cypher(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    cache.add_triples([("gmeow:message/m1", "gmeow:mentionsEntity", "gmeow:entity/Apollo", "m1")])
    status = cache.age_status()
    assert status["available"] is True
    rows = cache.age_cypher("MATCH (n) RETURN count(n)", columns="count agtype")
    assert rows
    with pytest.raises(ValueError):
        cache.age_cypher("CREATE (n)")


def test_ingest_issue_records_artifact(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    issue_id = cache.record_ingest_issue("bad1", "gmail.full", "error", "bad payload", {"id": "bad1", "payload": None})
    issues = cache.list_ingest_issues()
    assert issues[0]["id"] == issue_id
    assert issues[0]["message_id"] == "bad1"
    assert issues[0]["artifact_digest"]
    assert cache.sync_status()["counts"]["ingest_issues"] == 1


def test_contacts_resolve_address_variants(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    first_raw = gmail_fixture()
    second_raw = set_fixture_header(gmail_fixture_two(), "From", "Alice Example <alice@example.com>")
    for raw in [first_raw, second_raw]:
        upsert_fixture_message(cache, raw)
    contacts = cache.contacts()
    alice = next(contact for contact in contacts if contact["address"] == "alice@example.com")
    assert alice["messages"] == 2
    assert alice["display_name"] == "Alice Example"
    assert set(alice["names"]) == {"Alice", "Alice Example"}
    alias_raw = gmail_fixture()
    alias_raw["id"] = "m3"
    set_fixture_header(alias_raw, "From", "Alice Example <alice@work.example>")
    upsert_fixture_message(cache, alias_raw)
    people = cache.people()
    alice_person = next(person for person in people if person["name"] == "Alice Example")
    assert alice_person["addresses"] == ["alice@example.com", "alice@work.example"]
    cache.upsert_people_alias("a.smith@example.net", "Alice Example", display_name="Alice Example")
    alias_raw = gmail_fixture()
    alias_raw["id"] = "m4"
    set_fixture_header(alias_raw, "From", "A Smith <a.smith@example.net>")
    upsert_fixture_message(cache, alias_raw)
    aliased = next(person for person in cache.people() if person["name"] == "Alice Example")
    assert "a.smith@example.net" in aliased["addresses"]
    cache.close()


def test_toon_serializer_compacts_uniform_rows() -> None:
    payload = {"messages": [{"id": "m1", "subject": "Apollo"}, {"id": "m2", "subject": "Zeus"}]}
    toon = toon_dumps(payload)
    assert "messages[2]{id,subject}:" in toon
    assert "m1,Apollo" in toon
    assert isinstance(_format(payload, "toon"), str)
    assert _format(payload, "json") == payload


def test_priority_rule_serializes_for_help() -> None:
    rule = PriorityRule(name="mailbox-owner", gmail_query="from:owner.com")
    assert asdict(rule)["gmail_query"] == "from:owner.com"


class RecordingSemantic:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, dict[str, Any]]] = []
        self.attachments: list[tuple[str, str, dict[str, Any]]] = []

    def index_message(self, message_id: str, text: str, metadata: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        self.messages.append((message_id, text, metadata))

    def index_attachment(self, sha1: str, text: str, metadata: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        self.attachments.append((sha1, text, metadata))

    def search(self, _query: str, limit: int = 10, source_kind: str = DEFAULT_STR) -> list[dict[str, Any]]:
        return []

    def available(self) -> bool:
        return True


class FailingSemantic:
    def index_message(self, message_id: str, text: str, metadata: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        raise RuntimeError("semantic unavailable")

    def index_attachment(self, sha1: str, text: str, metadata: dict[str, Any] = DEFAULT_DICT_ANY) -> None:
        raise RuntimeError("semantic unavailable")

    def search(self, _query: str, limit: int = 10, source_kind: str = DEFAULT_STR) -> list[dict[str, Any]]:
        raise RuntimeError("semantic unavailable")

    def available(self) -> bool:
        return False


class FakeGmail:
    def __init__(
        self,
        messages: dict[str, dict[str, Any]],
        pages: dict[tuple[str, str], dict[str, Any]] = DEFAULT_FAKE_GMAIL_PAGES,
        metadata: dict[str, dict[str, Any]] = DEFAULT_FAKE_GMAIL_METADATA,
    ) -> None:
        self.messages = messages
        self.pages = pages or {}
        self.metadata = metadata or {}
        self.queries: list[str] = []
        self.page_queries: list[dict[str, Any]] = []
        self.metadata_fetched: list[str] = []
        self.fetched: list[tuple[str, str]] = []
        self.attachments_fetched: list[tuple[str, str]] = []
        self.modified: list[dict[str, Any]] = []

    def list_labels(self) -> list[dict[str, str]]:
        return [{"id": "INBOX", "name": "Inbox", "type": "system"}]

    def search_messages(self, query: str, limit: int = 100) -> list[str]:
        self.queries.append(query)
        return list(self.messages)[:limit]

    def search_messages_page(self, query: str, page_token: str = DEFAULT_STR, page_size: int = 50) -> dict[str, Any]:
        self.page_queries.append({"query": query, "page_token": page_token, "page_size": page_size})
        page = self.pages.get((query, page_token))
        if page is not None:
            return page
        ids = list(self.messages)[:page_size]
        return {"messages": [{"id": message_id} for message_id in ids], "next_page_token": ""}

    def list_history(self, start_history_id: str, label_id: str = DEFAULT_STR, limit: int = 500) -> dict[str, Any]:
        return {
            "history_id": "200",
            "history": [
                {"id": "101", "messagesAdded": [{"message": {"id": "m2"}}]},
                {"id": "102", "labelsAdded": [{"message": {"id": "m1"}}]},
            ],
        }

    def get_message(self, message_id: str, fmt: str = "full") -> dict[str, Any]:
        self.fetched.append((message_id, fmt))
        if fmt == "raw":
            return {"id": message_id, "raw": b64("Subject: Apollo\r\n\r\nBody")}
        return self.messages[message_id]

    def get_message_metadata(self, message_id: str) -> dict[str, Any]:
        self.metadata_fetched.append(message_id)
        if message_id in self.metadata:
            return self.metadata[message_id]
        raw = self.messages[message_id]
        return {
            "id": raw["id"],
            "threadId": raw.get("threadId"),
            "historyId": raw.get("historyId"),
            "labelIds": raw.get("labelIds", []),
            "internalDate": raw.get("internalDate"),
            "payload": {"headers": (raw.get("payload") or {}).get("headers", [])},
        }

    def get_thread(self, thread_id: str) -> dict[str, Any]:
        return {}

    def get_attachment(self, message_id: str, attachment_id: str) -> bytes:
        self.attachments_fetched.append((message_id, attachment_id))
        return b""

    def modify_message(
        self,
        message_id: str,
        add_label_ids: list[str] = DEFAULT_LIST_STR,
        remove_label_ids: list[str] = DEFAULT_LIST_STR,
    ) -> dict[str, str]:
        self.modified.append({"message_id": message_id, "add": add_label_ids or [], "remove": remove_label_ids or []})
        return {"id": message_id}


class FailingSearchGmail(FakeGmail):
    def __init__(self, messages: dict[str, dict[str, Any]]) -> None:
        super().__init__(messages)
        self.search_attempts = 0

    def search_messages(self, query: str, limit: int = 100) -> list[str]:
        self.search_attempts += 1
        raise OSError(f"gmail unavailable for {query} limit={limit}")


def test_intelligence_worker_runs_until_empty(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(gmail_fixture())
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    cache.add_attachment(
        {
            "sha1": "abc123",
            "message_id": "m1",
            "part_id": "2",
            "gmail_attachment_id": "a1",
            "filename": "invoice.pdf",
            "mime_type": "application/pdf",
            "size": 12,
            "metadata": {"source": {"gmail": {"message_id": "m1", "filename": "invoice.pdf"}}},
            "path": str(tmp_path / "invoice.pdf"),
        }
    )
    cache.add_triples(
        [
            ("gmeow:message/m1", "gmeow:mentions/org", "gmeow:org/DOCTYPE", "m1"),
            ("gmeow:attachment/abc123", "gmeow:fileType", "OLD", "m1"),
        ]
    )
    cache.enqueue_intelligence_job("message", "m1")
    cache.enqueue_intelligence_job("attachment", "abc123")
    semantic = RecordingSemantic()
    result = IntelligenceWorker(cache, semantic).run_until_empty()
    assert result == {"processed": 2, "failed": 0, "remaining": 0}
    assert semantic.messages[0][0] == "m1"
    assert "DOCTYPE" not in semantic.messages[0][1]
    assert semantic.attachments[0][0] == "abc123"
    assert cache.graph_triples_for_message("m1")
    assert not cache.graph_search("DOCTYPE")
    assert not cache.graph_search("OLD")
    assert cache.intelligence_job_status()["done"] == 2
    cache.close()


def test_search_uses_live_gmail_for_unbounded_queries(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("Apollo", limit=5)
    assert result["source"] == "gmail+cache"
    assert gmail.queries == ["Apollo"]
    assert result["messages"][0]["id"] == "m1"
    assert len(result["messages"]) == result["live"]["hydrated"]
    assert result["messages"][0]["text_body"]
    assert result["analysis"]["pending"] is True
    assert result["analysis"]["status_url"] == "/api/v1/search/analysis-status?message_ids=m1"
    assert cache.get_message("m1") is not None
    assert cache.intelligence_job_status()["pending"] >= 1
    cache.close()


def test_live_search_defers_attachment_and_raw_hydration(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)

    result = sync.search("Apollo", limit=5)

    assert result["source"] == "gmail+cache"
    assert result["messages"][0]["id"] == "m1"
    assert result["messages"][0]["subject"] == "Apollo Invoice"
    assert "Action required by Monday" in result["messages"][0]["text_body"]
    assert gmail.fetched == [("m1", "full")]
    assert gmail.attachments_fetched == []
    assert cache.attachments_for_message("m1") == []
    assert cache.get_message("m1")["has_raw_rfc822"] is False
    assert result["live"]["deferred_attachments"] == 1
    assert result["live"]["analysis"]["pending"] is True
    assert result["attachment_hydration"]["counts"] == {"pending": 1}
    assert result["search_id"]
    status = sync.search_status(result["search_id"])
    assert status["status"] == "complete"
    assert status["message_ids"] == ["m1"]
    assert status["messages"][0]["id"] == "m1"
    assert status["analysis"]["pending"] is True
    assert status["attachment_hydration"]["counts"] == {"pending": 1}
    cache.close()


def test_live_search_degrades_to_cache_and_pauses_gmail_after_error(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(gmail_fixture())
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    gmail = FailingSearchGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)

    result = sync.search("Apollo", limit=5)

    assert result["source"] == "gmail+cache"
    assert result["live"]["degraded"] is True
    assert "gmail unavailable" in result["live"]["error"]
    assert result["messages"][0]["id"] == "m1"
    assert gmail.search_attempts == 1
    assert cache.get_state("gmail.live_search.paused_until")

    paused = sync.search("Apollo", limit=5)

    assert paused["live"]["degraded"] is True
    assert paused["live"]["reason"] == "gmail live search temporarily paused after recent errors"
    assert paused["messages"][0]["id"] == "m1"
    assert gmail.search_attempts == 1
    cache.close()


def test_live_search_returns_default_hidden_messages_inline(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"u1": unifi_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("UniFi", limit=5)
    assert result["source"] == "gmail+cache"
    assert result["live"]["hydrated"] == 1
    assert result["messages"][0]["id"] == "u1"
    assert "camera_alert" in result["messages"][0]["categories"]


def test_live_search_prefers_cached_analyzed_message(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(gmail_fixture())
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    cache.apply_category("m1", "personal", reason="already analyzed")
    gmail = FakeGmail({"m1": gmail_fixture_two()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("Apollo", limit=5)
    assert result["messages"][0]["id"] == "m1"
    assert result["messages"][0]["subject"] == "Apollo Invoice"
    assert "personal" in result["messages"][0]["categories"]
    assert gmail.fetched == []
    cache.close()


def test_hydrate_applies_manual_rules_and_star_raw_helpers(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    CategoryEngine(cache).seed_initial_categories()
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    message = sync.hydrate_message("m1")
    assert "financial_statement" in message["categories"]
    assert sync.star("m1", starred=True)["id"] == "m1"
    assert gmail.modified[-1] == {"message_id": "m1", "add": ["STARRED"], "remove": []}
    assert sync.star("m1", starred=False)["id"] == "m1"
    assert gmail.modified[-1] == {"message_id": "m1", "add": [], "remove": ["STARRED"]}
    assert sync.hydrate_raw_rfc822("m1") == b"Subject: Apollo\r\n\r\nBody"
    assert sync.hydrate_raw_rfc822("m1") == b"Subject: Apollo\r\n\r\nBody"
    assert gmail.fetched.count(("m1", "raw")) == 1
    assert cache.get_state("gmail_history_id") == "100"
    cache.close()


def test_history_sync_hydrates_changed_messages(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    cache.set_state("gmail_history_id", "100")
    gmail = FakeGmail({"m1": gmail_fixture(), "m2": gmail_fixture_two()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.sync_history(limit=10)
    assert result["messages"] == 2
    assert result["hydrated"] == 2
    assert cache.get_message("m2") is not None
    assert cache.get_state("gmail_history_id") == "200"
    cache.close()


def test_backfill_processes_one_monthly_page_and_advances_page(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    cache.set_state("backfill.window_start", "2026-05-01")
    query = "in:anywhere after:2026/04/30 before:2026/06/01"
    gmail = FakeGmail(
        {"m1": gmail_fixture(), "m2": gmail_fixture_two()},
        pages={(query, ""): {"messages": [{"id": "m1"}], "next_page_token": "p2", "result_size_estimate": 2}},
    )
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.backfill_batch(batch_size=1)
    assert result["message_ids"] == ["m1"]
    assert result["advanced"] is True
    assert result["hydrated"] == 1
    assert cache.get_state("backfill.window_start") == "2026-05-01"
    assert cache.get_state("backfill.page_token") == "p2"
    assert cache.get_state("backfill.current_batch") == ""
    assert cache.intelligence_job_status()["done"] == 2
    cache.close()


def test_backfill_resumes_current_batch_without_fetching_next_page(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    cache.set_state(
        "backfill.current_batch",
        json.dumps(
            {
                "complete": False,
                "query": "in:anywhere after:2026/04/30 before:2026/06/01",
                "window_start": "2026-05-01",
                "window_end": "2026-06-01",
                "page_token": "",
                "next_page_token": "",
                "message_ids": ["m1"],
                "started_at": "2026-05-24T00:00:00Z",
            }
        ),
    )
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.backfill_batch(batch_size=1)
    assert result["advanced"] is True
    assert gmail.page_queries == []
    assert cache.get_state("backfill.window_start") == "2026-06-01"
    assert cache.get_state("backfill.current_batch") == ""
    cache.close()


def test_backfill_skips_complete_message_without_validation_metadata(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    sync.hydrate_message("m1", update_history_cursor=False)
    IntelligenceWorker(cache, RecordingSemantic()).run_until_empty()
    gmail.fetched.clear()
    cache.set_state("backfill.window_start", "2026-05-01")
    result = sync.backfill_batch(batch_size=1)
    assert result["skipped"] == 1
    assert result["hydrated"] == 0
    assert gmail.metadata_fetched == []
    assert gmail.fetched == []
    cache.close()


def test_backfill_validation_rehydrates_changed_metadata(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    sync.hydrate_message("m1", update_history_cursor=False)
    IntelligenceWorker(cache, RecordingSemantic()).run_until_empty()
    changed = gmail_fixture()
    changed["historyId"] = "999"
    changed["snippet"] = "Changed Apollo"
    gmail.messages["m1"] = changed
    gmail.metadata["m1"] = {
        "id": "m1",
        "threadId": changed["threadId"],
        "historyId": "999",
        "labelIds": changed["labelIds"],
        "payload": {"headers": changed["payload"]["headers"]},
    }
    gmail.fetched.clear()
    cache.set_state("backfill.window_start", "2026-05-01")
    result = sync.backfill_batch(batch_size=1, validate=True)
    assert result["metadata_checked"] == 1
    assert result["changed"] == 1
    assert result["hydrated"] == 1
    assert ("m1", "full") in gmail.fetched
    assert cache.get_message("m1")["snippet"] == "Changed Apollo"
    cache.close()


def test_backfill_advances_dead_intelligence_jobs(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    sync.hydrate_message("m1", update_history_cursor=False)
    with cache._connect() as conn:
        conn.execute("UPDATE intelligence_jobs SET max_attempts = 1 WHERE kind IN ('message', 'attachment')")
    cache.set_state(
        "backfill.current_batch",
        json.dumps(
            {
                "complete": False,
                "query": "in:anywhere after:2026/04/30 before:2026/06/01",
                "window_start": "2026-05-01",
                "window_end": "2026-06-01",
                "page_token": "",
                "next_page_token": "",
                "message_ids": ["m1"],
                "started_at": "2026-05-24T00:00:00Z",
            }
        ),
    )
    failing_sync = SyncService(
        config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=FailingSemantic(), gmail=gmail
    )

    result = failing_sync.backfill_batch(batch_size=1)

    assert result["advanced"] is True
    assert sorted(item["kind"] for item in result["target_status"]["dead"]) == ["attachment", "message"]
    assert cache.get_state("backfill.current_batch") == ""
    assert cache.get_state("backfill.window_start") == "2026-06-01"
    cache.close()


def test_search_stays_cache_only_for_time_bounded_queries(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(gmail_fixture())
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    gmail = FakeGmail({"m2": gmail_fixture_two()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("Apollo", after="2026-05-01", limit=5)
    assert result["source"] == "cache"
    assert gmail.queries == []
    assert result["messages"][0]["id"] == "m1"
    assert result["analysis"]["pending"] is True
    assert cache.intelligence_job_status()["pending"] >= 1
    assert should_search_gmail("Apollo") is True
    assert should_search_gmail("Apollo", after="2026-05-01") is False
    assert should_search_gmail("Apollo after:2026-05-01") is True
    assert should_search_gmail("before:2020/01/01") is True
    assert should_search_gmail("older_than:1y") is True
    cache.close()


def test_operator_only_gmail_query_goes_live(tmp_path: Path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=RuntimeConfig(), cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("older_than:1y", limit=5)
    assert result["source"] == "gmail+cache"
    assert gmail.queries == ["older_than:1y"]
    assert result["messages"][0]["id"] == "m1"
    cache.close()


def test_semantic_chunking_splits_long_text() -> None:
    chunks = chunk_text(" ".join(f"token{i}" for i in range(30)), chunk_size=10, overlap=2)
    assert len(chunks) > 1
    assert all(chunk.strip() for chunk in chunks)
