# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import base64
import json
import os
from dataclasses import asdict
from pathlib import Path

import psycopg
import pytest

from gmeow.categories import CategoryEngine, deterministic_assignments, message_document
from gmeow.cache import categorize_message
from gmeow.config import GmeowConfig, PriorityRule
from gmeow.graph import DOAP, FOAF, RDF_TYPE, SCHEMA, extract_triples
from gmeow.graph import extract_attachment_sidecar_triples
from gmeow.intelligence import IntelligenceWorker
from gmeow.kg import spacy_entities
from gmeow.markdown import message_to_markdown
from gmeow.mcp_server import _format, enriched_message, enriched_thread
from gmeow.object_store import CasAttachmentStore, ObjectStore
from gmeow.parser import parse_gmail_message
from gmeow.pg_cache import PgCache
from gmeow.semantic import chunk_text
from gmeow.sync import SyncService, message_matches_rule, should_search_gmail
from gmeow.text_index import TantivyMessageIndex
from gmeow.toon import dumps as toon_dumps


TEST_DSN = os.environ.get("GMEOW_TEST_POSTGRES_DSN")


def test_gmeow_config_loads_namespaced_toml(tmp_path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[gmeow]
data_dir = "cache"
subject = "user@example.com"
postgres_dsn = "postgresql://gmeow:change-me@127.0.0.1:5432/gmeow"

[gmeow.maintenance]
enabled = false

[gmeow.secrets]
file = "~/.config/gmeow/secrets.sops.yaml"
unlock_key = "test-key"
age_key = "AGE-SECRET-KEY-TEST"

[[gmeow.priority_rules]]
name = "recent"
gmail_query = "newer_than:7d"
priority = 5
""".strip()
    )

    config = GmeowConfig.load(config_path)

    assert config.data_dir == Path("cache")
    assert config.subject == "user@example.com"
    assert config.maintenance.enabled is False
    assert str(config.secrets.file).endswith(".config/gmeow/secrets.sops.yaml")
    assert config.secrets.unlock_key == "test-key"
    assert config.priority_rules[0].name == "recent"


@pytest.fixture(autouse=True)
def clean_pg_test_rows():
    cleanup_pg()
    yield
    cleanup_pg()


def cleanup_pg() -> None:
    if not TEST_DSN:
        return
    with psycopg.connect(TEST_DSN) as conn:
        for table in [
            "embedding_chunks",
            "ingest_issues",
            "intelligence_jobs",
            "graph_triples",
            "attachments",
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
        ]:
            conn.execute(f"DELETE FROM {table}")
        try:
            conn.execute("SET search_path=ag_catalog, public")
            conn.execute("SELECT drop_graph('gmeow_graph', true)")
            conn.execute("SELECT create_graph('gmeow_graph')")
        except Exception:
            pass


def make_cache(tmp_path) -> PgCache:
    if not TEST_DSN:
        pytest.skip("GMEOW_TEST_POSTGRES_DSN is required for Postgres integration tests")
    cache = PgCache(TEST_DSN, ObjectStore(tmp_path / "objects"), TantivyMessageIndex(tmp_path / "tantivy"))
    cache.ensure_default_categories()
    return cache


def make_attachments(tmp_path) -> CasAttachmentStore:
    return CasAttachmentStore(ObjectStore(tmp_path / "objects"))


def b64(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def gmail_fixture() -> dict:
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


def gmail_fixture_two() -> dict:
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


def unifi_fixture() -> dict:
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
    value["payload"]["parts"][0]["body"]["data"] = b64(
        "G4 Instant: 1 Smart Detection Link: https://unifi.ui.com/protect/events/event/123"
    )
    return value


def ringcentral_fixture() -> dict:
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


def github_fixture(message_id: str = "g1", subject: str = "Re: [example-org/example-project] Harden/core managed git e2e (PR #163)") -> dict:
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
    assert any(predicate == DOAP + "repository" and obj == "https://github.com/example-org/example-project" for _, predicate, obj, _ in triples)
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


def test_attachment_store_merges_sidecar(tmp_path) -> None:
    store = make_attachments(tmp_path)
    first = store.put(b"hello", {"gmail": {"filename": "a.txt"}}, extract_metadata=False)
    first.sidecar_path.write_text(json.dumps({"external_summary": "added elsewhere", "source": {"gmail": {"filename": "a.txt"}}}))
    second = store.put(b"hello", {"gmail": {"mime_type": "text/plain"}}, extract_metadata=False)
    assert second.sha1 == first.sha1
    assert second.metadata["external_summary"] == "added elsewhere"
    assert second.metadata["source"]["gmail"]["filename"] == "a.txt"
    assert second.metadata["source"]["gmail"]["mime_type"] == "text/plain"


def test_attachment_store_adds_exiftool_metadata(tmp_path) -> None:
    store = make_attachments(tmp_path)
    stored = store.put(b"hello", {"gmail": {"filename": "a.txt"}})
    assert stored.metadata["source"]["gmail"]["filename"] == "a.txt"
    assert "exiftool" in stored.metadata
    assert "extracted_at" in stored.metadata["exiftool"]


def test_cache_offline_message_search(tmp_path) -> None:
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


def test_compact_search_results_and_thread_order(tmp_path) -> None:
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


def test_message_categories_exclude_camera_alerts_by_default(tmp_path) -> None:
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


def test_category_engine_persists_system_and_manual_categories(tmp_path) -> None:
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


def test_seed_initial_categories_adds_manual_rules(tmp_path) -> None:
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


def test_category_discovery_uses_sklearn_clusters(tmp_path) -> None:
    cache = make_cache(tmp_path)
    raws = [github_fixture("g1"), github_fixture("g2", "Re: [example-org/example-project] Add output surface report (PR #162)")]
    raws += [unifi_fixture(), ringcentral_fixture()]
    for raw in raws:
        parsed = parse_gmail_message(raw)
        cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
        cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)
    run = CategoryEngine(cache).discover(since_hours=None)
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


def test_attachment_sidecar_text_search(tmp_path) -> None:
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
            "path": "/tmp/invoice.pdf",
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
            "path": "/tmp/scan.png",
        }
    )
    message_attachments = cache.attachment_text_for_message("m2")
    assert message_attachments[0]["text"] == "A scanned permit approval letter."
    assert "metadata" not in cache.attachment_text_for_message("m2", include_metadata=False)[0]
    cache.close()


def test_mcp_message_enrichment_omits_attachment_paths(tmp_path) -> None:
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
            "path": "/tmp/invoice.pdf",
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


def test_graph_discovery_helpers(tmp_path) -> None:
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


def test_project_ranking_and_ontology_profile(tmp_path) -> None:
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


def test_age_status_and_read_only_cypher(tmp_path) -> None:
    cache = make_cache(tmp_path)
    cache.add_triples([("gmeow:message/m1", "gmeow:mentionsEntity", "gmeow:entity/Apollo", "m1")])
    status = cache.age_status()
    assert status["available"] is True
    rows = cache.age_cypher("MATCH (n) RETURN count(n)", columns="count agtype")
    assert rows
    try:
        cache.age_cypher("CREATE (n)")
    except ValueError:
        pass
    else:
        raise AssertionError("mutating Cypher should be rejected")


def test_ingest_issue_records_artifact(tmp_path) -> None:
    cache = make_cache(tmp_path)
    issue_id = cache.record_ingest_issue("bad1", "gmail.full", "error", "bad payload", {"id": "bad1", "payload": None})
    issues = cache.list_ingest_issues()
    assert issues[0]["id"] == issue_id
    assert issues[0]["message_id"] == "bad1"
    assert issues[0]["artifact_digest"]
    assert cache.sync_status()["counts"]["ingest_issues"] == 1


def test_contacts_resolve_address_variants(tmp_path) -> None:
    cache = make_cache(tmp_path)
    first_raw = gmail_fixture()
    second_raw = gmail_fixture_two()
    for header in second_raw["payload"]["headers"]:
        if header["name"] == "From":
            header["value"] = "Alice Example <alice@example.com>"
    for raw in [first_raw, second_raw]:
        parsed = parse_gmail_message(raw)
        cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
        cache.upsert_message(parsed, raw, markdown=message_to_markdown(parsed), hydrated=True)
    contacts = cache.contacts()
    alice = next(contact for contact in contacts if contact["address"] == "alice@example.com")
    assert alice["messages"] == 2
    assert alice["display_name"] == "Alice Example"
    assert set(alice["names"]) == {"Alice", "Alice Example"}
    alias_raw = gmail_fixture()
    alias_raw["id"] = "m3"
    for header in alias_raw["payload"]["headers"]:
        if header["name"] == "From":
            header["value"] = "Alice Example <alice@work.example>"
    parsed = parse_gmail_message(alias_raw)
    cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
    cache.upsert_message(parsed, alias_raw, markdown=message_to_markdown(parsed), hydrated=True)
    people = cache.people()
    alice_person = next(person for person in people if person["name"] == "Alice Example")
    assert alice_person["addresses"] == ["alice@example.com", "alice@work.example"]
    cache.upsert_people_alias("a.smith@example.net", "Alice Example", display_name="Alice Example")
    alias_raw = gmail_fixture()
    alias_raw["id"] = "m4"
    for header in alias_raw["payload"]["headers"]:
        if header["name"] == "From":
            header["value"] = "A Smith <a.smith@example.net>"
    parsed = parse_gmail_message(alias_raw)
    cache.upsert_thread({"id": parsed.thread_id or parsed.gmail_id, "snippet": parsed.snippet})
    cache.upsert_message(parsed, alias_raw, markdown=message_to_markdown(parsed), hydrated=True)
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
    def __init__(self):
        self.messages = []
        self.attachments = []

    def index_message(self, message_id, text, metadata=None):
        self.messages.append((message_id, text, metadata))

    def index_attachment(self, sha1, text, metadata=None):
        self.attachments.append((sha1, text, metadata))


class FakeGmail:
    def __init__(self, messages):
        self.messages = messages
        self.queries = []
        self.fetched = []
        self.modified = []

    def list_labels(self):
        return [{"id": "INBOX", "name": "Inbox", "type": "system"}]

    def search_messages(self, query, limit=100):
        self.queries.append(query)
        return list(self.messages)[:limit]

    def list_history(self, start_history_id, label_id=None, limit=500):
        return {
            "history_id": "200",
            "history": [
                {"id": "101", "messagesAdded": [{"message": {"id": "m2"}}]},
                {"id": "102", "labelsAdded": [{"message": {"id": "m1"}}]},
            ],
        }

    def get_message(self, message_id, fmt="full"):
        self.fetched.append((message_id, fmt))
        if fmt == "raw":
            return {"id": message_id, "raw": b64("Subject: Apollo\r\n\r\nBody")}
        return self.messages[message_id]

    def get_thread(self, thread_id):
        return {}

    def get_attachment(self, message_id, attachment_id):
        return b""

    def modify_message(self, message_id, add_label_ids=None, remove_label_ids=None):
        self.modified.append({"message_id": message_id, "add": add_label_ids or [], "remove": remove_label_ids or []})
        return {"id": message_id}


def test_intelligence_worker_runs_until_empty(tmp_path) -> None:
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
            "path": "/tmp/invoice.pdf",
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


def test_search_uses_live_gmail_for_unbounded_queries(tmp_path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=None, cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("Apollo", limit=5)
    assert result["source"] == "gmail+cache"
    assert gmail.queries == ["Apollo"]
    assert result["messages"][0]["id"] == "m1"
    assert len(result["messages"]) == result["live"]["hydrated"]
    assert cache.get_message("m1") is not None
    assert cache.intelligence_job_status()["pending"] >= 1
    cache.close()


def test_live_search_returns_default_hidden_messages_inline(tmp_path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"u1": unifi_fixture()})
    sync = SyncService(config=None, cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("UniFi", limit=5)
    assert result["source"] == "gmail+cache"
    assert result["live"]["hydrated"] == 1
    assert result["messages"][0]["id"] == "u1"
    assert "camera_alert" in result["messages"][0]["categories"]


def test_live_search_prefers_cached_analyzed_message(tmp_path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(gmail_fixture())
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    cache.apply_category("m1", "personal", reason="already analyzed")
    gmail = FakeGmail({"m1": gmail_fixture_two()})
    sync = SyncService(config=None, cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("Apollo", limit=5)
    assert result["messages"][0]["id"] == "m1"
    assert result["messages"][0]["subject"] == "Apollo Invoice"
    assert "personal" in result["messages"][0]["categories"]
    assert gmail.fetched == []
    cache.close()


def test_hydrate_applies_manual_rules_and_star_raw_helpers(tmp_path) -> None:
    cache = make_cache(tmp_path)
    CategoryEngine(cache).seed_initial_categories()
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=None, cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
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


def test_history_sync_hydrates_changed_messages(tmp_path) -> None:
    cache = make_cache(tmp_path)
    cache.set_state("gmail_history_id", "100")
    gmail = FakeGmail({"m1": gmail_fixture(), "m2": gmail_fixture_two()})
    sync = SyncService(config=None, cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.sync_history(limit=10)
    assert result["messages"] == 2
    assert result["hydrated"] == 2
    assert cache.get_message("m2") is not None
    assert cache.get_state("gmail_history_id") == "200"
    cache.close()


def test_search_stays_cache_only_for_time_bounded_queries(tmp_path) -> None:
    cache = make_cache(tmp_path)
    parsed = parse_gmail_message(gmail_fixture())
    cache.upsert_thread({"id": "t1", "snippet": parsed.snippet})
    cache.upsert_message(parsed, gmail_fixture(), markdown=message_to_markdown(parsed), hydrated=True)
    gmail = FakeGmail({"m2": gmail_fixture_two()})
    sync = SyncService(config=None, cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("Apollo", after="2026-05-01", limit=5)
    assert result["source"] == "cache"
    assert gmail.queries == []
    assert result["messages"][0]["id"] == "m1"
    assert should_search_gmail("Apollo") is True
    assert should_search_gmail("Apollo", after="2026-05-01") is False
    assert should_search_gmail("Apollo after:2026-05-01") is True
    assert should_search_gmail("before:2020/01/01") is True
    assert should_search_gmail("older_than:1y") is True
    cache.close()


def test_operator_only_gmail_query_goes_live(tmp_path) -> None:
    cache = make_cache(tmp_path)
    gmail = FakeGmail({"m1": gmail_fixture()})
    sync = SyncService(config=None, cache=cache, attachments=make_attachments(tmp_path), semantic=RecordingSemantic(), gmail=gmail)
    result = sync.search("older_than:1y", limit=5)
    assert result["source"] == "gmail+cache"
    assert gmail.queries == ["older_than:1y"]
    assert result["messages"][0]["id"] == "m1"
    cache.close()


def test_semantic_chunking_splits_long_text() -> None:
    chunks = chunk_text(" ".join(f"token{i}" for i in range(30)), chunk_size=10, overlap=2)
    assert len(chunks) > 1
    assert all(chunk.strip() for chunk in chunks)
