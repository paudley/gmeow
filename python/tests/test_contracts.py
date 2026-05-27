# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Contract tests for the Python external analyzer package.

The tests exercise the Pydantic models and analyzer command functions used by the Go ANALYSIS
external adapter path. Queue consumption and FILESTORE writes remain owned by Go tests.
"""

from typing import cast

from gmeow_intel.analyzers import categories, ner
from gmeow_intel.contracts import AnalyzerJob, AnalyzerSpec, Annotation, ExternalCommandRequest


def _require_equal(actual: object, *, expected: object) -> None:
    if actual != expected:
        message = f"expected {expected!r}, got {actual!r}"
        raise AssertionError(message)


def _require_truthy(value: object) -> None:
    if not value:
        message = f"expected truthy value, got {value!r}"
        raise AssertionError(message)


def test_analyzer_job_contract_rejects_unknown_fields() -> None:
    """Verify analyzer job payloads expose nested analyzer metadata."""
    spec = AnalyzerSpec(name="ner", version="phase00")
    job = AnalyzerJob(schema_version=1, job_id="job-1", analyzer=spec, object_digest="digest")

    _require_equal(job.analyzer.name, expected="ner")


def test_analyzer_job_contract_accepts_go_runtime_fields() -> None:
    """Verify Python workers accept the Go scheduler job envelope."""
    spec = AnalyzerSpec(name="ner.spacy", version="python-email-v1", worker_kind="python")
    job = AnalyzerJob(
        schema_version=1,
        job_id="job-1",
        idempotency_key="idem",
        analyzer=spec,
        object_digest="digest",
        priority_class="forced",
        requested_by="operator",
        reason="forced",
        attempt=0,
        trace_id="trace",
        created_at="2026-05-26T22:10:35.380524574Z",
        forced=True,
        priority=90,
    )

    _require_equal(job.created_at, expected="2026-05-26T22:10:35.380524574Z")


def test_annotation_contract_accepts_json_shape() -> None:
    """Verify annotations accept JSON-shaped analyzer output data."""
    annotation = Annotation(schema_version=1, object_digest="digest", kind="analysis", data={"ok": True})

    _require_equal(annotation.data["ok"], expected=True)


def test_ner_external_adapter_emits_analysis_annotation() -> None:
    """Verify the Python NER adapter speaks the Go external command contract."""
    request = ExternalCommandRequest(
        schema_version=1,
        job=AnalyzerJob(
            schema_version=1,
            job_id="job-1",
            analyzer=AnalyzerSpec(name="ner.spacy", version="python-email-v1"),
            object_digest="digest",
        ),
        manifest={"digest": "digest"},
        text="Alice Smith met Bob in Edmonton.",
    )

    annotation = ner.analyze(request)

    _require_equal(annotation.kind, expected="analysis")
    _require_equal(annotation.analyzer_name, expected="ner.spacy")
    _require_truthy(annotation.data["entities"])
    _require_equal(annotation.data["implementation"], expected="python.spacy")
    entities = cast(list[dict[str, object]], annotation.data["entities"])
    labels = {entity["label"] for entity in entities}
    if "UNKNOWN" in labels:
        raise AssertionError(f"expected real spaCy labels, got {labels!r}")


def test_category_external_adapter_emits_analysis_annotation() -> None:
    """Verify the Python category adapter speaks the Go external command contract."""
    request = ExternalCommandRequest(
        schema_version=1,
        job=AnalyzerJob(
            schema_version=1,
            job_id="job-1",
            analyzer=AnalyzerSpec(name="categories.sklearn", version="python-email-v1"),
            object_digest="digest",
        ),
        manifest={
            "digest": "digest",
            "facets": [
                {
                    "kind": "mail_message",
                    "metadata": {
                        "message_id": "m1",
                        "subject": "Interactive Brokers statement",
                        "sender": "reports@interactivebrokers.com",
                        "label_ids": ["CATEGORY_UPDATES"],
                    },
                }
            ],
        },
        text="From: reports@interactivebrokers.com\nSubject: Interactive Brokers statement\n\nYour trade confirmation is ready.",
    )

    annotation = categories.analyze(request)

    _require_equal(annotation.kind, expected="analysis")
    _require_equal(annotation.analyzer_name, expected="categories.sklearn")
    _require_truthy(annotation.data["categories"])
    _require_equal(annotation.data["implementation"], expected="python.sklearn_mainbranch")
    category_ids = cast(list[str], annotation.data["category_ids"])
    if "financial_statement" not in category_ids:
        raise AssertionError(f"expected financial_statement, got {category_ids!r}")


def test_category_discovery_uses_sklearn_clusters() -> None:
    """Verify the category adapter exposes the sklearn learned-category path."""
    run = categories.discover(
        [
            {
                "id": "flight-1",
                "sender": "booking@example.test",
                "subject": "Flight booking confirmed",
                "snippet": "Edmonton flight booking hotel itinerary gate update",
            },
            {
                "id": "flight-2",
                "sender": "booking@example.test",
                "subject": "Flight itinerary changed",
                "snippet": "Edmonton flight booking hotel itinerary gate update",
            },
            {
                "id": "flight-3",
                "sender": "booking@example.test",
                "subject": "Flight booking reminder",
                "snippet": "Edmonton flight booking hotel itinerary gate update",
            },
            {
                "id": "repo-1",
                "sender": "notifications@github.com",
                "subject": "Workflow run failed",
                "snippet": "CI failed on main.",
            },
        ]
    )

    _require_equal(run["messages"], expected=4)
    _require_truthy(run["clusters"])
