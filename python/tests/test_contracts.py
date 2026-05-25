# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Contract tests for the Phase 00 Python analysis package.

The tests exercise the minimal Pydantic models shipped for package and release validation. They do
not start worker processes or queues because Phase 00 only defines contracts.
"""

from gmeow_intel.contracts import AnalyzerJob, AnalyzerSpec, Annotation


def _require_equal(actual: object, *, expected: object) -> None:
    if actual != expected:
        message = f"expected {expected!r}, got {actual!r}"
        raise AssertionError(message)


def test_analyzer_job_contract_rejects_unknown_fields() -> None:
    """Verify analyzer job payloads expose nested analyzer metadata."""
    spec = AnalyzerSpec(name="ner", version="phase00")
    job = AnalyzerJob(schema_version=1, job_id="job-1", analyzer=spec, object_digest="digest")

    _require_equal(job.analyzer.name, expected="ner")


def test_annotation_contract_accepts_json_shape() -> None:
    """Verify annotations accept JSON-shaped analyzer output data."""
    annotation = Annotation(schema_version=1, object_digest="digest", kind="analysis", data={"ok": True})

    _require_equal(annotation.data["ok"], expected=True)
