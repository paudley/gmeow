# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Minimal JSON-shaped contracts mirrored from the Go Phase 00 contracts.

The models validate analyzer specifications, scheduled jobs, and emitted annotations at package
boundaries. They reject unknown fields so future worker behavior cannot silently accept drift from
the Go scheduler contract.
"""

from pydantic import BaseModel, ConfigDict, Field


class AnalyzerSpec(BaseModel):
    """Describe an analyzer implementation selected by the scheduler."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str
    enabled: bool = True
    worker_kind: str = "python"
    media_types: list[str] = Field(default_factory=list)
    content_roles: list[str] = Field(default_factory=list)
    required_inputs: list[str] = Field(default_factory=list)
    output_sections: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    priority: int = 0
    idempotency_key_formula: str = ""
    deterministic: bool = False


class AnalyzerJob(BaseModel):
    """Describe one analyzer job delivered to a Python worker."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int
    job_id: str
    idempotency_key: str = ""
    analyzer: AnalyzerSpec
    object_digest: str
    priority_class: str = ""
    requested_by: str = ""
    reason: str = ""
    attempt: int = 0
    trace_id: str = ""
    deadline: str = ""
    forced: bool = False
    priority: int = 0


class Annotation(BaseModel):
    """Describe an analyzer output annotation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int
    object_digest: str
    kind: str
    analyzer_name: str = ""
    analyzer_version: str = ""
    data: dict[str, object] = Field(default_factory=dict)


class ExternalCommandRequest(BaseModel):
    """Describe the JSON request sent by the Go external analyzer adapter."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int
    job: AnalyzerJob
    manifest: dict[str, object]
    text: str = ""
