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


class AnalyzerJob(BaseModel):
    """Describe one analyzer job delivered to a Python worker."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int
    job_id: str
    analyzer: AnalyzerSpec
    object_digest: str
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
