# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Shared Python-side contracts for analysis payload validation.

These models mirror the JSON-shaped contracts introduced by the Go Phase 00 foundation. They are
kept deliberately minimal until the ANALYSIS phase defines queue transport and analyzer execution.

See Also:
    MODULE.md: Cross-language contract expectations for analyzer payloads.

"""

from .models import AnalyzerJob, AnalyzerSpec, Annotation

__all__ = ["AnalyzerJob", "AnalyzerSpec", "Annotation"]
