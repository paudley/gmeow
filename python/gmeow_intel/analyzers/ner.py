# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""NER external analyzer for the ANALYSIS phase.

The preferred production path is a configured Python NLP stack. This module deliberately keeps a
deterministic fallback so the Go external-adapter contract remains executable in lean environments.
"""

import re

from gmeow_intel.contracts import Annotation, ExternalCommandRequest

ENTITY_PATTERN = re.compile(r"\b[A-Z][A-Za-z0-9_.-]*(?:\s+[A-Z][A-Za-z0-9_.-]*){0,3}\b")
MIN_ENTITY_LENGTH = 2


def analyzer_name() -> str:
    """Return the stable analyzer name."""
    return "ner.spacy"


def analyze(request: ExternalCommandRequest) -> Annotation:
    """Return named entity candidates for the request text."""
    text = request.text.strip()
    entities: list[dict[str, object]] = []
    seen: set[str] = set()
    for match in ENTITY_PATTERN.finditer(text):
        value = match.group(0).strip()
        if len(value) < MIN_ENTITY_LENGTH or value in seen:
            continue
        seen.add(value)
        entities.append(
            {
                "text": value,
                "label": "UNKNOWN",
                "start": match.start(),
                "end": match.end(),
            }
        )

    return Annotation(
        schema_version=request.schema_version,
        object_digest=request.job.object_digest,
        kind="analysis",
        analyzer_name=request.job.analyzer.name,
        analyzer_version=request.job.analyzer.version,
        data={
            "status": "complete",
            "entities": entities,
            "implementation": "python.regex_fallback",
        },
    )
