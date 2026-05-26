# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Categorization external analyzer for the ANALYSIS phase.

The preferred production path is a configured sklearn model. This module provides a deterministic
fallback adapter so Go workers can execute the external analyzer contract without taking ownership
of category quality.
"""

from gmeow_intel.contracts import Annotation, ExternalCommandRequest

CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "finance": ("invoice", "payment", "bank", "statement", "receipt", "tax"),
    "travel": ("flight", "hotel", "booking", "reservation", "itinerary"),
    "security": ("password", "login", "mfa", "security", "alert"),
    "work": ("meeting", "project", "deadline", "review", "contract"),
}
MAX_KEYWORD_SCORE_HITS = 3


def analyzer_name() -> str:
    """Return the stable analyzer name."""
    return "categories.sklearn"


def analyze(request: ExternalCommandRequest) -> Annotation:
    """Return deterministic category hints for the request text."""
    text = request.text.lower()
    scores: list[dict[str, object]] = []
    for category, keywords in CATEGORY_KEYWORDS.items():
        hits = sorted({keyword for keyword in keywords if keyword in text})
        if hits:
            scores.append(
                {
                    "category": category,
                    "score": min(1.0, len(hits) / MAX_KEYWORD_SCORE_HITS),
                    "matched_terms": hits,
                }
            )

    scores.sort(key=category_sort_key)

    return Annotation(
        schema_version=request.schema_version,
        object_digest=request.job.object_digest,
        kind="analysis",
        analyzer_name=request.job.analyzer.name,
        analyzer_version=request.job.analyzer.version,
        data={
            "status": "complete",
            "categories": scores,
            "implementation": "python.keyword_fallback",
        },
    )


def category_sort_key(item: dict[str, object]) -> tuple[float, str]:
    """Sort category scores by descending score, then stable category name."""
    raw_score = item["score"]
    if not isinstance(raw_score, int | float):
        raw_score = 0.0

    return -float(raw_score), str(item["category"])
