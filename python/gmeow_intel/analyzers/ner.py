# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""spaCy-backed NER external analyzer for the ANALYSIS phase.

This module loads the configured spaCy English model and emits real entity labels.
It is intentionally a narrow adapter so Go remains responsible for FILESTORE writes.
"""

from functools import cache
from typing import Protocol, cast

import spacy

from gmeow_intel.contracts import Annotation, ExternalCommandRequest

MAX_INPUT_CHARS = 100_000
MIN_ENTITY_LENGTH = 2


class _EntityLike(Protocol):
    text: str
    label_: str
    start_char: int
    end_char: int


class _DocLike(Protocol):
    ents: tuple[_EntityLike, ...]


class _NLPLike(Protocol):
    def __call__(self, text: str) -> _DocLike: ...


def analyzer_name() -> str:
    """Return the stable analyzer name."""
    return "ner.spacy"


def analyze(request: ExternalCommandRequest) -> Annotation:
    """Return real spaCy named entities for the request text."""
    text = request.text.strip()[:MAX_INPUT_CHARS]
    entities: list[dict[str, object]] = []
    seen: set[tuple[str, str, int, int]] = set()
    for ent in _nlp()(text).ents:
        value = ent.text.strip()
        if len(value) < MIN_ENTITY_LENGTH:
            continue
        key = (value.lower(), ent.label_, ent.start_char, ent.end_char)
        if key in seen:
            continue
        seen.add(key)
        entities.append(
            {
                "text": value,
                "label": ent.label_,
                "start": ent.start_char,
                "end": ent.end_char,
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
            "implementation": "python.spacy",
            "model": "en_core_web_sm",
        },
    )


def warmup() -> None:
    """Load the spaCy model so a persistent backend is ready before serving."""
    _nlp()


@cache
def _nlp() -> _NLPLike:
    try:
        return cast(_NLPLike, spacy.load("en_core_web_sm"))
    except OSError as exc:
        msg = "ner.spacy requires the en_core_web_sm spaCy model"
        raise RuntimeError(msg) from exc
