# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Build knowledge-graph triples from messages and attachment sidecars.

This module turns parsed mail content and sidecar analysis output into RDF-style triples that
downstream consumers persist into the graph store. It owns the entity extraction, ontology mapping,
and per-section text harvesting that keep the graph aligned with the underlying mail corpus.
"""

import json
import re
from functools import lru_cache
from html import unescape
from typing import Any, cast

import spacy

from ._typing import ensure_dict

Triple = tuple[str, str, str, str]

MIN_ENTITY_TEXT_LENGTH = 2
MAX_CLEAN_TOKEN_LENGTH = 100
MAX_ENTITY_TEXT_LENGTH = 120


SPACY_ENTITY_TYPES = {
    "PERSON": "person",
    "ORG": "org",
    "GPE": "place",
    "LOC": "place",
    "PRODUCT": "product",
    "EVENT": "event",
    "WORK_OF_ART": "work",
    "LAW": "law",
    "LANGUAGE": "language",
    "FAC": "facility",
    "NORP": "group",
    "DATE": "date",
    "TIME": "time",
    "MONEY": "money",
    "PERCENT": "percent",
    "QUANTITY": "quantity",
    "ORDINAL": "ordinal",
    "CARDINAL": "cardinal",
}

STOP_ENTITY_VALUES = {
    "doctype",
    "html",
    "head",
    "body",
    "style",
    "script",
    "href",
    "class",
    "nbsp",
    "unread",
    "inbox",
    "important",
    "starred",
    "sent",
    "draft",
    "spam",
    "trash",
    "category_forums",
    "category_updates",
    "category_promotions",
    "category_social",
    "category_personal",
    "view",
    "click",
    "open",
    "close",
    "submit",
    "cancel",
    "save",
    "delete",
    "detection",
    "regards",
}


@lru_cache(maxsize=1)
def _nlp() -> spacy.language.Language:
    return spacy.load("en_core_web_sm")


def extract_message_kg(message_id: str, text: str) -> list[Triple]:
    """Extract message kg."""
    subject = _node("message", message_id)
    triples: list[Triple] = []
    clean = clean_text_for_kg(text)
    for entity in spacy_entities(clean):
        entity_node = _node(entity["kind"], entity["text"])
        triples.append((subject, f"gmeow:mentions/{entity['kind']}", entity_node, message_id))
        triples.append((entity_node, "rdf:type", f"gmeow:{entity['kind'].title()}", message_id))
        triples.append((entity_node, "gmeow:canonicalText", entity["text"], message_id))
    triples.extend((subject, "gmeow:mentionsUrl", _node("url", url), message_id) for url in extract_urls(clean))
    triples.extend((subject, "gmeow:mentionsEmail", _node("address", email), message_id) for email in extract_email_addresses(clean))
    return _dedupe(triples)


def extract_sidecar_kg(sha1: str, metadata: dict[str, Any]) -> list[Triple]:
    """Extract sidecar kg."""
    subject = _node("attachment", sha1)
    source = cast(dict[str, Any], metadata.get("source", {}))
    gmail = cast(dict[str, Any], source.get("gmail", {}))
    message = cast(dict[str, Any], source.get("message", {}))
    message_id = str(gmail.get("message_id") or "")
    message_node = _node("message", message_id) if message_id else ""
    triples: list[Triple] = [(subject, "rdf:type", "gmeow:Attachment", "")]
    if message_id:
        triples.append((message_node, "gmeow:hasAttachment", subject, message_id))
    triples.extend(_sidecar_mapped_triples(subject, gmail, message, metadata, message_id))
    analysis = _analysis_dict(metadata)
    triples.extend(_sidecar_analysis_triples(subject, message_node, message_id, sha1, analysis))
    searchable_text = json.dumps({"source": source, "exiftool": _exiftool_tags(metadata), "analysis": analysis}, sort_keys=True)
    triples.extend(extract_message_kg(f"attachment/{sha1}", searchable_text))
    return _dedupe(triples)


def _exiftool_tags(metadata: dict[str, Any]) -> dict[str, Any]:
    return ensure_dict(ensure_dict(metadata, "exiftool"), "tags")


def _analysis_dict(metadata: dict[str, Any]) -> dict[str, Any]:
    return ensure_dict(metadata, "analysis")


def _sidecar_mapped_triples(
    subject: str, gmail: dict[str, Any], message: dict[str, Any], metadata: dict[str, Any], message_id: str
) -> list[Triple]:
    triples: list[Triple] = []
    triples.extend(_mapped_triples(subject, gmail, message_id, _GMAIL_ATTACHMENT_FIELDS))
    triples.extend(_mapped_triples(subject, message, message_id, _SOURCE_MESSAGE_FIELDS))
    triples.extend(_mapped_triples(subject, _exiftool_tags(metadata), message_id, _EXIF_TAG_FIELDS, include_falsey=True))
    return triples


def _sidecar_analysis_triples(subject: str, message_node: str, message_id: str, sha1: str, analysis: dict[str, Any]) -> list[Triple]:
    triples: list[Triple] = []
    file_info = ensure_dict(analysis, "file")
    triples.extend(_mapped_triples(subject, file_info, message_id, _ANALYSIS_FILE_FIELDS))
    if analysis.get("available_text") is not None:
        triples.append((subject, "gmeow:analysisHasText", str(bool(analysis.get("available_text"))).lower(), message_id))
    document = ensure_dict(analysis, "document")
    triples.extend(_document_triples(subject, message_node, message_id, sha1, document))
    calendar = ensure_dict(analysis, "calendar")
    fields = ensure_dict(calendar, "fields")
    triples.extend(_calendar_triples(subject, message_node, message_id, sha1, fields))
    archive = ensure_dict(analysis, "archive")
    triples.extend(_archive_file_triples(subject, message_node, message_id, sha1, archive))
    extracted_sections: dict[str, str] = {
        "document": str(document.get("text") or ""),
        "content": str(analysis.get("content_text") or ""),
        "ocr": str(_section_text(analysis, ["image"], ["ocr_text"]) or analysis.get("ocr_text") or ""),
        "vision": str(_section_text(analysis, ["vision"], ["vision_caption"]) or analysis.get("vision_caption") or ""),
        "calendar": str(calendar.get("text") or ""),
    }
    for source_kind, text in extracted_sections.items():
        triples.extend(_attachment_text_entities(subject, message_node, message_id, source_kind, text))
    return triples


_GMAIL_ATTACHMENT_FIELDS = {
    "filename": "gmeow:attachmentFilename",
    "mime_type": "gmeow:attachmentMimeType",
    "size": "gmeow:attachmentSize",
}
_SOURCE_MESSAGE_FIELDS = {
    "subject": "gmeow:sourceMessageSubject",
    "from": "gmeow:sourceMessageFrom",
    "to": "gmeow:sourceMessageTo",
    "date": "gmeow:sourceMessageDate",
}
_EXIF_TAG_FIELDS = {
    "File:FileType": "gmeow:fileType",
    "File:MIMEType": "gmeow:fileMimeType",
    "File:ImageWidth": "gmeow:imageWidth",
    "File:ImageHeight": "gmeow:imageHeight",
    "PNG:ImageWidth": "gmeow:imageWidth",
    "PNG:ImageHeight": "gmeow:imageHeight",
    "EXIF:ImageWidth": "gmeow:imageWidth",
    "EXIF:ImageHeight": "gmeow:imageHeight",
    "PDF:Title": "gmeow:documentTitle",
    "PDF:Author": "gmeow:documentAuthor",
    "PDF:Creator": "gmeow:documentCreator",
    "PDF:Producer": "gmeow:documentProducer",
}
_ANALYSIS_FILE_FIELDS = {
    "media_type": "gmeow:analysisMediaType",
    "detected_media_type": "gmeow:detectedMimeType",
    "file_description": "gmeow:fileDescription",
    "extension": "gmeow:fileExtension",
}
_DOCUMENT_FIELDS = {
    "title": "gmeow:documentTitle",
    "author": "gmeow:documentAuthor",
    "pages": "gmeow:documentPages",
    "page_size": "gmeow:documentPageSize",
    "producer": "gmeow:documentProducer",
    "creator": "gmeow:documentCreator",
    "pdf_version": "gmeow:documentPdfVersion",
}
_CALENDAR_FIELDS = {
    "summary": "gmeow:calendarSummary",
    "location": "gmeow:calendarLocation",
    "dtstart": "gmeow:calendarStart",
    "dtend": "gmeow:calendarEnd",
    "organizer": "gmeow:calendarOrganizer",
    "attendee": "gmeow:calendarAttendee",
}


def _mapped_triples(
    subject: str, values: dict[str, Any], message_id: str, mapping: dict[str, str], *, include_falsey: bool = False
) -> list[Triple]:
    return [
        (subject, predicate, str(values[key]), message_id)
        for key, predicate in mapping.items()
        if key in values and (include_falsey or values.get(key))
    ]


def _document_triples(subject: str, message_node: str, message_id: str, sha1: str, document: dict[str, Any]) -> list[Triple]:
    if not document:
        return []
    document_node = _node("document", sha1)
    triples = [
        (subject, "gmeow:hasDocument", document_node, message_id),
        (document_node, "rdf:type", "gmeow:Document", message_id),
    ]
    if message_node:
        triples.append((message_node, "gmeow:hasAttachmentDocument", document_node, message_id))
    for triple in _mapped_triples(subject, document, message_id, _DOCUMENT_FIELDS):
        triples.append(triple)
        triples.append((document_node, triple[1], triple[2], message_id))
    if document.get("author"):
        author_node = _node("documentAuthor", str(document["author"]))
        triples.extend(
            [
                (subject, "gmeow:hasDocumentAuthor", author_node, message_id),
                (author_node, "gmeow:canonicalText", str(document["author"]), message_id),
            ]
        )
        if message_node:
            triples.append((message_node, "gmeow:attachmentMentions", author_node, message_id))
    return triples


def _calendar_triples(subject: str, message_node: str, message_id: str, sha1: str, fields: dict[str, Any]) -> list[Triple]:
    if not fields:
        return []
    event_identity = _first_field(fields, "uid") or "|".join([_first_field(fields, "summary"), _first_field(fields, "dtstart"), sha1])
    event_node = _node("event", event_identity)
    triples = [(subject, "gmeow:hasCalendarEvent", event_node, message_id), (event_node, "rdf:type", "gmeow:Event", message_id)]
    if message_node:
        triples.append((message_node, "gmeow:hasAttachmentEvent", event_node, message_id))
    for key, predicate in _CALENDAR_FIELDS.items():
        for value in fields.get(key, [])[:25]:
            triples.extend(
                _calendar_value_triples(
                    {"subject": subject, "message_node": message_node, "message_id": message_id, "event_node": event_node},
                    key,
                    predicate,
                    str(value),
                )
            )
    return triples


def _calendar_value_triples(context: dict[str, str], key: str, predicate: str, value: str) -> list[Triple]:
    subject = str(context["subject"])
    message_node = context["message_node"]
    message_id = context["message_id"]
    event_node = str(context["event_node"])
    triples = [(subject, predicate, value, message_id), (event_node, predicate, value, message_id)]
    if key in {"attendee", "organizer"}:
        attendee_node = _node("calendarAttendee", value)
        triples.extend(
            [
                (subject, "gmeow:hasCalendarParticipant", attendee_node, message_id),
                (event_node, "gmeow:hasParticipant", attendee_node, message_id),
            ]
        )
        if message_node:
            triples.append((message_node, "gmeow:attachmentMentions", attendee_node, message_id))
    return triples


def _archive_file_triples(subject: str, message_node: str, message_id: str, sha1: str, archive: dict[str, Any]) -> list[Triple]:
    triples: list[Triple] = []
    for filename in archive.get("files", [])[:100]:
        child = _node("archiveFile", f"{sha1}/{filename}")
        triples.extend(
            [
                (subject, "gmeow:containsFile", child, message_id),
                (child, "rdf:type", "gmeow:ArchiveFile", message_id),
                (child, "gmeow:filename", str(filename), message_id),
            ]
        )
        extension = _extension(filename)
        if extension:
            triples.append((child, "gmeow:fileExtension", extension, message_id))
        if message_node:
            triples.append((message_node, "gmeow:attachmentContainsFile", child, message_id))
    return triples


def _attachment_text_entities(attachment_node: str, message_node: str, message_id: str, source_kind: str, text: str) -> list[Triple]:
    if not text:
        return []
    triples: list[Triple] = []
    source_node = _node("attachmentEvidence", f"{attachment_node}/{source_kind}")
    triples.append((attachment_node, "gmeow:hasAttachmentEvidence", source_node, message_id))
    triples.append((source_node, "gmeow:evidenceSource", source_kind, message_id))
    predicate = {
        "ocr": "gmeow:ocrText",
        "vision": "gmeow:visionCaption",
        "document": "gmeow:documentText",
        "calendar": "gmeow:calendarText",
    }.get(source_kind, "gmeow:analysisText")
    excerpt = clean_text_for_kg(text)[:1000]
    if excerpt:
        triples.append((attachment_node, predicate, excerpt, message_id))
    for entity in spacy_entities(text, limit=100):
        entity_node = _node(_entity_node_kind(source_kind), f"{entity['kind']}/{entity['text']}")
        canonical_node = _node(entity["kind"], entity["text"])
        triples.append((attachment_node, f"gmeow:{source_kind}Mentions", entity_node, message_id))
        triples.append((source_node, "gmeow:mentionsEntity", entity_node, message_id))
        triples.append((entity_node, "rdf:type", f"gmeow:{entity['kind'].title()}", message_id))
        triples.append((entity_node, "gmeow:canonicalText", entity["text"], message_id))
        triples.append((entity_node, "gmeow:evidenceSource", source_kind, message_id))
        triples.append((entity_node, "gmeow:canonicalEntity", canonical_node, message_id))
        if message_node:
            triples.append((message_node, "gmeow:attachmentMentions", entity_node, message_id))
            triples.append((message_node, "gmeow:attachmentMentionsCanonical", canonical_node, message_id))
    for url in extract_urls(text):
        url_node = _node("url", url)
        triples.append((attachment_node, "gmeow:attachmentMentionsUrl", url_node, message_id))
        if message_node:
            triples.append((message_node, "gmeow:attachmentMentions", url_node, message_id))
    for email in extract_email_addresses(text):
        address_node = _node("address", email)
        triples.append((attachment_node, "gmeow:attachmentMentionsEmail", address_node, message_id))
        if message_node:
            triples.append((message_node, "gmeow:attachmentMentions", address_node, message_id))
    return triples


def _first_field(fields: dict[str, Any], key: str) -> str:
    value = fields.get(key)
    if isinstance(value, list) and value:
        values = cast(list[object], value)
        return str(values[0])
    return str(cast(object, value or ""))


def _section_text(analysis: dict[str, Any], sections: list[str], keys: list[str]) -> str:
    found: list[str] = []
    for section in sections:
        value = analysis.get(section)
        if not isinstance(value, dict):
            continue
        typed_value = cast(dict[str, object], value)
        found.extend(str(typed_value[key]) for key in keys if isinstance(typed_value.get(key), str))
    return "\n\n".join(found)


def _extension(filename: str) -> str:
    match = re.search(r"\.([A-Za-z0-9]{1,12})$", filename or "")
    return match.group(1).lower() if match else ""


def _entity_node_kind(source_kind: str) -> str:
    return {
        "ocr": "ocrEntity",
        "vision": "visionEntity",
        "document": "documentEntity",
        "calendar": "calendarEntity",
    }.get(source_kind, "attachmentEntity")


def spacy_entities(text: str, limit: int = 200) -> list[dict[str, str]]:
    """Spacy entities."""
    clean = clean_text_for_kg(text)
    if not clean:
        return []
    doc = _nlp()(clean[:100_000])
    entities: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    _append_spacy_entities(entities, seen, doc, limit)
    _append_fallback_org_entities(entities, seen, clean, limit)
    return entities


def _append_spacy_entities(entities: list[dict[str, str]], seen: set[tuple[str, str]], doc: object, limit: int) -> None:
    for ent in getattr(doc, "ents", []):
        kind = SPACY_ENTITY_TYPES.get(ent.label_)
        if not kind:
            continue
        value = ent.text.strip()
        if len(value) < MIN_ENTITY_TEXT_LENGTH:
            continue
        if _noisy_entity(value):
            continue
        key = (kind, value.lower())
        if key in seen:
            continue
        seen.add(key)
        entities.append({"text": value, "label": ent.label_, "kind": kind})
        if len(entities) >= limit:
            break


def _append_fallback_org_entities(entities: list[dict[str, str]], seen: set[tuple[str, str]], clean: str, limit: int) -> None:
    for value in re.findall(r"\b[A-Z][A-Za-z0-9]*(?:AI|ML|OS|DB|API|Inc|Corp|Labs?)\b", clean):
        if _noisy_entity(value):
            continue
        key = ("org", value.lower())
        if key in seen:
            continue
        seen.add(key)
        entities.append({"text": value, "label": "ORG_FALLBACK", "kind": "org"})
        if len(entities) >= limit:
            break


def clean_text_for_kg(text: str) -> str:
    """Clean text for kg."""
    value = unescape(text or "")
    value = re.sub(r"(?is)<!--.*?-->", " ", value)
    value = re.sub(r"(?is)<(script|style|head|svg|noscript)[^>]*>.*?</\1>", " ", value)
    value = re.sub(r"(?is)<[^>]+>", " ", value)
    cleaned_tokens: list[str] = []
    for token in value.split():
        if len(token) > MAX_CLEAN_TOKEN_LENGTH:
            continue
        if re.fullmatch(r"[A-Za-z0-9+/=_-]{40,}", token):
            continue
        cleaned_tokens.append(token)
    return " ".join(cleaned_tokens)


def _noisy_entity(value: str) -> bool:
    normalized = value.strip().strip(":;,.()[]{}").lower()
    if normalized in STOP_ENTITY_VALUES:
        return True
    if normalized.startswith("label_"):
        return True
    if re.search(r"[<>{}=;]", value):
        return True
    return len(value) > MAX_ENTITY_TEXT_LENGTH


def extract_urls(text: str) -> list[str]:
    """Extract urls."""
    return sorted(set(re.findall(r"https?://[^\s<>\"]+", text or "")))[:100]


def extract_email_addresses(text: str) -> list[str]:
    """Extract email addresses."""
    return sorted(set(re.findall(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", text or "")))[:100]


def _node(kind: str, value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.:@+-]+", "_", str(value).strip())[:240]
    return f"gmeow:{kind}/{safe}"


def _dedupe(triples: list[Triple]) -> list[Triple]:
    return list(dict.fromkeys(triples))
