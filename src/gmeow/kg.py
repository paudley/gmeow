# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import re
from html import unescape
from functools import lru_cache
from typing import Any


Triple = tuple[str, str, str, str | None]


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
def _nlp():
    import spacy

    try:
        return spacy.load("en_core_web_sm")
    except OSError:
        nlp = spacy.blank("en")
        nlp.add_pipe("sentencizer")
        return nlp


def extract_message_kg(message_id: str, text: str) -> list[Triple]:
    subject = _node("message", message_id)
    triples: list[Triple] = []
    clean = clean_text_for_kg(text)
    for entity in spacy_entities(clean):
        entity_node = _node(entity["kind"], entity["text"])
        triples.append((subject, f"gmeow:mentions/{entity['kind']}", entity_node, message_id))
        triples.append((entity_node, "rdf:type", f"gmeow:{entity['kind'].title()}", message_id))
        triples.append((entity_node, "gmeow:canonicalText", entity["text"], message_id))
    for url in extract_urls(clean):
        triples.append((subject, "gmeow:mentionsUrl", _node("url", url), message_id))
    for email in extract_email_addresses(clean):
        triples.append((subject, "gmeow:mentionsEmail", _node("address", email), message_id))
    return _dedupe(triples)


def extract_sidecar_kg(sha1: str, metadata: dict[str, Any]) -> list[Triple]:
    subject = _node("attachment", sha1)
    triples: list[Triple] = [(subject, "rdf:type", "gmeow:Attachment", None)]
    source = metadata.get("source", {})
    gmail = source.get("gmail", {})
    message = source.get("message", {})
    message_id = gmail.get("message_id")
    message_node = _node("message", message_id) if message_id else None
    if gmail.get("message_id"):
        triples.append((message_node, "gmeow:hasAttachment", subject, message_id))
    for key, predicate in [
        ("filename", "gmeow:attachmentFilename"),
        ("mime_type", "gmeow:attachmentMimeType"),
        ("size", "gmeow:attachmentSize"),
    ]:
        if gmail.get(key) is not None:
            triples.append((subject, predicate, str(gmail[key]), gmail.get("message_id")))
    for key, predicate in [
        ("subject", "gmeow:sourceMessageSubject"),
        ("from", "gmeow:sourceMessageFrom"),
        ("to", "gmeow:sourceMessageTo"),
        ("date", "gmeow:sourceMessageDate"),
    ]:
        if message.get(key):
            triples.append((subject, predicate, str(message[key]), gmail.get("message_id")))
    tags = metadata.get("exiftool", {}).get("tags", {})
    tag_map = {
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
    for key, predicate in tag_map.items():
        if tags.get(key) is not None:
            triples.append((subject, predicate, str(tags[key]), message_id))
    analysis = metadata.get("analysis", {}) if isinstance(metadata.get("analysis"), dict) else {}
    file_info = analysis.get("file", {}) if isinstance(analysis.get("file"), dict) else {}
    if file_info:
        for key, predicate in [
            ("media_type", "gmeow:analysisMediaType"),
            ("detected_media_type", "gmeow:detectedMimeType"),
            ("file_description", "gmeow:fileDescription"),
            ("extension", "gmeow:fileExtension"),
        ]:
            if file_info.get(key):
                triples.append((subject, predicate, str(file_info[key]), message_id))
    if analysis.get("available_text") is not None:
        triples.append((subject, "gmeow:analysisHasText", str(bool(analysis.get("available_text"))).lower(), message_id))
    document = analysis.get("document", {}) if isinstance(analysis.get("document"), dict) else {}
    if document:
        document_node = _node("document", sha1)
        triples.append((subject, "gmeow:hasDocument", document_node, message_id))
        triples.append((document_node, "rdf:type", "gmeow:Document", message_id))
        if message_node:
            triples.append((message_node, "gmeow:hasAttachmentDocument", document_node, message_id))
    for key, predicate in [
        ("title", "gmeow:documentTitle"),
        ("author", "gmeow:documentAuthor"),
        ("pages", "gmeow:documentPages"),
        ("page_size", "gmeow:documentPageSize"),
        ("producer", "gmeow:documentProducer"),
        ("creator", "gmeow:documentCreator"),
        ("pdf_version", "gmeow:documentPdfVersion"),
    ]:
        if document.get(key):
            triples.append((subject, predicate, str(document[key]), message_id))
            if document:
                triples.append((document_node, predicate, str(document[key]), message_id))
    if document.get("author"):
        author_node = _node("documentAuthor", str(document["author"]))
        triples.append((subject, "gmeow:hasDocumentAuthor", author_node, message_id))
        triples.append((author_node, "gmeow:canonicalText", str(document["author"]), message_id))
        if message_node:
            triples.append((message_node, "gmeow:attachmentMentions", author_node, message_id))
    calendar = analysis.get("calendar", {}) if isinstance(analysis.get("calendar"), dict) else {}
    fields = calendar.get("fields", {}) if isinstance(calendar.get("fields"), dict) else {}
    event_node = None
    if fields:
        event_identity = _first_field(fields, "uid") or "|".join([_first_field(fields, "summary"), _first_field(fields, "dtstart"), sha1])
        event_node = _node("event", event_identity)
        triples.append((subject, "gmeow:hasCalendarEvent", event_node, message_id))
        triples.append((event_node, "rdf:type", "gmeow:Event", message_id))
        if message_node:
            triples.append((message_node, "gmeow:hasAttachmentEvent", event_node, message_id))
    for key, predicate in [
        ("summary", "gmeow:calendarSummary"),
        ("location", "gmeow:calendarLocation"),
        ("dtstart", "gmeow:calendarStart"),
        ("dtend", "gmeow:calendarEnd"),
        ("organizer", "gmeow:calendarOrganizer"),
        ("attendee", "gmeow:calendarAttendee"),
    ]:
        for value in fields.get(key, [])[:25]:
            triples.append((subject, predicate, str(value), message_id))
            if event_node:
                triples.append((event_node, predicate, str(value), message_id))
            if key in {"attendee", "organizer"}:
                attendee_node = _node("calendarAttendee", str(value))
                triples.append((subject, "gmeow:hasCalendarParticipant", attendee_node, message_id))
                if event_node:
                    triples.append((event_node, "gmeow:hasParticipant", attendee_node, message_id))
                if message_node:
                    triples.append((message_node, "gmeow:attachmentMentions", attendee_node, message_id))
    archive = analysis.get("archive", {}) if isinstance(analysis.get("archive"), dict) else {}
    for filename in archive.get("files", [])[:100]:
        child = _node("archiveFile", f"{sha1}/{filename}")
        triples.append((subject, "gmeow:containsFile", child, message_id))
        triples.append((child, "rdf:type", "gmeow:ArchiveFile", message_id))
        triples.append((child, "gmeow:filename", str(filename), message_id))
        extension = _extension(filename)
        if extension:
            triples.append((child, "gmeow:fileExtension", extension, message_id))
        if message_node:
            triples.append((message_node, "gmeow:attachmentContainsFile", child, message_id))
    extracted_sections = {
        "document": document.get("text") or "",
        "content": analysis.get("content_text") or "",
        "ocr": _section_text(analysis, ["image"], ["ocr_text"]) or analysis.get("ocr_text") or "",
        "vision": _section_text(analysis, ["vision"], ["vision_caption"]) or analysis.get("vision_caption") or "",
        "calendar": calendar.get("text") or "",
    }
    for source_kind, text in extracted_sections.items():
        triples.extend(_attachment_text_entities(subject, message_node, message_id, source_kind, text))
    searchable_text = json.dumps({"source": source, "exiftool": tags, "analysis": analysis}, sort_keys=True)
    triples.extend(extract_message_kg(f"attachment/{sha1}", searchable_text))
    return _dedupe(triples)


def _attachment_text_entities(attachment_node: str, message_node: str | None, message_id: str | None, source_kind: str, text: str) -> list[Triple]:
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
        return str(value[0])
    return str(value or "")


def _section_text(analysis: dict[str, Any], sections: list[str], keys: list[str]) -> str:
    found = []
    for section in sections:
        value = analysis.get(section)
        if not isinstance(value, dict):
            continue
        for key in keys:
            if isinstance(value.get(key), str):
                found.append(value[key])
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
    clean = clean_text_for_kg(text)
    if not clean:
        return []
    doc = _nlp()(clean[:100_000])
    entities = []
    seen: set[tuple[str, str]] = set()
    for ent in getattr(doc, "ents", []):
        kind = SPACY_ENTITY_TYPES.get(ent.label_)
        if not kind:
            continue
        value = ent.text.strip()
        if len(value) < 2:
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
    return entities


def clean_text_for_kg(text: str) -> str:
    value = unescape(text or "")
    value = re.sub(r"(?is)<!--.*?-->", " ", value)
    value = re.sub(r"(?is)<(script|style|head|svg|noscript)[^>]*>.*?</\1>", " ", value)
    value = re.sub(r"(?is)<[^>]+>", " ", value)
    cleaned_tokens = []
    for token in value.split():
        if len(token) > 100:
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
    if len(value) > 120:
        return True
    return False


def extract_urls(text: str) -> list[str]:
    return sorted(set(re.findall(r"https?://[^\s<>\"]+", text or "")))[:100]


def extract_email_addresses(text: str) -> list[str]:
    return sorted(set(re.findall(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", text or "")))[:100]


def _node(kind: str, value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.:@+-]+", "_", str(value).strip())[:240]
    return f"gmeow:{kind}/{safe}"


def _dedupe(triples: list[Triple]) -> list[Triple]:
    return list(dict.fromkeys(triples))
