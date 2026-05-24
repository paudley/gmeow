# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any


DEFAULT_EXCLUDED_CATEGORIES = {"camera_alert", "machine_notification", "bulk_status_noise", "call_notice", "dev_activity", "dev_review", "mailing_list"}


def categorize_message(message: dict[str, Any]) -> list[str]:
    subject = (message.get("subject") or "").lower()
    sender = (message.get("sender") or "").lower()
    snippet = (message.get("snippet") or "").lower()
    text = " ".join([subject, sender, snippet, (message.get("text_body") or "")[:2000].lower()])
    labels = {label.lower() for label in message.get("labels", [])}
    categories = []
    if "notifications.ui.com" in sender or "unifi os" in sender or "unifi.ui.com" in text:
        categories.append("camera_alert")
    elif "smart detection" in text and ("recorded an animal" in text or "protect/events" in text):
        categories.append("camera_alert")
    if "googlealerts-noreply@google.com" in sender:
        categories.append("news_alert")
    if "google-workspace-alerts-noreply@google.com" in sender:
        categories.append("admin_alert")
    if "category_promotions" in labels:
        categories.append("promotion")
    if "category_updates" in labels:
        categories.append("update")
    if not categories:
        categories.append("primary")
    return sorted(set(categories))


def _title_category(category: str) -> str:
    return category.replace("_", " ").replace("-", " ").title()


def _parse_message_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _coerce_query_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    return _parse_message_date(value) or value


def _filter_by_date(messages: list[dict[str, Any]], after: str | None, before: str | None) -> list[dict[str, Any]]:
    after_iso = _coerce_query_date(after)
    before_iso = _coerce_query_date(before)
    result = []
    for message in messages:
        date = message.get("message_date_iso")
        if after_iso and (not date or date < after_iso):
            continue
        if before_iso and (not date or date > before_iso):
            continue
        result.append(message)
    return result


def _parse_local_query(query: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {"from": [], "to": [], "subject": [], "label": [], "after": None, "before": None, "fts": ""}
    text = query or ""
    text = re.sub(r"[{}]", " ", text)
    text = re.sub(r"\b(?:AND|OR)\b", " ", text, flags=re.I)
    operator_pattern = re.compile(r'\b(from|to|subject|label|after|before):(?:"([^"]+)"|\'([^\']+)\'|(\S+))', re.I)
    terms = []
    last = 0
    for match in operator_pattern.finditer(text):
        terms.append(text[last : match.start()])
        key = match.group(1).lower()
        value = next(group for group in match.groups()[1:] if group is not None)
        value = value.strip().strip("()")
        if key in {"after", "before"}:
            parsed[key] = value
        else:
            parsed[key].append(value)
        last = match.end()
    terms.append(text[last:])
    parsed["fts"] = _fts_query(" ".join(terms))
    return parsed


def _fts_query(value: str) -> str:
    tokens = re.findall(r'"([^"]+)"|([A-Za-z0-9_.@+-]+)', value or "")
    parts = []
    for phrase, word in tokens:
        token = phrase or word
        if not token or token.lower() in {"and", "or"}:
            continue
        token = token.replace('"', '""')
        parts.append(f'"{token}"')
    return " ".join(parts)


def _noisy_graph_node(node: str, label: str) -> bool:
    return _graph_node_profile(node, label).get("visibility") != "user"


def _graph_node_profile(node: str, label: str | None = None, predicate: str | None = None) -> dict[str, Any]:
    label = label if label is not None else _graph_node_label(node)
    value = label.strip()
    lowered = value.lower()
    namespace = _ontology_namespace(node)
    kind = _graph_node_kind(node, predicate)
    profile: dict[str, Any] = {
        "kind": kind,
        "namespace": namespace,
        "visibility": "user",
        "role": "entity",
        "noise_reason": None,
    }
    structural_nodes = {
        "gmeow:Message",
        "gmeow:Org",
        "gmeow:Person",
        "gmeow:Date",
        "gmeow:Cardinal",
        "gmeow:Event",
        "gmeow:Document",
        "gmeow:ArchiveFile",
        "gmeow:AttachmentEvidence",
        "gmeow:Location",
        "gmeow:Money",
        "gmeow:Product",
        "gmeow:Time",
        "gmeow:Place",
        "gmeow:Facility",
        "gmeow:Group",
        "gmeow:Work",
        "gmeow:Law",
        "gmeow:Language",
        "gmeow:Percent",
        "gmeow:Quantity",
        "gmeow:Ordinal",
        "gmeow:Attachment",
        "http://rdfs.org/sioc/ns#Post",
        "http://rdfs.org/sioc/ns#Thread",
        "https://schema.org/EmailMessage",
        "https://schema.org/SoftwareSourceCode",
        "http://xmlns.com/foaf/0.1/Agent",
        "http://www.w3.org/2004/02/skos/core#Concept",
        "http://usefulinc.com/ns/doap#Project",
    }
    if node in structural_nodes:
        profile.update({"kind": "ontology_classes", "visibility": "structural", "role": "class", "noise_reason": "ontology_class"})
        return profile
    if namespace is not None and (predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" or node.endswith(("#Agent", "#Concept", "#Post", "#Thread", "#Project"))):
        profile.update({"kind": "ontology_classes", "visibility": "structural", "role": "class", "noise_reason": "ontology_class"})
        return profile
    if node.startswith("gmeow:message/") or node.startswith("gmeow:thread/") or node.startswith("gmeow:gmailMessage/"):
        profile.update({"visibility": "structural", "role": "mailbox_object", "noise_reason": "mailbox_identifier"})
        return profile
    if node.startswith("gmeow:project/"):
        profile.update({"kind": "projects", "role": "project"})
        return profile
    if node.startswith("gmeow:org/"):
        if value.startswith("@"):
            profile.update({"kind": "handles", "role": "handle"})
            return profile
        if _looks_like_mail_label_artifact(value) or _looks_like_metadata_artifact(value) or _looks_like_ui_artifact(value):
            if _looks_like_mail_label_artifact(value):
                reason = "mail_label_artifact"
            elif _looks_like_metadata_artifact(value):
                reason = "metadata_key_artifact"
            else:
                reason = "ui_text_artifact"
            profile.update({"kind": "orgs", "visibility": "noise", "role": "organization", "noise_reason": reason})
            return profile
        profile.update({"kind": "orgs", "role": "organization"})
        return profile
    if node.startswith("gmeow:document/"):
        profile.update({"kind": "documents", "role": "document"})
        return profile
    if node.startswith("gmeow:event/"):
        profile.update({"kind": "events", "role": "event"})
        return profile
    if node.startswith("gmeow:archiveFile/"):
        profile.update({"kind": "archive_files", "role": "archive_file"})
        return profile
    if node.startswith("gmeow:documentAuthor/"):
        profile.update({"kind": "document_authors", "role": "document_author"})
        return profile
    if node.startswith("gmeow:calendarAttendee/"):
        profile.update({"kind": "calendar_attendees", "role": "calendar_attendee"})
        return profile
    if node.startswith("gmeow:attachmentEvidence/"):
        profile.update({"kind": "attachment_evidence", "visibility": "structural", "role": "evidence", "noise_reason": "evidence_node"})
        return profile
    if node.startswith("gmeow:ocrEntity/"):
        profile.update({"kind": "ocr_entities", "role": "ocr_entity"})
        return profile
    if node.startswith("gmeow:visionEntity/"):
        profile.update({"kind": "vision_entities", "role": "vision_entity"})
        return profile
    if node.startswith("gmeow:documentEntity/"):
        profile.update({"kind": "document_entities", "role": "document_entity"})
        return profile
    if node.startswith("gmeow:calendarEntity/"):
        profile.update({"kind": "calendar_entities", "role": "calendar_entity"})
        return profile
    if node.startswith("gmeow:address/") or lowered.startswith("mailto:"):
        profile.update({"kind": "addresses", "role": "contact"})
        return profile
    if node.startswith("gmeow:label/"):
        profile.update({"kind": "labels", "role": "label"})
        return profile
    if node.startswith("gmeow:attachment/"):
        profile.update({"kind": "attachments", "role": "attachment"})
        return profile
    if node.startswith("gmeow:url/") or lowered.startswith(("http://", "https://")):
        profile.update({"kind": "urls", "role": "url"})
        return profile
    noisy = {
        "doctype",
        "utf-8",
        "dtd",
        "w3c",
        "arial",
        "roboto",
        "lato",
        "tr",
        "public",
        "transitional",
        "html",
        "css",
        "nbsp",
    }
    if lowered in noisy:
        profile.update({"visibility": "noise", "role": "boilerplate", "noise_reason": "html_css_boilerplate"})
        return profile
    if re.fullmatch(r"\d+(?:\.\d+)?", lowered):
        profile.update({"visibility": "noise", "role": "numeric_literal", "noise_reason": "bare_number"})
        return profile
    if re.fullmatch(r"\d+(?:\.\d+)?\s*(?:px|fps|kb|mb|gb|bytes?)", lowered):
        profile.update({"visibility": "noise", "role": "numeric_literal", "noise_reason": "measurement_literal"})
        return profile
    if node.startswith("gmeow:entity/") and (len(value) <= 2 or re.fullmatch(r"[a-z]{1,3}", lowered)):
        profile.update({"visibility": "noise", "role": "short_entity", "noise_reason": "too_short"})
        return profile
    if node.startswith("gmeow:"):
        roles = {
            "entities": "entity",
            "claims": "claim",
            "tasks": "task",
            "headers": "header",
            "literals": "literal",
        }
        profile.update({"role": roles.get(kind, kind[:-1] if kind.endswith("s") else kind)})
        return profile
    if re.fullmatch(r"<[^>\s]+@[^>\s]+>", value):
        profile.update({"kind": "headers", "visibility": "noise", "role": "message_identifier", "noise_reason": "message_id_header"})
        return profile
    if "@" in value and not any(char.isspace() for char in value):
        profile.update({"kind": "addresses", "role": "contact"})
        return profile
    if namespace:
        profile.update({"visibility": "structural", "role": "ontology_term", "noise_reason": "ontology_term"})
        return profile
    profile.update({"kind": "literals", "role": "literal"})
    return profile


def _ontology_namespace(value: str) -> str | None:
    namespaces = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "sioc": "http://rdfs.org/sioc/ns#",
        "schemaorg": "https://schema.org/",
        "skos": "http://www.w3.org/2004/02/skos/core#",
        "prov_o": "http://www.w3.org/ns/prov#",
        "doap": "http://usefulinc.com/ns/doap#",
    }
    for name, namespace in namespaces.items():
        if value.startswith(namespace):
            return name
    return "gmeow" if value.startswith("gmeow:") else None


def _looks_like_mail_label_artifact(value: str) -> bool:
    normalized = value.strip().lower()
    gmail_labels = {
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
    }
    return normalized in gmail_labels or normalized.startswith("label_")


def _looks_like_metadata_artifact(value: str) -> bool:
    normalized = value.strip().lower()
    metadata_keys = {
        "imageheight",
        "imagewidth",
        "filesize",
        "filetype",
        "mimetype",
        "sourcefile",
        "directory",
        "filename",
        "filemodifydate",
        "fileaccessdate",
        "filecreatedate",
        "exiftoolversion",
        "bitdepth",
        "colortype",
        "compression",
        "interlace",
    }
    return normalized in metadata_keys


def _looks_like_ui_artifact(value: str) -> bool:
    normalized = value.strip().lower()
    ui_terms = {
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
    return normalized in ui_terms


def _graph_node_kind(value: str, predicate: str | None) -> str:
    if value.startswith("gmeow:message/"):
        return "messages"
    if value.startswith("gmeow:thread/"):
        return "threads"
    if value.startswith("gmeow:address/"):
        return "addresses"
    if value.startswith("gmeow:entity/"):
        return "entities"
    if value.startswith("gmeow:org/"):
        return "orgs"
    if value.startswith("gmeow:label/"):
        return "labels"
    if value.startswith("gmeow:attachment/"):
        return "attachments"
    if value.startswith("gmeow:document/"):
        return "documents"
    if value.startswith("gmeow:event/"):
        return "events"
    if value.startswith("gmeow:archiveFile/"):
        return "archive_files"
    if value.startswith("gmeow:documentAuthor/"):
        return "document_authors"
    if value.startswith("gmeow:calendarAttendee/"):
        return "calendar_attendees"
    if value.startswith("gmeow:attachmentEvidence/"):
        return "attachment_evidence"
    if value.startswith("gmeow:ocrEntity/"):
        return "ocr_entities"
    if value.startswith("gmeow:visionEntity/"):
        return "vision_entities"
    if value.startswith("gmeow:documentEntity/"):
        return "document_entities"
    if value.startswith("gmeow:calendarEntity/"):
        return "calendar_entities"
    if value.startswith("gmeow:project/"):
        return "projects"
    if value.startswith("gmeow:url/"):
        return "urls"
    if predicate == "gmeow:hasClaim":
        return "claims"
    if predicate == "gmeow:hasTask":
        return "tasks"
    if predicate and predicate.startswith("gmeow:header/"):
        return "headers"
    return "other"


def _graph_node_label(value: str) -> str:
    if value.startswith("gmeow:"):
        return value.rsplit("/", 1)[-1].replace("_", " ").strip()
    return value


def _best_contact_name(names: list[str], address: str) -> str:
    cleaned = [name.strip().strip('"') for name in names if name.strip()]
    if cleaned:
        return sorted(cleaned, key=lambda value: (-len(value), value.lower()))[0]
    return parseaddr(address)[0] or address


def _person_key(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.lower()).strip()
    normalized = re.sub(r"[^a-z0-9@._+-]+", " ", normalized)
    return " ".join(part for part in normalized.split() if len(part) > 1)


def _attachment_text_from_metadata(metadata: dict[str, Any], limit: int = 20_000) -> str:
    keys = {
        "text",
        "extracted_text",
        "ocr_text",
        "transcript",
        "description",
        "summary",
        "image_description",
        "vision_caption",
        "pdf_text",
        "ocr_text",
        "content_text",
    }
    found: list[str] = []

    def walk(value: Any, key: str = "") -> None:
        if len("\n\n".join(found)) >= limit:
            return
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                walk(child_value, str(child_key).lower())
        elif isinstance(value, list):
            for child in value:
                walk(child, key)
        elif isinstance(value, str) and key in keys and value.strip():
            found.append(value.strip())

    walk(metadata)
    return "\n\n".join(found)[:limit]


def _json_compact(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
