# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Provide cache functionality for Gmeow.

This module keeps pure parsing, categorization, and graph display helpers separate from the
PostgreSQL cache implementation. Callers use these helpers to normalize message metadata before
storage and to derive user-facing graph labels from indexed triples.
"""

import re
from datetime import UTC, datetime
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any, cast

DEFAULT_EXCLUDED_CATEGORIES = {
    "camera_alert",
    "machine_notification",
    "bulk_status_noise",
    "call_notice",
    "dev_activity",
    "dev_review",
    "mailing_list",
}

MIN_USER_ENTITY_LENGTH = 2

STRUCTURAL_GRAPH_NODES = {
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
GRAPH_NODE_PREFIX_PROFILES = [
    ("gmeow:project/", {"kind": "projects", "role": "project"}),
    ("gmeow:document/", {"kind": "documents", "role": "document"}),
    ("gmeow:event/", {"kind": "events", "role": "event"}),
    ("gmeow:archiveFile/", {"kind": "archive_files", "role": "archive_file"}),
    ("gmeow:documentAuthor/", {"kind": "document_authors", "role": "document_author"}),
    ("gmeow:calendarAttendee/", {"kind": "calendar_attendees", "role": "calendar_attendee"}),
    (
        "gmeow:attachmentEvidence/",
        {"kind": "attachment_evidence", "visibility": "structural", "role": "evidence", "noise_reason": "evidence_node"},
    ),
    ("gmeow:ocrEntity/", {"kind": "ocr_entities", "role": "ocr_entity"}),
    ("gmeow:visionEntity/", {"kind": "vision_entities", "role": "vision_entity"}),
    ("gmeow:documentEntity/", {"kind": "document_entities", "role": "document_entity"}),
    ("gmeow:calendarEntity/", {"kind": "calendar_entities", "role": "calendar_entity"}),
    ("gmeow:label/", {"kind": "labels", "role": "label"}),
    ("gmeow:attachment/", {"kind": "attachments", "role": "attachment"}),
]
GRAPH_NODE_KIND_PREFIXES = {
    "gmeow:message/": "messages",
    "gmeow:thread/": "threads",
    "gmeow:address/": "addresses",
    "gmeow:entity/": "entities",
    "gmeow:org/": "orgs",
    "gmeow:label/": "labels",
    "gmeow:attachment/": "attachments",
    "gmeow:document/": "documents",
    "gmeow:event/": "events",
    "gmeow:archiveFile/": "archive_files",
    "gmeow:documentAuthor/": "document_authors",
    "gmeow:calendarAttendee/": "calendar_attendees",
    "gmeow:attachmentEvidence/": "attachment_evidence",
    "gmeow:ocrEntity/": "ocr_entities",
    "gmeow:visionEntity/": "vision_entities",
    "gmeow:documentEntity/": "document_entities",
    "gmeow:calendarEntity/": "calendar_entities",
    "gmeow:project/": "projects",
    "gmeow:url/": "urls",
}


def categorize_message(message: dict[str, Any]) -> list[str]:
    """Categorize message."""
    subject = (message.get("subject") or "").lower()
    sender = (message.get("sender") or "").lower()
    snippet = (message.get("snippet") or "").lower()
    text = " ".join([subject, sender, snippet, (message.get("text_body") or "")[:2000].lower()])
    labels = {label.lower() for label in message.get("labels", [])}
    categories: list[str] = []
    categories.extend(_sender_categories(sender, text))
    categories.extend(_label_categories(labels))
    if not categories:
        categories.append("primary")
    return sorted(set(categories))


def _sender_categories(sender: str, text: str) -> list[str]:
    categories: list[str] = []
    camera_alert = (
        "notifications.ui.com" in sender
        or "unifi os" in sender
        or "unifi.ui.com" in text
        or ("smart detection" in text and ("recorded an animal" in text or "protect/events" in text))
    )
    if camera_alert:
        categories.append("camera_alert")
    if "googlealerts-noreply@google.com" in sender:
        categories.append("news_alert")
    if "google-workspace-alerts-noreply@google.com" in sender:
        categories.append("admin_alert")
    return categories


def _label_categories(labels: set[str]) -> list[str]:
    categories: list[str] = []
    if "category_promotions" in labels:
        categories.append("promotion")
    if "category_updates" in labels:
        categories.append("update")
    return categories


def _title_category(category: str) -> str:
    return category.replace("_", " ").replace("-", " ").title()


def title_category(category: str) -> str:
    """Return a display label for a category slug."""
    return _title_category(category)


def _parse_message_date(value: str) -> str:
    if not value:
        msg = "Message date is required."
        raise ValueError(msg)
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as iso_exc:
            msg = f"Invalid message date: {value!r}"
            raise ValueError(msg) from iso_exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_message_date(value: str) -> str:
    """Parse a message date into a UTC ISO timestamp."""
    return _parse_message_date(value)


def _coerce_query_date(value: str) -> str:
    value = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    return _parse_message_date(value)


def _filter_by_date(messages: list[dict[str, Any]], after: str, before: str) -> list[dict[str, Any]]:
    after_iso = _coerce_query_date(after) if after else ""
    before_iso = _coerce_query_date(before) if before else ""
    result: list[dict[str, Any]] = []
    for message in messages:
        date = message.get("message_date_iso")
        if after_iso and (not date or date < after_iso):
            continue
        if before_iso and (not date or date > before_iso):
            continue
        result.append(message)
    return result


def filter_by_date(messages: list[dict[str, Any]], after: str = "", before: str = "") -> list[dict[str, Any]]:
    """Filter messages using local query date bounds."""
    return _filter_by_date(messages, after, before)


def _parse_local_query(query: str) -> dict[str, Any]:
    from_values: list[str] = []
    to_values: list[str] = []
    subject_values: list[str] = []
    label_values: list[str] = []
    after = ""
    before = ""
    text = query or ""
    text = re.sub(r"[{}]", " ", text)
    text = re.sub(r"\b(?:AND|OR)\b", " ", text, flags=re.IGNORECASE)
    operator_pattern = re.compile(r'\b(from|to|subject|label|after|before):(?:"([^"]+)"|\'([^\']+)\'|(\S+))', re.IGNORECASE)
    terms: list[str] = []
    last = 0
    for match in operator_pattern.finditer(text):
        terms.append(text[last : match.start()])
        key = match.group(1).lower()
        value = next(group for group in match.groups()[1:] if group is not None)
        value = value.strip().strip("()")
        if key == "after":
            after = value
        elif key == "before":
            before = value
        elif key == "from":
            from_values.append(value)
        elif key == "to":
            to_values.append(value)
        elif key == "subject":
            subject_values.append(value)
        else:
            label_values.append(value)
        last = match.end()
    terms.append(text[last:])
    return {
        "from": from_values,
        "to": to_values,
        "subject": subject_values,
        "label": label_values,
        "after": after,
        "before": before,
        "fts": _fts_query(" ".join(terms)),
    }


def parse_local_query(query: str) -> dict[str, Any]:
    """Parse a local search query into structured filters."""
    return _parse_local_query(query)


def _fts_query(value: str) -> str:
    tokens = re.findall(r'"([^"]+)"|([A-Za-z0-9_.@+-]+)', value or "")
    parts: list[str] = []
    for phrase, word in tokens:
        token = phrase or word
        if not token or token.lower() in {"and", "or"}:
            continue
        token = token.replace('"', '""')
        parts.append(f'"{token}"')
    return " ".join(parts)


def _graph_node_profile(node: str, label: str = "", predicate: str = "") -> dict[str, Any]:
    label = label or _graph_node_label(node)
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
    for candidate in (
        _structural_graph_profile(node, namespace, predicate),
        _org_graph_profile(node, value),
        _prefix_profile(node),
        _literal_graph_profile(node, value, lowered, namespace, kind),
    ):
        if candidate:
            profile.update(candidate)
            break
    return profile


def graph_node_profile(node: str, label: str = "", predicate: str = "") -> dict[str, Any]:
    """Return a display and filtering profile for a graph node."""
    return _graph_node_profile(node, label, predicate)


def _structural_graph_profile(node: str, namespace: str, predicate: str) -> dict[str, str]:
    if node in STRUCTURAL_GRAPH_NODES or (
        namespace
        and (
            predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            or node.endswith(("#Agent", "#Concept", "#Post", "#Thread", "#Project"))
        )
    ):
        return {"kind": "ontology_classes", "visibility": "structural", "role": "class", "noise_reason": "ontology_class"}
    if node.startswith(("gmeow:message/", "gmeow:thread/", "gmeow:gmailMessage/")):
        return {"visibility": "structural", "role": "mailbox_object", "noise_reason": "mailbox_identifier"}
    return {}


def _org_graph_profile(node: str, value: str) -> dict[str, str]:
    if not node.startswith("gmeow:org/"):
        return {}
    if value.startswith("@"):
        return {"kind": "handles", "role": "handle"}
    reason = _org_noise_reason(value)
    if reason:
        return {"kind": "orgs", "visibility": "noise", "role": "organization", "noise_reason": reason}
    return {"kind": "orgs", "role": "organization"}


def _org_noise_reason(value: str) -> str:
    if _looks_like_mail_label_artifact(value):
        return "mail_label_artifact"
    if _looks_like_metadata_artifact(value):
        return "metadata_key_artifact"
    if _looks_like_ui_artifact(value):
        return "ui_text_artifact"
    return ""


def _literal_graph_profile(node: str, value: str, lowered: str, namespace: str, kind: str) -> dict[str, str]:
    profile = _literal_address_or_url_profile(node, lowered)
    noise = _literal_noise_profile(node, value, lowered)
    if not profile and noise:
        profile = noise
    if not profile and node.startswith("gmeow:"):
        roles = {"entities": "entity", "claims": "claim", "tasks": "task", "headers": "header", "literals": "literal"}
        profile = {"role": roles.get(kind, kind.removesuffix("s"))}
    if not profile and re.fullmatch(r"<[^>\s]+@[^>\s]+>", value):
        profile = {"kind": "headers", "visibility": "noise", "role": "message_identifier", "noise_reason": "message_id_header"}
    if not profile and "@" in value and not any(char.isspace() for char in value):
        profile = {"kind": "addresses", "role": "contact"}
    if not profile and namespace:
        profile = {"visibility": "structural", "role": "ontology_term", "noise_reason": "ontology_term"}
    return profile or {"kind": "literals", "role": "literal"}


def _literal_address_or_url_profile(node: str, lowered: str) -> dict[str, str]:
    if node.startswith("gmeow:address/") or lowered.startswith("mailto:"):
        return {"kind": "addresses", "role": "contact"}
    if node.startswith("gmeow:url/") or lowered.startswith(("http://", "https://")):
        return {"kind": "urls", "role": "url"}
    return {}


def _literal_noise_profile(node: str, value: str, lowered: str) -> dict[str, str]:
    noisy = {"doctype", "utf-8", "dtd", "w3c", "arial", "roboto", "lato", "tr", "public", "transitional", "html", "css", "nbsp"}
    if lowered in noisy:
        return {"visibility": "noise", "role": "boilerplate", "noise_reason": "html_css_boilerplate"}
    if re.fullmatch(r"\d+(?:\.\d+)?", lowered):
        return {"visibility": "noise", "role": "numeric_literal", "noise_reason": "bare_number"}
    if re.fullmatch(r"\d+(?:\.\d+)?\s*(?:px|fps|kb|mb|gb|bytes?)", lowered):
        return {"visibility": "noise", "role": "numeric_literal", "noise_reason": "measurement_literal"}
    if node.startswith("gmeow:entity/") and (len(value) <= MIN_USER_ENTITY_LENGTH or re.fullmatch(r"[a-z]{1,3}", lowered)):
        return {"visibility": "noise", "role": "short_entity", "noise_reason": "too_short"}
    return {}


def _ontology_namespace(value: str) -> str:
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
    return "gmeow" if value.startswith("gmeow:") else ""


def _prefix_profile(node: str) -> dict[str, str]:
    for prefix, profile in GRAPH_NODE_PREFIX_PROFILES:
        if node.startswith(prefix):
            return profile
    return {}


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


def _graph_node_kind(value: str, predicate: str) -> str:
    for prefix, kind in GRAPH_NODE_KIND_PREFIXES.items():
        if value.startswith(prefix):
            return kind
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


def graph_node_label(value: str) -> str:
    """Return a compact display label for a graph node IRI."""
    return _graph_node_label(value)


def _contact_name_rank(value: str) -> tuple[int, str]:
    return -len(value), value.lower()


def _best_contact_name(names: list[str], address: str) -> str:
    cleaned = [name.strip().strip('"') for name in names if name.strip()]
    if cleaned:
        return sorted(cleaned, key=_contact_name_rank)[0]
    return parseaddr(address)[0] or address


def best_contact_name(names: list[str], address: str) -> str:
    """Return the best display name for an address."""
    return _best_contact_name(names, address)


def _person_key(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.lower()).strip()
    normalized = re.sub(r"[^a-z0-9@._+-]+", " ", normalized)
    return " ".join(part for part in normalized.split() if len(part) > 1)


def person_key(value: str) -> str:
    """Return the normalized key used for person aliases."""
    return _person_key(value)


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
        "content_text",
    }
    found: list[str] = []

    def walk(value: object, key: str = "") -> None:
        """Walk."""
        if len("\n\n".join(found)) >= limit:
            return
        if isinstance(value, dict):
            for child_key, child_value in cast(dict[object, object], value).items():
                walk(child_value, str(child_key).lower())
        elif isinstance(value, list):
            for child in cast(list[object], value):
                walk(child, key)
        elif isinstance(value, str) and key in keys and value.strip():
            found.append(value.strip())

    walk(metadata)
    return "\n\n".join(found)[:limit]


def attachment_text_from_metadata(metadata: dict[str, Any], limit: int = 20_000) -> str:
    """Extract searchable attachment text from sidecar metadata."""
    return _attachment_text_from_metadata(metadata, limit)
