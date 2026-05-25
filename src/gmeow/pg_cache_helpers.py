# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide shared SQL and cache helper routines.

The module contains query builders, normalization helpers, date filters, and graph profile utilities
used by PostgreSQL cache modules. It keeps repeated SQL fragments and parsing behavior out of the
main cache class.
"""

import os
import re
import socket
from datetime import UTC, datetime
from email.utils import getaddresses, parseaddr
from typing import Any, cast

import blake3
from psycopg import sql

from .cache import parse_message_date

DEFAULT_DATETIME = cast(datetime, None)
DEFAULT_SQL_COMPOSABLE = cast(sql.Composable, None)
DEFAULT_STR = cast(str, None)

AGE_COLUMN_PARTS = 2


def age_sql(cypher: str, columns: str) -> sql.Composable:
    """Build a safe Apache AGE query wrapper."""
    return sql.SQL("SELECT * FROM cypher('gmeow_graph', {}) AS ({})").format(age_cypher_sql(cypher), age_columns_sql(columns))


def age_cypher_sql(cypher: str) -> sql.Composable:
    """Build a safely dollar-quoted Cypher literal for Apache AGE."""
    tag = "gmeow_age"
    while f"${tag}$" in cypher:
        tag = f"_{tag}"
    return sql.SQL(f"${tag}${cypher}${tag}$")


def apply_category_filters_sql(
    where: list[str],
    params: list[Any],
    include_categories: list[str],
    exclude_categories: list[str],
    default_excluded_categories: set[str],
) -> None:
    """Append category include and exclude predicates to a message query."""
    if include_categories:
        where.append("EXISTS (SELECT 1 FROM message_categories mc WHERE mc.message_id = messages.id AND mc.category = ANY(%s))")
        params.append(include_categories)
    excluded = exclude_categories
    if not excluded and not include_categories:
        excluded = list(default_excluded_categories)
    if excluded:
        where.append("NOT EXISTS (SELECT 1 FROM message_categories mc WHERE mc.message_id = messages.id AND mc.category = ANY(%s))")
        params.append(excluded)


def import_insert_sql(
    table: str, keys: list[str], conflict_keys: list[str], conflict_target: sql.Composable = DEFAULT_SQL_COMPOSABLE
) -> sql.Composable:
    """Build an INSERT ... ON CONFLICT update statement."""
    columns = sql.SQL(", ").join(sql.Identifier(key) for key in keys)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in keys)
    assignments = sql.SQL(", ").join(
        sql.SQL("{}=excluded.{}").format(sql.Identifier(key), sql.Identifier(key)) for key in keys if key not in set(conflict_keys)
    )
    return sql.SQL("INSERT INTO {}({}) VALUES({}) ON CONFLICT({}) DO UPDATE SET {}").format(
        sql.Identifier(table),
        columns,
        placeholders,
        conflict_target or sql.SQL(", ").join(sql.Identifier(key) for key in conflict_keys),
        assignments,
    )


def age_columns_sql(columns: str) -> sql.Composable:
    """Build a validated AGE result column declaration."""
    rendered: list[Any] = []
    for column in columns.split(","):
        parts = column.split()
        if len(parts) != AGE_COLUMN_PARTS or parts[1].lower() != "agtype" or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", parts[0]):
            msg = f"Unsupported AGE result column definition: {column!r}"
            raise ValueError(msg)
        rendered.append(sql.SQL("{} agtype").format(sql.Identifier(parts[0])))
    if not rendered:
        msg = "AGE query must declare at least one result column."
        raise ValueError(msg)
    return cast(sql.Composable, sql.SQL(", ").join(rendered))


def worker_id() -> str:
    """Return a stable worker identifier for the current process."""
    return f"{socket.gethostname()}:{os.getpid()}"


def blake3_digest(content: bytes) -> str:
    """Blake3 digest."""
    return str(blake3.blake3(content).hexdigest())


def imap_mailbox_name(value: str) -> str:
    """Normalize an IMAP mailbox name."""
    cleaned = (value or "").strip().replace("\\", "/")
    if not cleaned:
        return "Unknown"
    upper = cleaned.upper()
    system = {
        "INBOX": "INBOX",
        "SENT": "Sent",
        "TRASH": "Trash",
        "SPAM": "Spam",
        "DRAFT": "Drafts",
        "IMPORTANT": "Important",
        "STARRED": "Starred",
    }
    return system.get(upper, cleaned.strip("/"))


def read_only_cypher(query: str) -> bool:
    """Return whether a Cypher query is limited to read-only MATCH usage."""
    lowered = query.strip().lower()
    if not lowered.startswith("match "):
        return False
    forbidden = {" create ", " merge ", " delete ", " detach ", " set ", " remove ", " drop ", " call "}
    padded = f" {lowered} "
    return not any(word in padded for word in forbidden)


def profile_allowed(profile: dict[str, Any], visibility: str = "user", kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR) -> bool:
    """Return whether a graph node profile matches the requested filters."""
    requested_visibility = (visibility or "user").lower()
    if requested_visibility not in {"all", "any"} and profile.get("visibility") != requested_visibility:
        return False
    if kind and normalize_kind(str(profile.get("kind"))) != normalize_kind(kind):
        return False
    return not (namespace and profile.get("namespace") != namespace)


def algorithm_profile_allowed(
    profile: dict[str, Any], visibility: str = "user", kind: str = DEFAULT_STR, namespace: str = DEFAULT_STR
) -> bool:
    """Return whether a profile should be included in graph algorithms."""
    if not profile_allowed(profile, visibility=visibility, kind=kind, namespace=namespace):
        return False
    if kind or namespace or (visibility or "user").lower() in {"all", "any", "noise", "structural"}:
        return True
    return profile.get("kind") != "literals"


def normalize_kind(kind: str) -> str:
    """Normalize user-facing graph kind aliases."""
    value = kind.lower().replace("-", "_")
    aliases = {
        "address": "addresses",
        "contact": "addresses",
        "contacts": "addresses",
        "entity": "entities",
        "project": "projects",
        "label": "labels",
        "attachment": "attachments",
        "document": "documents",
        "documents": "documents",
        "event": "events",
        "events": "events",
        "archive_file": "archive_files",
        "archive_files": "archive_files",
        "document_author": "document_authors",
        "document_authors": "document_authors",
        "calendar_attendee": "calendar_attendees",
        "calendar_attendees": "calendar_attendees",
        "ocr_entity": "ocr_entities",
        "ocr_entities": "ocr_entities",
        "vision_entity": "vision_entities",
        "vision_entities": "vision_entities",
        "document_entity": "document_entities",
        "document_entities": "document_entities",
        "calendar_entity": "calendar_entities",
        "calendar_entities": "calendar_entities",
        "attachment_evidence": "attachment_evidence",
        "url": "urls",
        "literal": "literals",
        "class": "ontology_classes",
        "ontology_class": "ontology_classes",
        "message": "messages",
        "thread": "threads",
        "org": "orgs",
        "organization": "orgs",
        "organizations": "orgs",
        "handle": "handles",
        "claim": "claims",
        "task": "tasks",
        "header": "headers",
    }
    return aliases.get(value, value)


def kind_prefixes(kind: str) -> list[str]:
    """Return graph node prefixes for a normalized kind."""
    normalized = normalize_kind(kind or "")
    prefixes = {
        "addresses": ["gmeow:address/%"],
        "entities": ["gmeow:entity/%"],
        "projects": ["gmeow:project/%"],
        "labels": ["gmeow:label/%"],
        "attachments": ["gmeow:attachment/%"],
        "documents": ["gmeow:document/%"],
        "events": ["gmeow:event/%"],
        "archive_files": ["gmeow:archiveFile/%"],
        "document_authors": ["gmeow:documentAuthor/%"],
        "calendar_attendees": ["gmeow:calendarAttendee/%"],
        "attachment_evidence": ["gmeow:attachmentEvidence/%"],
        "ocr_entities": ["gmeow:ocrEntity/%"],
        "vision_entities": ["gmeow:visionEntity/%"],
        "document_entities": ["gmeow:documentEntity/%"],
        "calendar_entities": ["gmeow:calendarEntity/%"],
        "urls": ["gmeow:url/%"],
        "orgs": ["gmeow:org/%"],
        "handles": ["gmeow:org/@%"],
        "messages": ["gmeow:message/%"],
        "threads": ["gmeow:thread/%"],
    }
    return prefixes.get(normalized, [])


def message_search_fields(sender: str, recipients: str, message_date: str) -> dict[str, Any]:
    """Normalize fields used by PostgreSQL message search indexes."""
    sender_addr = clean_address(parseaddr(sender or "")[1])
    sender_domain = address_domain(sender_addr)
    recipient_addrs = sorted(
        {address for _name, raw_address in getaddresses([recipients or ""]) if (address := clean_address(raw_address))}
    )
    recipient_domains = sorted({domain for address in recipient_addrs if (domain := address_domain(address))})
    return {
        "message_ts": query_datetime(parse_message_date(message_date)) if message_date else None,
        "sender_addr": sender_addr or None,
        "sender_domain": sender_domain or None,
        "recipient_addrs": recipient_addrs,
        "recipient_domains": recipient_domains,
    }


def message_search_text(fields: dict[str, Any]) -> str:
    """Return full-text search input for a message row."""
    return "\n".join(_message_search_parts(fields))


def _message_search_parts(fields: dict[str, Any]) -> list[str]:
    """Return non-empty text fragments for message search indexing."""
    subject = fields.get("subject")
    sender = fields.get("sender")
    recipients = fields.get("recipients")
    snippet = fields.get("snippet")
    text_body = fields.get("text_body")
    markdown = fields.get("markdown")
    headers = cast(dict[str, object], fields.get("headers") or {})
    label_ids = [str(label_id) for label_id in cast(list[object], fields.get("label_ids") or [])]
    header_text = " ".join(f"{key}: {value}" for key, value in sorted((headers or {}).items()) if isinstance(value, str))
    parts = [subject, sender, recipients, snippet, " ".join(label_ids), header_text, text_body, markdown]
    return [str(part) for part in parts if part]


def sql_text_query(fts_query: str) -> str:
    """Convert local FTS syntax into PostgreSQL websearch text."""
    values = re.findall(r'"([^"]+)"', fts_query or "")
    if values:
        return " ".join(values)
    return fts_query.strip()


def apply_fts_filter(where: list[str], params: list[Any], fts_query: str) -> str:
    """Append PostgreSQL full-text filters and return rank SQL."""
    sql_query_text = sql_text_query(fts_query)
    if not sql_query_text:
        return "0.0"
    where.append("search_tsv @@ websearch_to_tsquery('simple', %s)")
    params.append(sql_query_text)
    params.append(sql_query_text)
    return "ts_rank_cd(search_tsv, websearch_to_tsquery('simple', %s))"


def apply_address_filters(where: list[str], params: list[Any], values: list[str], field: str) -> None:
    """Append sender or recipient address filters."""
    for value in values:
        address, domain = address_filter_value(value)
        if field == "sender":
            append_sender_filter(where, params, value, address, domain)
        else:
            append_recipient_filter(where, params, value, address, domain)


def append_sender_filter(where: list[str], params: list[Any], value: str, address: str, domain: str) -> None:
    """Append sender search predicates."""
    if address:
        where.append("(sender_addr = %s OR sender ILIKE %s)")
        params.extend([address, f"%{value}%"])
    elif domain:
        where.append("(sender_domain = %s OR sender ILIKE %s)")
        params.extend([domain, f"%{value}%"])
    else:
        where.append("sender ILIKE %s")
        params.append(f"%{value}%")


def append_recipient_filter(where: list[str], params: list[Any], value: str, address: str, domain: str) -> None:
    """Append recipient search predicates."""
    if address:
        where.append("(%s = ANY(recipient_addrs) OR recipients ILIKE %s)")
        params.extend([address, f"%{value}%"])
    elif domain:
        where.append("(%s = ANY(recipient_domains) OR recipients ILIKE %s)")
        params.extend([domain, f"%{value}%"])
    else:
        where.append("recipients ILIKE %s")
        params.append(f"%{value}%")


def apply_subject_label_filters(where: list[str], params: list[Any], parsed: dict[str, Any]) -> None:
    """Append subject and label filters from a parsed search query."""
    for value in parsed["subject"]:
        where.append("subject ILIKE %s")
        params.append(f"%{value}%")
    for label in parsed["label"]:
        where.append("label_ids ? %s")
        params.append(label)


def apply_date_filters(where: list[str], params: list[Any], parsed: dict[str, Any]) -> tuple[datetime, datetime]:
    """Append date filters from a parsed search query."""
    after_dt = DEFAULT_DATETIME
    before_dt = DEFAULT_DATETIME
    after = str(parsed["after"] or "")
    before = str(parsed["before"] or "")
    if after:
        after_dt = query_datetime(after)
        where.append("message_ts >= %s")
        params.append(after_dt)
    if before:
        before_dt = query_datetime(before)
        where.append("message_ts <= %s")
        params.append(before_dt)
    return after_dt, before_dt


def query_datetime(value: str) -> datetime:
    """Coerce a user query date into a timezone-aware datetime."""
    if not value:
        msg = "Query date is required."
        raise ValueError(msg)
    parsed = parse_message_date(value)
    try:
        result = datetime.fromisoformat(parsed)
    except (TypeError, ValueError) as exc:
        msg = f"Invalid query date: {value!r}"
        raise ValueError(msg) from exc
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def address_filter_value(value: str) -> tuple[str, str]:
    """Return normalized address and domain filters for a raw query value."""
    raw = (parseaddr(value or "")[1] or value or "").strip().lower()
    if raw.startswith("@"):
        return "", raw[1:]
    if "@" in raw:
        address = clean_address(raw)
        return address, address_domain(address)
    clean = raw.strip("<> ")
    if "." in clean and " " not in clean:
        return "", clean
    return "", ""


def clean_address(value: str) -> str:
    """Normalize an email address."""
    return (value or "").strip().strip("<>").lower()


def address_domain(address: str) -> str:
    """Return the domain part of an email address."""
    if not address or "@" not in address:
        return ""
    return address.rsplit("@", 1)[-1].strip().lower()


PROJECT_TABLES = [
    "content_objects",
    "content_object_refs",
    "labels",
    "threads",
    "messages",
    "message_parts",
    "attachments",
    "sync_state",
    "priority_rules",
    "graph_triples",
    "graph_node_profiles",
    "graph_edge_stats",
    "summary_items",
    "intelligence_jobs",
    "dead_letter_jobs",
    "operational_events",
    "sync_runs",
    "retention_policies",
    "attachment_metadata_versions",
    "imap_mailboxes",
    "imap_message_uids",
    "archive_exports",
    "categories",
    "message_categories",
    "category_rules",
    "category_overrides",
    "learned_category_runs",
    "people_aliases",
    "embedding_chunks",
    "ingest_issues",
]
