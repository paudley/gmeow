# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Parse Gmail API payloads into the Gmeow ``ParsedMessage`` shape.

The parser folds Gmail JSON, header intelligence, body parts, and attachment references into a
single dataclass that downstream cache, markdown, and intelligence modules consume. Keeping the
parsing here lets the rest of the codebase stay agnostic of Gmail's wire format.
"""

import base64
from dataclasses import dataclass, field
from email import policy
from email.parser import BytesParser
from typing import Any, cast

DEFAULT_RAW_HEADERS = cast(list[dict[str, str]], None)


def _empty_headers() -> dict[str, str]:
    return {}


@dataclass(slots=True)
class ParsedPart:
    """Represent ParsedPart data and behavior."""

    part_id: str
    mime_type: str
    filename: str = ""
    body_text: str = ""
    attachment_id: str = ""
    size: int = 0
    headers: dict[str, str] = field(default_factory=_empty_headers)


@dataclass(slots=True)
class ParsedMessage:
    """Represent ParsedMessage data and behavior."""

    gmail_id: str
    thread_id: str
    label_ids: list[str]
    snippet: str
    headers: dict[str, str]
    parts: list[ParsedPart]
    text: str
    subject: str
    sender: str
    recipients: str
    date: str


def decode_gmail_data(data: str = "") -> bytes:
    """Decode gmail data."""
    if not data:
        return b""
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _headers(raw_headers: Any = DEFAULT_RAW_HEADERS) -> dict[str, str]:
    result: dict[str, str] = {}
    items: list[dict[str, str]] = list(raw_headers or [])
    for header in items:
        name = header.get("name")
        value = header.get("value")
        if name and value is not None:
            result[name.lower()] = value
    return result


def _walk_payload(payload: dict[str, Any], prefix: str = "0") -> list[ParsedPart]:
    headers = _headers(payload.get("headers"))
    body = payload.get("body", {})
    mime_type = payload.get("mimeType") or "application/octet-stream"
    filename = payload.get("filename") or ""
    data = decode_gmail_data(body.get("data", ""))
    text = ""
    if data and (mime_type.startswith("text/") or mime_type == "message/rfc822"):
        text = data.decode("utf-8", errors="replace")
    part = ParsedPart(
        part_id=payload.get("partId", prefix),
        mime_type=mime_type,
        filename=filename,
        body_text=text,
        attachment_id=body.get("attachmentId", ""),
        size=int(body.get("size", 0) or len(data)),
        headers=headers,
    )
    parts = [part]
    for index, child in enumerate(payload.get("parts", []) or []):
        parts.extend(_walk_payload(child, f"{prefix}.{index}"))
    return parts


def parse_gmail_message(message: dict[str, Any]) -> ParsedMessage:
    """Parse gmail message."""
    payload = message.get("payload", {})
    headers = _headers(payload.get("headers"))
    parts = _walk_payload(payload)
    text_parts = [part.body_text for part in parts if part.body_text and part.mime_type.startswith("text/")]
    return ParsedMessage(
        gmail_id=message["id"],
        thread_id=message.get("threadId", ""),
        label_ids=list(message.get("labelIds", [])),
        snippet=message.get("snippet", ""),
        headers=headers,
        parts=parts,
        text="\n\n".join(text_parts),
        subject=headers.get("subject", ""),
        sender=headers.get("from", ""),
        recipients=headers.get("to", ""),
        date=headers.get("date", ""),
    )


def parse_rfc822(raw: bytes) -> tuple[dict[str, str], str]:
    """Parse rfc822."""
    email_message = BytesParser(policy=policy.default).parsebytes(raw)
    headers = {key.lower(): str(value) for key, value in email_message.items()}
    body = email_message.get_body(preferencelist=("plain", "html"))
    return headers, body.get_content() if body else ""
