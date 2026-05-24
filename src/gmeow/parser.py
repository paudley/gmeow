# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import base64
from dataclasses import dataclass, field
from email import policy
from email.parser import BytesParser
from typing import Any


@dataclass(slots=True)
class ParsedPart:
    part_id: str
    mime_type: str
    filename: str | None = None
    body_text: str | None = None
    attachment_id: str | None = None
    size: int = 0
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ParsedMessage:
    gmail_id: str
    thread_id: str | None
    label_ids: list[str]
    snippet: str
    headers: dict[str, str]
    parts: list[ParsedPart]
    text: str
    subject: str | None
    sender: str | None
    recipients: str | None
    date: str | None


def decode_gmail_data(data: str | None) -> bytes:
    if not data:
        return b""
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _headers(raw_headers: list[dict[str, str]] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for header in raw_headers or []:
        name = header.get("name")
        value = header.get("value")
        if name and value is not None:
            result[name.lower()] = value
    return result


def _walk_payload(payload: dict[str, Any], prefix: str = "0") -> list[ParsedPart]:
    headers = _headers(payload.get("headers"))
    body = payload.get("body", {})
    mime_type = payload.get("mimeType") or "application/octet-stream"
    filename = payload.get("filename") or None
    data = decode_gmail_data(body.get("data"))
    text: str | None = None
    if data and (mime_type.startswith("text/") or mime_type == "message/rfc822"):
        text = data.decode("utf-8", errors="replace")
    part = ParsedPart(
        part_id=payload.get("partId", prefix),
        mime_type=mime_type,
        filename=filename,
        body_text=text,
        attachment_id=body.get("attachmentId"),
        size=int(body.get("size", 0) or len(data)),
        headers=headers,
    )
    parts = [part]
    for index, child in enumerate(payload.get("parts", []) or []):
        parts.extend(_walk_payload(child, f"{prefix}.{index}"))
    return parts


def parse_gmail_message(message: dict[str, Any]) -> ParsedMessage:
    payload = message.get("payload", {})
    headers = _headers(payload.get("headers"))
    parts = _walk_payload(payload)
    text_parts = [part.body_text for part in parts if part.body_text and part.mime_type.startswith("text/")]
    return ParsedMessage(
        gmail_id=message["id"],
        thread_id=message.get("threadId"),
        label_ids=list(message.get("labelIds", [])),
        snippet=message.get("snippet", ""),
        headers=headers,
        parts=parts,
        text="\n\n".join(text_parts),
        subject=headers.get("subject"),
        sender=headers.get("from"),
        recipients=headers.get("to"),
        date=headers.get("date"),
    )


def parse_rfc822(raw: bytes) -> tuple[dict[str, str], str]:
    email_message = BytesParser(policy=policy.default).parsebytes(raw)
    headers = {key.lower(): str(value) for key, value in email_message.items()}
    body = email_message.get_body(preferencelist=("plain", "html"))
    return headers, body.get_content() if body else ""
