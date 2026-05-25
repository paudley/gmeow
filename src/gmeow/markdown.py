# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Render Gmail messages as Gmeow markdown summaries.

The renderer formats parsed messages and header intelligence into the markdown shape used by the
cache and the MCP surfaces. It keeps presentation logic out of ``parser`` and ``cache`` so the
canonical artifacts remain rebuildable.
"""

import re

from .headers import analyze_headers
from .parser import ParsedMessage


def message_to_markdown(message: ParsedMessage, attachment_sha1s: list[str] | None = None) -> str:
    """Message to markdown."""
    attachment_sha1s = attachment_sha1s or []
    lines = [
        f"# {message.subject or '(no subject)'}",
        "",
        f"- Gmail ID: `{message.gmail_id}`",
        f"- Thread ID: `{message.thread_id or ''}`",
        f"- From: {message.sender or ''}",
        f"- To: {message.recipients or ''}",
        f"- Date: {message.date or ''}",
        f"- Labels: {', '.join(message.label_ids)}",
        "",
    ]
    lines.extend(_header_intelligence_lines(message))
    lines.extend(_attachment_lines(attachment_sha1s))
    lines.extend(_critical_fact_lines(message))
    lines.extend(["## Body", "", message.text or message.snippet or ""])
    return "\n".join(lines).strip() + "\n"


def _header_intelligence_lines(message: ParsedMessage) -> list[str]:
    intel = analyze_headers(message.headers)
    if not (intel.message_id or intel.authentication_results or intel.warnings):
        return []
    lines = ["## Header Intelligence", ""]
    optional_lines = [
        ("Message-ID", intel.message_id),
        ("Reply-To", intel.reply_to),
        ("List-ID", intel.list_id),
        ("Delivered-To", intel.delivered_to),
        ("Received hops", str(intel.received_hops) if intel.received_hops else None),
    ]
    lines.extend(f"- {label}: `{value}`" for label, value in optional_lines if value)
    if intel.spf or intel.dkim or intel.dmarc:
        lines.append(f"- Auth: SPF `{intel.spf or 'unknown'}`, DKIM `{intel.dkim or 'unknown'}`, DMARC `{intel.dmarc or 'unknown'}`")
    lines.extend(f"- Warning: `{warning}`" for warning in intel.warnings)
    lines.append("")
    return lines


def _attachment_lines(attachment_sha1s: list[str]) -> list[str]:
    if not attachment_sha1s:
        return []
    return ["## Attachments", "", *(f"- `{sha1}`" for sha1 in attachment_sha1s), ""]


def _critical_fact_lines(message: ParsedMessage) -> list[str]:
    facts = critical_facts(message.text or message.snippet or "")
    if not any(facts.values()):
        return []
    lines = ["## Critical Facts", ""]
    lines.extend(f"- {key.replace('_', ' ').title()}: {', '.join(values[:8])}" for key, values in facts.items() if values)
    lines.append("")
    return lines


def critical_facts(text: str) -> dict[str, list[str]]:
    """Critical facts."""
    value = text or ""
    facts = {
        "dates": _unique(
            re.findall(
                r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?(?:day)?[,]?\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?[,]?\s+\d{4}\b|\b\d{4}-\d{2}-\d{2}\b",
                value,
                re.IGNORECASE,
            )
        ),
        "money": _unique(re.findall(r"\$\s?\d[\d,]*(?:\.\d{2})?", value)),
        "emails": _unique(re.findall(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", value)),
        "phones": _unique(re.findall(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", value)),
        "urls": _unique(re.findall(r"https?://[^\s<>\"]+", value)),
        "action_sentences": [],
    }
    for sentence in re.split(r"(?<=[.!?])\s+", value):
        if re.search(r"\b(action required|please|todo|must|need to|due|deadline|by \w+day)\b", sentence, re.IGNORECASE):
            facts["action_sentences"].append(sentence.strip()[:300])
    facts["action_sentences"] = _unique(facts["action_sentences"])
    return facts


def _unique(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.strip().rstrip(".,)")
        key = cleaned.lower()
        if cleaned and key not in seen:
            seen.add(key)
            result.append(cleaned)
    return result
