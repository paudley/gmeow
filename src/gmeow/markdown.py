# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import re

from .headers import analyze_headers
from .parser import ParsedMessage


def message_to_markdown(message: ParsedMessage, attachment_sha1s: list[str] | None = None) -> str:
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
    intel = analyze_headers(message.headers)
    if intel.message_id or intel.authentication_results or intel.warnings:
        lines.extend(["## Header Intelligence", ""])
        if intel.message_id:
            lines.append(f"- Message-ID: `{intel.message_id}`")
        if intel.reply_to:
            lines.append(f"- Reply-To: {intel.reply_to}")
        if intel.list_id:
            lines.append(f"- List-ID: `{intel.list_id}`")
        if intel.delivered_to:
            lines.append(f"- Delivered-To: {intel.delivered_to}")
        if intel.received_hops:
            lines.append(f"- Received hops: {intel.received_hops}")
        if intel.spf or intel.dkim or intel.dmarc:
            lines.append(f"- Auth: SPF `{intel.spf or 'unknown'}`, DKIM `{intel.dkim or 'unknown'}`, DMARC `{intel.dmarc or 'unknown'}`")
        for warning in intel.warnings:
            lines.append(f"- Warning: `{warning}`")
        lines.append("")
    if attachment_sha1s:
        lines.extend(["## Attachments", ""])
        lines.extend(f"- `{sha1}`" for sha1 in attachment_sha1s)
        lines.append("")
    facts = critical_facts(message.text or message.snippet or "")
    if any(facts.values()):
        lines.extend(["## Critical Facts", ""])
        for key, values in facts.items():
            if values:
                lines.append(f"- {key.replace('_', ' ').title()}: {', '.join(values[:8])}")
        lines.append("")
    lines.extend(["## Body", "", message.text or message.snippet or ""])
    return "\n".join(lines).strip() + "\n"


def critical_facts(text: str) -> dict[str, list[str]]:
    value = text or ""
    facts = {
        "dates": _unique(re.findall(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?(?:day)?[,]?\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?[,]?\s+\d{4}\b|\b\d{4}-\d{2}-\d{2}\b", value, re.I)),
        "money": _unique(re.findall(r"\$\s?\d[\d,]*(?:\.\d{2})?", value)),
        "emails": _unique(re.findall(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", value)),
        "phones": _unique(re.findall(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", value)),
        "urls": _unique(re.findall(r"https?://[^\s<>\"]+", value)),
        "action_sentences": [],
    }
    for sentence in re.split(r"(?<=[.!?])\s+", value):
        if re.search(r"\b(action required|please|todo|must|need to|due|deadline|by \w+day)\b", sentence, re.I):
            facts["action_sentences"].append(sentence.strip()[:300])
    facts["action_sentences"] = _unique(facts["action_sentences"])
    return facts


def _unique(values: list[str]) -> list[str]:
    result = []
    seen = set()
    for value in values:
        cleaned = value.strip().rstrip(".,)")
        key = cleaned.lower()
        if cleaned and key not in seen:
            seen.add(key)
            result.append(cleaned)
    return result
