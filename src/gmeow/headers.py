# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide headers functionality for Gmeow."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(slots=True)
class HeaderIntelligence:
    """Represent HeaderIntelligence data and behavior."""

    message_id: str | None = None
    references: list[str] = field(default_factory=list)
    list_id: str | None = None
    list_unsubscribe: str | None = None
    delivered_to: str | None = None
    reply_to: str | None = None
    return_path: str | None = None
    precedence: str | None = None
    auto_submitted: str | None = None
    authentication_results: str | None = None
    received_hops: int = 0
    spf: str | None = None
    dkim: str | None = None
    dmarc: str | None = None
    warnings: list[str] = field(default_factory=list)


def analyze_headers(headers: dict[str, str]) -> HeaderIntelligence:
    """Analyze headers."""
    auth = headers.get("authentication-results")
    intel = HeaderIntelligence(
        message_id=headers.get("message-id"),
        references=re.findall(r"<[^>]+>", headers.get("references", "")),
        list_id=headers.get("list-id"),
        list_unsubscribe=headers.get("list-unsubscribe"),
        delivered_to=headers.get("delivered-to"),
        reply_to=headers.get("reply-to"),
        return_path=headers.get("return-path"),
        precedence=headers.get("precedence"),
        auto_submitted=headers.get("auto-submitted"),
        authentication_results=auth,
        received_hops=sum(1 for key in headers if key == "received"),
    )
    if auth:
        for key in ["spf", "dkim", "dmarc"]:
            match = re.search(rf"\b{key}=(\w+)", auth, re.IGNORECASE)
            if match:
                setattr(intel, key, match.group(1).lower())
    sender = headers.get("from", "")
    return_path = headers.get("return-path", "")
    if sender and return_path and "@" in sender and "@" in return_path:
        sender_domain = sender.split("@")[-1].strip(" >").lower()
        return_domain = return_path.split("@")[-1].strip(" >").lower()
        if sender_domain != return_domain:
            intel.warnings.append("from_domain_differs_from_return_path")
    if intel.auto_submitted and intel.auto_submitted.lower() != "no":
        intel.warnings.append("automated_message")
    if intel.precedence and intel.precedence.lower() in {"bulk", "list", "junk"}:
        intel.warnings.append(f"precedence_{intel.precedence.lower()}")
    return intel
