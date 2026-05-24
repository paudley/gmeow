# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Parse and classify Gmail message headers.

This module turns raw RFC822 header rows into the ``HeaderIntelligence`` record that downstream
search, graph, and category pipelines consume. It centralizes auto-reply detection, list
signaling, and authentication-result extraction so producers see consistent fields.
"""

import re
from dataclasses import dataclass, field


def _empty_list() -> list[str]:
    return []


@dataclass(slots=True)
class HeaderIntelligence:
    """Represent HeaderIntelligence data and behavior."""

    message_id: str = ""
    references: list[str] = field(default_factory=_empty_list)
    list_id: str = ""
    list_unsubscribe: str = ""
    delivered_to: str = ""
    reply_to: str = ""
    return_path: str = ""
    precedence: str = ""
    auto_submitted: str = ""
    authentication_results: str = ""
    received_hops: int = 0
    spf: str = ""
    dkim: str = ""
    dmarc: str = ""
    warnings: list[str] = field(default_factory=_empty_list)


def analyze_headers(headers: dict[str, str]) -> HeaderIntelligence:
    """Analyze headers."""
    auth = headers.get("authentication-results", "")
    intel = HeaderIntelligence(
        message_id=headers.get("message-id", ""),
        references=re.findall(r"<[^>]+>", headers.get("references", "")),
        list_id=headers.get("list-id", ""),
        list_unsubscribe=headers.get("list-unsubscribe", ""),
        delivered_to=headers.get("delivered-to", ""),
        reply_to=headers.get("reply-to", ""),
        return_path=headers.get("return-path", ""),
        precedence=headers.get("precedence", ""),
        auto_submitted=headers.get("auto-submitted", ""),
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
