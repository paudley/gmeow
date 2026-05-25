# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Encode Gmeow responses as the compact TOON format.

TOON is the default MCP/HTTP response shape because it stays under the context-window budget that
the agent surfaces care about. This module provides ``dumps``/``loads`` helpers so callers do not
have to reimplement the uniform-row compaction rules each time.
"""

import json
import re
from typing import Any, cast


def dumps(value: Any) -> str:
    """Serialize a value to TOON text."""
    return _encode(value, 0).rstrip() + "\n"


def _encode(value: Any, indent: int) -> str:
    pad = "  " * indent
    if isinstance(value, dict):
        mapping: dict[str, Any] = cast(Any, value)
        lines: list[str] = []
        for key, item in mapping.items():
            child: Any = item
            if isinstance(child, list):
                child_list: list[Any] = cast(Any, child)
                lines.append(_encode_named_list(str(key), child_list, indent))
            elif isinstance(child, dict):
                lines.append(f"{pad}{key}:")
                lines.append(_encode(child, indent + 1))
            else:
                lines.append(f"{pad}{key}: {_scalar(child)}")
        return "\n".join(line for line in lines if line != "")
    if isinstance(value, list):
        sequence: list[Any] = cast(Any, value)
        return _encode_named_list("items", sequence, indent)
    return f"{pad}{_scalar(value)}"


def _encode_named_list(name: str, values: list[Any], indent: int) -> str:
    pad = "  " * indent
    if not values:
        return f"{pad}{name}[0]:"
    if _uniform_object_array(values):
        fields = list(values[0].keys())
        lines = [f"{pad}{name}[{len(values)}]{{{','.join(fields)}}}:"]
        lines.extend(f"{pad}  " + ",".join(_cell(item.get(field)) for field in fields) for item in values)
        return "\n".join(lines)
    if all(not isinstance(item, (dict, list)) for item in values):
        return f"{pad}{name}[{len(values)}]: " + ",".join(_cell(item) for item in values)
    lines = [f"{pad}{name}[{len(values)}]:"]
    for item in values:
        if isinstance(item, dict):
            lines.append(f"{pad}  -")
            lines.append(_encode(item, indent + 2))
        else:
            lines.append(f"{pad}  - {_scalar(item)}")
    return "\n".join(lines)


def _uniform_object_array(values: list[Any]) -> bool:
    if not values or not all(isinstance(item, dict) for item in values):
        return False
    fields = list(values[0].keys())
    return all(list(item.keys()) == fields and all(not isinstance(item.get(field), (dict, list)) for field in fields) for item in values)


def _cell(value: object) -> str:
    if value is None:
        return ""
    text = _scalar(value)
    if any(char in text for char in [",", "\n", "{", "}", "[", "]"]):
        return json.dumps(str(value), ensure_ascii=False)
    return text


def _scalar(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text == "":
        return '""'
    if re.search(r"[:#,\n{}\[\]]", text) or text.strip() != text:
        return json.dumps(text, ensure_ascii=False)
    return text
