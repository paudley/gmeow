# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import re
from typing import Any


def dumps(value: Any) -> str:
    return _encode(value, 0).rstrip() + "\n"


def _encode(value: Any, indent: int) -> str:
    pad = "  " * indent
    if isinstance(value, dict):
        lines = []
        for key, item in value.items():
            if isinstance(item, list):
                lines.append(_encode_named_list(str(key), item, indent))
            elif isinstance(item, dict):
                lines.append(f"{pad}{key}:")
                lines.append(_encode(item, indent + 1))
            else:
                lines.append(f"{pad}{key}: {_scalar(item)}")
        return "\n".join(line for line in lines if line != "")
    if isinstance(value, list):
        return _encode_named_list("items", value, indent)
    return f"{pad}{_scalar(value)}"


def _encode_named_list(name: str, values: list[Any], indent: int) -> str:
    pad = "  " * indent
    if not values:
        return f"{pad}{name}[0]:"
    if _uniform_object_array(values):
        fields = list(values[0].keys())
        lines = [f"{pad}{name}[{len(values)}]{{{','.join(fields)}}}:"]
        for item in values:
            lines.append(f"{pad}  " + ",".join(_cell(item.get(field)) for field in fields))
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


def _cell(value: Any) -> str:
    if value is None:
        return ""
    text = _scalar(value)
    if any(char in text for char in [",", "\n", "{", "}", "[", "]"]):
        return json.dumps(str(value), ensure_ascii=False)
    return text


def _scalar(value: Any) -> str:
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
