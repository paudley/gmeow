# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Internal typing helpers used to narrow dynamic dict payloads.

Gmeow ingests Gmail, OpenAI, and exiftool payloads that all arrive as ``dict[str, Any]``. Several
modules need to safely descend into nested dict keys while keeping pyright/mypy from widening the
narrowed branch back to ``Unknown``. The helpers here centralize that repeated cast pattern in one
place so the rest of the codebase stays terse.
"""

from typing import Any, cast


def ensure_dict(parent: dict[str, Any], key: str) -> dict[str, Any]:
    """Return ``parent[key]`` as a typed dict or an empty dict when the value is missing.

    The helper combines the common ``isinstance`` guard and ``cast`` step so callers can replace
    ``cast(dict[str, Any], parent.get(key)) if isinstance(parent.get(key), dict) else {}`` with a
    single call. Non-dict values (including ``None``) collapse to an empty dict, which matches the
    existing semantics at every replaced call site.
    """
    value = parent.get(key)
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}
