# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide metadata functionality for Gmeow."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


def extract_exiftool_metadata(path: Path) -> dict[str, Any]:
    """Extract exiftool metadata."""
    try:
        result = subprocess.run(
            ["exiftool", "-json", "-G", "-struct", str(path)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        return {"available": False, "error": "exiftool not found"}
    except subprocess.TimeoutExpired:
        return {"available": False, "error": "exiftool timed out"}
    if result.returncode != 0:
        return {"available": False, "error": result.stderr.strip() or result.stdout.strip()}
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {"available": False, "error": f"invalid exiftool JSON: {exc}"}
    if not payload:
        return {"available": True, "tags": {}}
    tags = dict(payload[0])
    tags.pop("SourceFile", None)
    return {"available": True, "tags": tags}
