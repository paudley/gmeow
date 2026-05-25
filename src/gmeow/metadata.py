# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Extract attachment metadata via external probes.

This module shells out to ``exiftool`` (when available) and parses its JSON output into the
dict shapes the object store and knowledge graph consume. It isolates the external-tool boundary
so the rest of the pipeline can stay synchronous and deterministic.
"""

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
