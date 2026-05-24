# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide public release check functionality for Gmeow."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPDX = "SPDX-License-Identifier: MIT"
REQUIRED_FILES = [
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "CONTRIBUTING.md",
    "CODE_OF_CONDUCT.md",
    "config.toml-example",
    "docs/PUBLIC_RELEASE_CHECKLIST.md",
    ".github/dependabot.yml",
    ".github/workflows/ci.yml",
]
SOURCE_SUFFIXES = {".py"}
SOURCE_EXTRA = {"migrations/script.py.mako"}
FORBIDDEN_TRACKED_PATHS = {
    "config.toml",
    "config/gmeow.yaml",
    "config/gmeow.example.yaml",
}
FORBIDDEN_TEXT = [
    "paud" + "ley",
    "hi" + "ve2",
    "104" + "306" + "190" + "268" + "698" + "565" + "204",
    "/home/" + "paud" + "ley",
    "mg" + "eow",
    "gmeow-gmail" + "@",
    "~/.ssh/" + "gloud/" + "gcats.sops.yaml",
    "Pat" + "rick " + "Aud" + "ley",
    "Jan" + "et " + "Mc" + "hugh",
    "(604) " + "785-5446",
    "bii-" + "north" + "point-unvr",
    "ry" + "an@" + "the" + "vancouver" + "life.com",
    "the" + "vancouver" + "life",
    "1325 " + "Rol" + "ston Street",
]


def main() -> int:
    """Run public release checks."""
    tracked = _tracked_files()
    tracked_set = set(tracked)

    failures = _missing_required_files()
    failures.extend(_forbidden_tracked_paths(tracked_set))
    failures.extend(_forbidden_text_matches(tracked))
    failures.extend(_missing_spdx_headers(tracked))

    failures.extend(_example_config_failures())
    if failures:
        for failure in failures:
            print(f"release-check: {failure}", file=sys.stderr)
        return 1
    print("release-check: ok")
    return 0


def _missing_required_files() -> list[str]:
    return [f"missing required file: {rel}" for rel in REQUIRED_FILES if not (ROOT / rel).exists()]


def _forbidden_tracked_paths(tracked_set: set[str]) -> list[str]:
    return [f"forbidden tracked local config path: {rel}" for rel in sorted(FORBIDDEN_TRACKED_PATHS & tracked_set) if (ROOT / rel).exists()]


def _forbidden_text_matches(tracked: list[str]) -> list[str]:
    failures: list[str] = []
    for rel in tracked:
        path = ROOT / rel
        if not path.is_file() or _skip_text_scan(rel):
            continue
        text = _read_text(path)
        lowered = text.lower()
        failures.extend(f"forbidden private string {needle!r} found in {rel}" for needle in FORBIDDEN_TEXT if needle.lower() in lowered)
    return failures


def _missing_spdx_headers(tracked: list[str]) -> list[str]:
    return [f"missing SPDX header: {rel}" for rel in tracked if _needs_spdx(rel) and SPDX not in _read_text(ROOT / rel, default="")]


def _example_config_failures() -> list[str]:
    failures: list[str] = []
    example = _read_text(ROOT / "config.toml-example", default="")
    if "[gmeow]" not in example:
        failures.append("config.toml-example must contain a [gmeow] table")
    if "[gmeow]" in example and any(value in example for value in ["user@your-domain.example", "paud" + "ley"]):
        failures.append("config.toml-example contains non-public identity placeholders")
    return failures


def _tracked_files() -> list[str]:
    output = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=ROOT, text=True)
    return [line for line in output.splitlines() if line]


def _needs_spdx(rel: str) -> bool:
    path = Path(rel)
    return path.suffix in SOURCE_SUFFIXES or rel in SOURCE_EXTRA


def _skip_text_scan(rel: str) -> bool:
    return rel in {".gitmodules", "uv.lock", "scripts/public_release_check.py"}


def _read_text(path: Path, default: str = "") -> str:
    try:
        return path.read_text()
    except UnicodeDecodeError:
        return default


if __name__ == "__main__":
    raise SystemExit(main())
