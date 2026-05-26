# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Contracts for the retired root Python runtime.

The root Python entrypoint should now point operators at the Go binaries.
This test prevents the legacy Python CLI from becoming an operator path again.
"""

from pathlib import Path


def test_root_python_entrypoint_points_to_go_runtime() -> None:
    """The root Python app must not reintroduce operator runtime behavior."""
    content = Path("main.py").read_text(encoding="utf-8")

    assert "Python runtime CLI is retired" in content
    assert "cmd/gmeow" in content
