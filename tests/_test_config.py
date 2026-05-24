# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Resolve the Postgres test DSN from the local ``config.toml``.

This helper centralizes the lookup so individual test modules can read a single constant rather
than repeating filesystem and TOML logic. It returns an empty string when no local configuration
is available so callers can decide whether to skip or fail.
"""

import tomllib
from pathlib import Path

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.toml"


def _load_test_postgres_dsn() -> str:
    if not _CONFIG_PATH.exists():
        return ""
    with _CONFIG_PATH.open("rb") as handle:
        data = tomllib.load(handle)
    dsn = data.get("gmeow", {}).get("testing", {}).get("postgres_dsn", "")
    return dsn if isinstance(dsn, str) else ""


TEST_POSTGRES_DSN = _load_test_postgres_dsn()
