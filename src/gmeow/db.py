# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Alembic migration wiring for the Gmeow PostgreSQL schema.

This module exposes the small wrapper that resolves the alembic config and runs upgrades on the
configured DSN. Callers (CLI, app bootstrap) use it so migration behavior stays consistent across
entry points.
"""

from pathlib import Path
from typing import cast

from alembic import command
from alembic.config import Config

DEFAULT_PATH = cast(Path, None)


def sqlalchemy_url(dsn: str) -> str:
    """Sqlalchemy url."""
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgresql://")
    return dsn


def run_migrations(dsn: str, root: Path = DEFAULT_PATH) -> None:
    """Run migrations."""
    project_root = root or Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "migrations"))
    config.set_main_option("sqlalchemy.url", sqlalchemy_url(dsn))
    command.upgrade(config, "head")
