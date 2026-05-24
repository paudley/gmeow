# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config


def sqlalchemy_url(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgresql://")
    return dsn


def run_migrations(dsn: str, root: Path | None = None) -> None:
    project_root = root or Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "migrations"))
    config.set_main_option("sqlalchemy.url", sqlalchemy_url(dsn))
    command.upgrade(config, "head")
