# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Entry point that delegates to the Gmeow CLI.

This module exists so ``python main.py`` and ``python -m main`` route through the same
``gmeow.cli.main`` Typer application as the installed ``gmeow`` script. Operators and tests run
the same code path through every invocation.
"""

from gmeow.cli import main

if __name__ == "__main__":
    main()
