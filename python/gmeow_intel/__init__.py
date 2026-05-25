# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Python analysis worker package for Gmeow.

The package is intentionally small in Phase 00 because runtime queue consumption is introduced
in the ANALYSIS migration phase. It publishes the import surface that Go-side scheduler contracts
can target without starting a worker process.

See Also:
    MODULE.md: Package contract for the Phase 00 Python analysis package.

"""

__all__ = ["__version__"]

__version__ = "0.0.0"
