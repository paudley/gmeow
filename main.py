# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Retired Python runtime entry point.

The Go Phase 00 binaries own operator startup and config validation.
"""

if __name__ == "__main__":
    raise SystemExit("Python runtime CLI is retired; use cmd/gmeow or cmd/gmeow-admin.")
