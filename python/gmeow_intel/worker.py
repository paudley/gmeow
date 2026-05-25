# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Phase 00 Python ANALYSIS worker entry point.

The command exists so packaging and release automation can install `gmeow-intel` consistently.
It exits explicitly because RabbitMQ consumption and analyzer dispatch are later-phase work.
"""

PHASE_00_WORKER_UNAVAILABLE = "gmeow-intel worker runtime is not implemented in Phase 00"


def main() -> None:
    """Start the worker.

    Phase 00 only defines the package and command surface. Runtime RabbitMQ consumption is added in
    the ANALYSIS phase.
    """
    raise SystemExit(PHASE_00_WORKER_UNAVAILABLE)
