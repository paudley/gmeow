# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""NER analyzer placeholder for the ANALYSIS phase.

The Phase 00 package declares where named-entity analysis code will live without registering a
runtime worker. This keeps downstream packaging, imports, and documentation stable while Go owns
startup and configuration contracts.
"""


def analyzer_name() -> str:
    """Return the stable analyzer name."""
    return "ner"
