# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Python external analyzer adapter package for Gmeow.

The Go worker owns queue consumption, FILESTORE access, config loading, and ack/nack behavior.
This package provides the narrow command surface used for explicitly configured Python/model
analyzers such as NER and categorization.

See Also:
    MODULE.md: Package contract for Python ANALYSIS external adapters.

"""

__all__ = ["__version__"]

__version__ = "0.0.0"
