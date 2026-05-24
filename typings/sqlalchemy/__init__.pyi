# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT

from typing import Any

from . import pool as pool

def engine_from_config(configuration: dict[str, Any], prefix: str = ..., **kwargs: Any) -> Any: ...
