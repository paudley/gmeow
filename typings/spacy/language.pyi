# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

from typing import Any

class Language:
    def __call__(self, text: str) -> Any: ...
    def add_pipe(self, name: str) -> Any: ...
