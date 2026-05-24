# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT

from . import language as language

def load(name: str) -> language.Language: ...
def blank(lang: str) -> language.Language: ...
