# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""HTTP JSON helpers.

Provides a tiny wrapper around ``http.client`` that posts JSON payloads and parses JSON responses
without dragging in the full ``requests`` dependency. The single ``HttpJsonError`` exception keeps
network and decoding failures uniform across callers.
"""

import http.client
import json
from typing import Any, cast
from urllib.parse import urlsplit

HTTP_SERVER_ERROR = 500


class HttpJsonError(RuntimeError):
    """Raised when an HTTP JSON request fails."""

    def __init__(self, status: int, body: str) -> None:
        """Initialize HttpJsonError."""
        super().__init__(f"HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body


def post_json(endpoint: str, payload: dict[str, Any], *, timeout: float) -> tuple[int, dict[str, Any]]:
    """Post JSON to an HTTP or HTTPS endpoint."""
    parsed = urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        msg = f"Unsupported HTTP endpoint: {endpoint!r}"
        raise ValueError(msg)

    body = json.dumps(payload).encode()
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    connection_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    connection = connection_cls(parsed.hostname, parsed.port, timeout=timeout)
    try:
        connection.request("POST", path, body=body, headers={"content-type": "application/json"})
        response = connection.getresponse()
        response_body = response.read().decode(errors="replace")
    finally:
        connection.close()

    if response.status >= HTTP_SERVER_ERROR:
        raise HttpJsonError(response.status, response_body)
    if not response_body:
        return response.status, {}
    data = json.loads(response_body)
    if not isinstance(data, dict):
        msg = "HTTP JSON endpoint returned a non-object response"
        raise TypeError(msg)
    return response.status, cast(dict[str, Any], data)
