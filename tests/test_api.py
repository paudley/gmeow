# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import os

from fastapi.testclient import TestClient
import pytest

from gmeow.app import create_app
from gmeow.config import GmeowConfig


TEST_DSN = os.environ.get("GMEOW_TEST_POSTGRES_DSN")
def test_health_and_no_delete_or_send_routes(tmp_path) -> None:
    if not TEST_DSN:
        pytest.skip("GMEOW_TEST_POSTGRES_DSN is required for API integration tests")
    app = create_app(GmeowConfig(data_dir=tmp_path, postgres_dsn=TEST_DSN))
    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.get("/api/v1/health", headers={"host": "127.0.0.1"})
    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert client.post("/api/v1/messages/m1/send", headers={"host": "127.0.0.1"}).status_code == 404
    assert client.delete("/api/v1/messages/m1", headers={"host": "127.0.0.1"}).status_code == 405


def test_loopback_guard_rejects_non_loopback_host(tmp_path) -> None:
    if not TEST_DSN:
        pytest.skip("GMEOW_TEST_POSTGRES_DSN is required for API integration tests")
    app = create_app(GmeowConfig(data_dir=tmp_path, postgres_dsn=TEST_DSN))
    client = TestClient(app, base_url="http://127.0.0.1")
    response = client.get("/api/v1/health", headers={"host": "example.com"})
    assert response.status_code == 403
