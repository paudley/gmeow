# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Provide resilience functionality for Gmeow."""

from __future__ import annotations

import contextlib
from typing import Any

from .http_json import HTTP_SERVER_ERROR, HttpJsonError, post_json

CHECK_EXCEPTIONS = (AttributeError, KeyError, TypeError, ValueError, RuntimeError, OSError)


def startup_self_check(config: object, cache: object, sync: object, _semantic: object | None = None) -> dict[str, Any]:
    """Startup self check."""
    features: dict[str, dict[str, Any]] = {}
    features["postgres"] = _check("ok", "PostgreSQL cache opened and migrations ran.")
    try:
        cache.reclaim_stale_intelligence_jobs()
        features["job_recovery"] = _check("ok", "Stale intelligence jobs reclaimed.")
    except CHECK_EXCEPTIONS as exc:
        features["job_recovery"] = _check("degraded", repr(exc))
    try:
        state = cache.age_status()
        features["age"] = _check("ok" if state.get("available") else "degraded", state.get("error") or "AGE graph available.")
    except CHECK_EXCEPTIONS as exc:
        features["age"] = _check("degraded", repr(exc))
    try:
        config.object_store_dir.mkdir(parents=True, exist_ok=True)
        probe = config.object_store_dir / ".gmeow-write-check"
        probe.write_text("ok")
        probe.unlink(missing_ok=True)
        features["object_store"] = _check("ok", "Object store is writable.")
    except CHECK_EXCEPTIONS as exc:
        features["object_store"] = _check("degraded", repr(exc))
    try:
        config.tantivy_dir.mkdir(parents=True, exist_ok=True)
        features["tantivy"] = _check("ok", "Tantivy directory is present.")
    except CHECK_EXCEPTIONS as exc:
        features["tantivy"] = _check("degraded", repr(exc))
    try:
        matviews = cache.storage_diagnostics().get("materialized_views", [])
        missing = [view["matviewname"] for view in matviews if not view.get("ispopulated")]
        features["materialized_views"] = _check(
            "degraded" if missing else "ok",
            "Unpopulated materialized views: " + ", ".join(missing) if missing else "Materialized views populated.",
        )
    except CHECK_EXCEPTIONS as exc:
        features["materialized_views"] = _check("degraded", repr(exc))
    features["gmail"] = _check(
        "ok" if sync.gmail is not None else "degraded",
        "Gmail client configured." if sync.gmail is not None else "Gmail client is not configured.",
    )
    features["embeddings"] = _embedding_check(getattr(config, "embedding_endpoint", ""), getattr(config, "embedding_model", ""))
    degraded = [name for name, feature in features.items() if feature["status"] != "ok"]
    status = {"ok": not degraded, "degraded": degraded, "features": features}
    with contextlib.suppress(*CHECK_EXCEPTIONS):
        cache.record_operational_event(
            "startup.self_check", "warning" if degraded else "info", "startup", None, "Startup self-check completed.", status
        )
    return status


def readiness(cache: object, startup_status: dict[str, Any] | None = None) -> dict[str, Any]:
    """Readiness."""
    try:
        status = cache.sync_status()
        ready = bool(status.get("counts", {}).get("messages", 0) >= 0)
    except CHECK_EXCEPTIONS as exc:
        return {"ready": False, "error": repr(exc), "startup": startup_status or {}}
    degraded = list((startup_status or {}).get("degraded", []))
    return {"ready": ready, "degraded": degraded, "counts": status.get("counts", {}), "startup": startup_status or {}}


def degraded_status(cache: object, startup_status: dict[str, Any] | None = None) -> dict[str, Any]:
    """Degraded status."""
    resilience = cache.resilience_status()
    features = dict((startup_status or {}).get("features", {}))
    return {
        "ok": resilience.get("ok") and not (startup_status or {}).get("degraded"),
        "startup": startup_status or {},
        "resilience": resilience,
        "features": features,
    }


def _embedding_check(endpoint: str, model: str) -> dict[str, Any]:
    if not endpoint:
        return _check("degraded", "No embedding endpoint configured.")
    try:
        status, _ = post_json(endpoint, {"model": model, "input": ["health check"]}, timeout=5.0)
        return _check("ok" if status < HTTP_SERVER_ERROR else "degraded", f"Embedding endpoint HTTP {status}.")
    except (HttpJsonError, OSError, TimeoutError, ValueError, TypeError) as exc:
        return _check("degraded", repr(exc))


def _check(status: str, detail: str) -> dict[str, Any]:
    return {"status": status, "detail": detail}
