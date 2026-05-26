# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Provide resilience functionality for Gmeow.

This module runs startup checks for cache, graph, storage, Gmail, and embedding dependencies. It
also exposes readiness and degraded-health summaries used by the API and CLI. Checks are best-effort:
each feature reports degraded state instead of preventing unrelated local operations from starting.
"""

import contextlib
from pathlib import Path
from typing import Any, Protocol, cast

from .http_json import HTTP_SERVER_ERROR, HttpJsonError, post_json

CHECK_EXCEPTIONS = (AttributeError, KeyError, TypeError, ValueError, RuntimeError, OSError)
DEFAULT_DICT_ANY = cast(dict[str, Any], None)
DEFAULT_OBJECT = cast(object, None)


class _ResilienceConfig(Protocol):
    """Configuration surface needed for resilience checks."""

    @property
    def object_store_dir(self) -> Path:
        """Return object-store directory."""
        ...

    @property
    def tantivy_dir(self) -> Path:
        """Return Tantivy directory."""
        ...

    @property
    def embedding_endpoint(self) -> str:
        """Return embedding endpoint."""
        ...

    @property
    def embedding_model(self) -> str:
        """Return embedding model."""
        ...


class _ResilienceCache(Protocol):
    """Cache surface needed for resilience checks."""

    def age_status(self) -> dict[str, Any]:
        """Return AGE graph status."""
        ...

    def storage_diagnostics(self) -> dict[str, Any]:
        """Return storage diagnostics."""
        ...

    def record_operational_event(
        self,
        event_type: str,
        severity: str,
        component: str,
        subject_id: str = "",
        detail: str = "",
        metadata: dict[str, Any] = DEFAULT_DICT_ANY,
    ) -> int:
        """Record an operational event."""
        _ = (event_type, severity, component, subject_id, detail, metadata)
        raise NotImplementedError

    def sync_status(self) -> dict[str, Any]:
        """Return sync status."""
        ...

    def resilience_status(self) -> dict[str, Any]:
        """Return resilience status."""
        ...


class _ResilienceSync(Protocol):
    """Sync surface needed for resilience checks."""

    def gmail_available(self) -> bool:
        """Return whether Gmail is configured."""
        ...


def startup_self_check(
    config: _ResilienceConfig, cache: _ResilienceCache, sync: _ResilienceSync, _semantic: object = DEFAULT_OBJECT
) -> dict[str, Any]:
    """Startup self check."""
    features: dict[str, dict[str, Any]] = {}
    features["postgres"] = _check("ok", "PostgreSQL cache opened and migrations ran.")
    features["age"] = _age_check(cache)
    features["tantivy"] = _directory_check(config.tantivy_dir, "Tantivy directory is present.")
    features["materialized_views"] = _materialized_view_check(cache)
    features["gmail"] = _gmail_check(sync)
    features["embeddings"] = _embedding_check(getattr(config, "embedding_endpoint", ""), getattr(config, "embedding_model", ""))
    degraded = [name for name, feature in features.items() if feature["status"] != "ok"]
    status = {"ok": not degraded, "degraded": degraded, "features": features}
    with contextlib.suppress(*CHECK_EXCEPTIONS):
        cache.record_operational_event(
            "startup.self_check", "warning" if degraded else "info", "startup", "", "Startup self-check completed.", status
        )
    return status


def readiness(cache: _ResilienceCache, startup_status: dict[str, Any] = DEFAULT_DICT_ANY) -> dict[str, Any]:
    """Readiness."""
    try:
        status = cache.sync_status()
        ready = bool(status.get("counts", {}).get("messages", 0) >= 0)
    except CHECK_EXCEPTIONS as exc:
        return {"ready": False, "error": repr(exc), "startup": startup_status or {}}
    degraded = list((startup_status or {}).get("degraded", []))
    return {"ready": ready, "degraded": degraded, "counts": status.get("counts", {}), "startup": startup_status or {}}


def degraded_status(cache: _ResilienceCache, startup_status: dict[str, Any] = DEFAULT_DICT_ANY) -> dict[str, Any]:
    """Degraded status."""
    resilience = cache.resilience_status()
    features = dict((startup_status or {}).get("features", {}))
    return {
        "ok": resilience.get("ok") and not (startup_status or {}).get("degraded"),
        "startup": startup_status or {},
        "resilience": resilience,
        "features": features,
    }


def _age_check(cache: _ResilienceCache) -> dict[str, Any]:
    try:
        state = cache.age_status()
        return _check("ok" if state.get("available") else "degraded", state.get("error") or "AGE graph available.")
    except CHECK_EXCEPTIONS as exc:
        return _check("degraded", repr(exc))


def _directory_check(path: Path, ok_detail: str) -> dict[str, Any]:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return _check("ok", ok_detail)
    except CHECK_EXCEPTIONS as exc:
        return _check("degraded", repr(exc))


def _materialized_view_check(cache: _ResilienceCache) -> dict[str, Any]:
    try:
        matviews = cache.storage_diagnostics().get("materialized_views", [])
        missing = [view["matviewname"] for view in matviews if not view.get("ispopulated")]
        return _check(
            "degraded" if missing else "ok",
            "Unpopulated materialized views: " + ", ".join(missing) if missing else "Materialized views populated.",
        )
    except CHECK_EXCEPTIONS as exc:
        return _check("degraded", repr(exc))


def _gmail_check(sync: _ResilienceSync) -> dict[str, Any]:
    configured = sync.gmail_available()
    return _check("ok" if configured else "degraded", "Gmail client configured." if configured else "Gmail client is not configured.")


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
