# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
from typing import Any
from urllib import request


def startup_self_check(config: Any, cache: Any, sync: Any, semantic: Any | None = None) -> dict[str, Any]:
    features: dict[str, dict[str, Any]] = {}
    features["postgres"] = _check("ok", "PostgreSQL cache opened and migrations ran.")
    try:
        cache.reclaim_stale_intelligence_jobs()
        features["job_recovery"] = _check("ok", "Stale intelligence jobs reclaimed.")
    except Exception as exc:
        features["job_recovery"] = _check("degraded", repr(exc))
    try:
        state = cache.age_status()
        features["age"] = _check("ok" if state.get("available") else "degraded", state.get("error") or "AGE graph available.")
    except Exception as exc:
        features["age"] = _check("degraded", repr(exc))
    try:
        config.object_store_dir.mkdir(parents=True, exist_ok=True)
        probe = config.object_store_dir / ".gmeow-write-check"
        probe.write_text("ok")
        probe.unlink(missing_ok=True)
        features["object_store"] = _check("ok", "Object store is writable.")
    except Exception as exc:
        features["object_store"] = _check("degraded", repr(exc))
    try:
        config.tantivy_dir.mkdir(parents=True, exist_ok=True)
        features["tantivy"] = _check("ok", "Tantivy directory is present.")
    except Exception as exc:
        features["tantivy"] = _check("degraded", repr(exc))
    try:
        matviews = cache.storage_diagnostics().get("materialized_views", [])
        missing = [view["matviewname"] for view in matviews if not view.get("ispopulated")]
        features["materialized_views"] = _check("degraded" if missing else "ok", "Unpopulated materialized views: " + ", ".join(missing) if missing else "Materialized views populated.")
    except Exception as exc:
        features["materialized_views"] = _check("degraded", repr(exc))
    features["gmail"] = _check("ok" if sync.gmail is not None else "degraded", "Gmail client configured." if sync.gmail is not None else "Gmail client is not configured.")
    features["embeddings"] = _embedding_check(getattr(config, "embedding_endpoint", ""), getattr(config, "embedding_model", ""))
    degraded = [name for name, feature in features.items() if feature["status"] != "ok"]
    status = {"ok": not degraded, "degraded": degraded, "features": features}
    try:
        cache.record_operational_event("startup.self_check", "warning" if degraded else "info", "startup", None, "Startup self-check completed.", status)
    except Exception:
        pass
    return status


def readiness(cache: Any, startup_status: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        status = cache.sync_status()
        ready = bool(status.get("counts", {}).get("messages", 0) >= 0)
    except Exception as exc:
        return {"ready": False, "error": repr(exc), "startup": startup_status or {}}
    degraded = list((startup_status or {}).get("degraded", []))
    return {"ready": ready, "degraded": degraded, "counts": status.get("counts", {}), "startup": startup_status or {}}


def degraded_status(cache: Any, startup_status: dict[str, Any] | None = None) -> dict[str, Any]:
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
        payload = json.dumps({"model": model, "input": ["health check"]}).encode()
        req = request.Request(endpoint, data=payload, headers={"content-type": "application/json"}, method="POST")
        with request.urlopen(req, timeout=5.0) as response:
            return _check("ok" if response.status < 500 else "degraded", f"Embedding endpoint HTTP {response.status}.")
    except Exception as exc:
        return _check("degraded", repr(exc))


def _check(status: str, detail: str) -> dict[str, Any]:
    return {"status": status, "detail": detail}
