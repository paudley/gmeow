# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

"""Python ANALYSIS external adapter entry point.

The Go worker runtime owns RabbitMQ, FILESTORE writes, config loading, scheduling, and ack/nack
behavior. This command is intentionally a narrow stdin/stdout adapter for quality-critical Python
analyzers that are explicitly configured by the Go worker.
"""

import argparse
import json
import sys
from collections.abc import Callable
from typing import Any, cast

from pydantic import ValidationError

from gmeow_intel.analyzers import categories, ner
from gmeow_intel.contracts import Annotation, ExternalCommandRequest

AnalyzerFunc = Callable[[ExternalCommandRequest], Annotation]

ANALYZERS: dict[str, AnalyzerFunc] = {
    ner.analyzer_name(): ner.analyze,
    categories.analyzer_name(): categories.analyze,
}

WarmupFunc = Callable[[], None]

# Analyzers whose models load lazily expose a warmup so a persistent backend can
# finish loading before it reports ready. Analyzers without an entry need none.
WARMUPS: dict[str, WarmupFunc] = {
    ner.analyzer_name(): ner.warmup,
}


def main() -> None:
    """Run a configured external analyzer once."""
    parser = argparse.ArgumentParser(prog="gmeow-intel-worker")
    subcommands = parser.add_subparsers(dest="command", required=True)

    analyze = subcommands.add_parser("analyze")
    analyze.add_argument("analyzer")
    serve = subcommands.add_parser("serve")
    serve.add_argument("analyzer")
    subcommands.add_parser("discover-categories")

    args = parser.parse_args()
    if args.command == "serve":
        _serve(args.analyzer)
        return
    if args.command == "discover-categories":
        messages = json.loads(sys.stdin.read())
        if not isinstance(messages, list):
            print("discover-categories expects a JSON array of message documents", file=sys.stderr)
            raise SystemExit(2)
        sys.stdout.write(json.dumps(categories.discover(_message_documents(cast(list[object], messages))), separators=(",", ":")))
        return

    if args.command != "analyze":
        parser.error("unsupported command")

    analyzer = ANALYZERS.get(args.analyzer)
    if analyzer is None:
        print("unsupported analyzer:", args.analyzer, file=sys.stderr)
        raise SystemExit(2)

    request = ExternalCommandRequest.model_validate_json(sys.stdin.read())
    if request.job.analyzer.name != args.analyzer:
        print(
            "configured analyzer does not match job analyzer:",
            args.analyzer,
            "!=",
            request.job.analyzer.name,
            file=sys.stderr,
        )
        raise SystemExit(2)

    annotation = analyzer(request)
    sys.stdout.write(json.dumps(annotation.model_dump(mode="json"), separators=(",", ":")))


def _serve(analyzer_name: str) -> None:
    """Serve one analyzer as a persistent backend, one object per stdin line.

    Models load once here, not per object. A ready sentinel is emitted only after
    warmup so the Go worker never sends work to a backend still loading models.
    Each stdin line is one ExternalCommandRequest; each stdout line is a framed
    response: {"annotation": ...} on success or {"error": ...} on a per-object
    failure (the backend keeps serving). EOF on stdin ends the loop.
    """
    analyzer = ANALYZERS.get(analyzer_name)
    if analyzer is None:
        print("unsupported analyzer:", analyzer_name, file=sys.stderr)
        raise SystemExit(2)

    WARMUPS.get(analyzer_name, lambda: None)()
    _write_line({"ready": True})

    for raw in sys.stdin:
        payload = raw.strip()
        if not payload:
            continue
        _write_line(_run_one(analyzer_name, analyzer, payload))


def _run_one(analyzer_name: str, analyzer: AnalyzerFunc, payload: str) -> dict[str, Any]:
    """Process one request line, returning a framed response.

    An expected per-object failure -- a malformed request or a data/inference
    error from the analyzer -- becomes an {"error": ...} response so the
    persistent backend keeps serving subsequent objects. Interpreter-level
    faults (e.g. MemoryError, SystemError) are left to propagate so the Go
    worker that supervises this subprocess can recycle it.
    """
    try:
        request = ExternalCommandRequest.model_validate_json(payload)
    except ValidationError as exc:
        return {"error": f"decode request: {exc}"}

    if request.job.analyzer.name != analyzer_name:
        return {"error": f"job analyzer {request.job.analyzer.name} does not match served {analyzer_name}"}

    try:
        annotation = analyzer(request)
    except (ValueError, LookupError, TypeError, AttributeError, ArithmeticError, RuntimeError, OSError) as exc:
        return {"error": str(exc)}

    return {"annotation": annotation.model_dump(mode="json")}


def _write_line(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _message_documents(value: list[object]) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            print("discover-categories expects message objects", file=sys.stderr)
            raise SystemExit(2)
        messages.append(cast(dict[str, Any], item))

    return messages


if __name__ == "__main__":
    main()
