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

from gmeow_intel.analyzers import categories, ner
from gmeow_intel.contracts import Annotation, ExternalCommandRequest

AnalyzerFunc = Callable[[ExternalCommandRequest], Annotation]

ANALYZERS: dict[str, AnalyzerFunc] = {
    ner.analyzer_name(): ner.analyze,
    categories.analyzer_name(): categories.analyze,
}


def main() -> None:
    """Run a configured external analyzer once."""
    parser = argparse.ArgumentParser(prog="gmeow-intel-worker")
    subcommands = parser.add_subparsers(dest="command", required=True)

    analyze = subcommands.add_parser("analyze")
    analyze.add_argument("analyzer")
    subcommands.add_parser("discover-categories")

    args = parser.parse_args()
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
