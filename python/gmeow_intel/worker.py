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

    args = parser.parse_args()
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


if __name__ == "__main__":
    main()
