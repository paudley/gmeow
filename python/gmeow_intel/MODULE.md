# Python Analysis Package

`gmeow_intel` contains the Python-side ANALYSIS package promised by the Go migration plan. Phase
00 keeps this package limited to importable contracts, analyzer namespaces, and a command stub so
release automation can build the package before worker runtime behavior exists.

The package must not parse operator configuration or start queues in Phase 00. Go owns startup,
config validation, and scheduler-facing contracts during this phase.
