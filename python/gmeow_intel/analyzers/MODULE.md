# Analyzer Modules

This package reserves the namespace for Python-native analyzers such as categorization and
named-entity extraction. Phase 00 modules are placeholders because analyzer execution and RabbitMQ
delivery are introduced in the ANALYSIS phase.

Analyzer implementations added later must consume validated contract models from
`gmeow_intel.contracts` and emit JSON-shaped annotations that match the Go scheduler contract.
