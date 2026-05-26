# Analyzer Modules

This package contains Python-native external analyzer adapters such as categorization and
named-entity extraction. They are invoked by the Go ANALYSIS worker only when explicitly configured
with a command, arguments, timeout, analyzer name, and analyzer version.

Analyzer implementations consume validated contract models from `gmeow_intel.contracts` and emit
JSON-shaped annotations that match the Go FILESTORE annotation contract. They must not parse
operator config, connect to RabbitMQ, or write FILESTORE state directly.
