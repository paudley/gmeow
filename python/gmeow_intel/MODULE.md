# Python Analysis Package

`gmeow_intel` contains the Python-side ANALYSIS external adapter package promised by the Go
migration plan. It provides contract models and explicitly configured analyzer commands for
quality-critical Python/model behavior that has not been replaced by equal-or-better Go analyzers.

The package must not parse operator configuration, start queues, write FILESTORE state, or own
scheduling. Go owns startup, config validation, RabbitMQ, FILESTORE writes, and ack/nack behavior.
