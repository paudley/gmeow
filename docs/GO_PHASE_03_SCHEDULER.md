# Go Migration Phase 03 - SCHEDULER And RabbitMQ

## Goal

Implement SCHEDULER as the work-derivation layer for analysis and projection. It creates prioritized,
idempotent, retryable jobs from FILESTORE state, analyzer specs, interface requests, and repair
events.

## Build

- Define analyzer specs and versioning.
- Derive missing/stale work from FILESTORE annotations.
- Generate deterministic idempotency keys.
- Configure RabbitMQ exchanges, queues, priority classes, retries, and dead letters.
  Production RabbitMQ queues must be prefixed with `gmeow.`; integration-test queues must be
  prefixed with `gmeow.test.`.
  Production connections use the RabbitMQ `gmeow` vhost; integration tests use `gmeow-test`.
- Expose SCHEDULER through typed gRPC `SchedulerService` over the configured Unix socket
  (`/run/gmeow/scheduler.sock` by default). RPC requests use protobuf messages, not whole-request
  JSON envelopes.
- Implement scheduler scan command and background loop.
- Implement forced reanalysis and interactive reprioritization.
- Emit projection refresh requests after FILESTORE changes where needed.
- Add `gmeow-admin scheduler scan`, `status`, `requeue`, and dead-letter inspection commands.

## Retire Python Equivalent

After Go SCHEDULER owns work derivation and RabbitMQ dispatch, remove old Python maintenance and job
orchestration code:

- `src/gmeow/maintenance.py`
- the retired Python PostgreSQL job queue path
- scheduler-like paths in `src/gmeow/sync_backfill.py` that only exist to enqueue old work
- CLI commands that enqueue old Python intelligence or maintenance jobs

## Functional Proof

- Repeated scheduler scans do not create duplicate effective work.
- Interactive jobs outrank background jobs.
- Failed jobs retry with backoff and then land in dead letter.
- Analyzer version changes schedule only affected outputs.
- Worker crash before ack redelivers work.
- Worker crash after durable FILESTORE write does not duplicate outputs.

## Exit Gate

- SCHEDULER owns priority, retry, dead-letter, and idempotency behavior.
- RabbitMQ queues are durable and named under the `gmeow.` or `gmeow.test.` prefix.
- `gmeow scheduler-serve` runs the gRPC server and registers `SchedulerService`.
- ANALYSIS workers receive jobs only through the scheduler/broker contract.
- Old Python maintenance/job orchestration is removed once Go SCHEDULER is authoritative.
