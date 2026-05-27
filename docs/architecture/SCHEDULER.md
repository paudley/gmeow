# SCHEDULER Architecture

SCHEDULER owns RabbitMQ and all runtime work coordination. No other Gmeow
service opens RabbitMQ connections or manages queues directly.

## Responsibilities

SCHEDULER derives work from FILESTORE notifications, analyzer specs, forced
interface requests, failed jobs, repair requests, and projection needs. It owns
priority, retry, exponential backoff, dead-letter routing, requeue, and
deterministic job identity.

Production RabbitMQ vhost and queue prefix are deployment configuration, not
hardcoded runtime policy. The production example uses the `gmeow-prod` vhost
and `gmeow.` queue prefix. Integration tests use the `gmeow-test` vhost and
`gmeow.test.` queue prefix.

## Analysis Jobs

Analysis jobs are scoped by object digest, analyzer name, and analyzer version.
This keeps retries granular: a failed summary endpoint call reschedules only the
summary job, not already-complete NER, categorization, embedding, header, or
metadata annotations.

ANALYSIS workers consume scheduler-delivered jobs and ack only after durable
FILESTORE writes. If a worker crashes before ack, RabbitMQ redelivers. If a
worker crashes after the annotation write, the next attempt skips the completed
analyzer output by checking FILESTORE.

## Projection Jobs

FILESTORE object and annotation changes can require QUERY refresh. SCHEDULER
decides and calls QUERY over gRPC. QUERY owns PostgreSQL; SCHEDULER owns when
projection refresh should happen.

Projection refresh queue messages carry exact object digests. Runtime refresh
processing loads those named objects from FILESTORE and calls QUERY projection
for each object. It must not walk the FILESTORE root; full scans are explicit
operator workflows.

## Admin Operations

`gmeow-admin scheduler` exposes scan, status, failed-job inspection,
dead-letter inspection, requeue, and forced scheduling workflows. Destructive or
production-like operations require explicit instance confirmation.
