<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc. -->
<!-- SPDX-License-Identifier: AGPL-3.0-only -->

# Architecture Boundaries

Gmeow is a service-oriented system. Services communicate with each other over
gRPC contracts; external protocols such as REST, IMAP, and MCP terminate at the
interface service and are translated into internal service calls.

## Ownership Rules

- FILESTORE owns object bytes, manifests, compound parts, annotations, overlays,
  provenance, and source ingest claims.
- QUERY owns PostgreSQL. Other services do not open PostgreSQL connections,
  run migrations, or issue SQL; they use QUERY service contracts.
- SCHEDULER owns RabbitMQ. RabbitMQ is only a scheduler coordination detail:
  scheduling, retry, failure, dead-letter, and worker job delivery all go
  through scheduler-owned code. FILESTORE notifies SCHEDULER over gRPC when
  authoritative object state changes; SCHEDULER decides whether that means
  missing/stale analysis work, QUERY projection refresh, or both.
- ANALYSIS only talks to FILESTORE. An analyzer receives scheduled work, reads
  object manifests and parts from FILESTORE, and writes annotations back to
  FILESTORE. It does not talk to Gmail, QUERY, PostgreSQL, RabbitMQ, or other
  backends directly. Configured analyzers must produce real annotations or fail
  closed; regex, keyword, placeholder, and other degraded substitute outputs are
  production bugs.
- Analyzer versions are part of the production contract. The email cutover
  versions are `phase04-email-v2` for Go analyzers and `python-email-v1` for
  Python analyzers; older `phase04` and `python-current` annotations are stale
  and must be rescheduled by SCHEDULER before cutover validation passes.
- BACKENDS talk to their backend APIs and FILESTORE-facing ingest contracts.
  Gmail, local, and push sources hydrate data into FILESTORE; they do not run
  analysis, query PostgreSQL, or coordinate RabbitMQ.
- Gmail backfill is a BACKEND/SOURCE workflow. It pages Gmail, hydrates
  messages, writes Gmail compound objects and source cursors to FILESTORE, and
  stops there. FILESTORE notification then lets SCHEDULER derive analysis and
  projection work through the normal scheduler-owned RabbitMQ path.
- INTERFACE talks to QUERY and BACKENDS through application service contracts.
  It does not own storage, queue, analysis, or database transport details.

## Flow

1. BACKENDS hydrate source data into FILESTORE.
2. FILESTORE notifies SCHEDULER after object, annotation, and overlay writes.
3. SCHEDULER schedules missing/stale analysis work and coordinates RabbitMQ
   delivery. Analysis annotation writes enqueue QUERY projection refresh only;
   they must not force unrelated analyzers to rerun.
4. ANALYSIS workers consume scheduler-provided jobs, read from FILESTORE, and
   write FILESTORE annotations. If one analyzer fails, ANALYSIS routes that
   job back to SCHEDULER failure handling; SCHEDULER retries that analyzer job
   with exponential backoff without rescheduling successful analyzer outputs.
   Before running an analyzer, ANALYSIS asks FILESTORE whether the exact
   analyzer name/version annotation already exists and skips complete work.
5. SCHEDULER processes projection refresh work by calling QUERY over gRPC;
   QUERY projects FILESTORE manifests and annotations into PostgreSQL.
6. INTERFACE serves REST, IMAP, MCP, and CLI workflows by calling QUERY,
   FILESTORE-facing retrieval services, SCHEDULER, and BACKENDS through typed
   service boundaries.

## Concurrency And Latency

Services must be internally concurrent and must not serialize unrelated
requests. Request handlers should keep low-latency work on the request path and
push blocking backend, database, queue, or analysis work behind the owning
service boundary. Long-running work should be scheduled or streamed instead of
holding an interface request open when a typed asynchronous path exists.

The architecture gate in `internal/contracts/phase_gate_test.go` enforces the
core dependency ownership rules so package imports cannot silently bypass these
boundaries.
