# SCHEDULER Architecture

SCHEDULER owns RabbitMQ and all runtime work coordination. No other Gmeow
service opens RabbitMQ connections or manages queues directly.

SCHEDULER is an **in-memory-only coordinator**. It holds no durable state: if it
restarts, it loses its working set and rebuilds it from events and (rarely) from
a bounded self-heal sweep. It **never writes to FILESTORE** — FILESTORE is the
single source of truth (see `docs/architecture/ANALYSIS.md` and the data-store
rules below), and the only writes to it are a new object or a completed analysis
annotation, both produced by workers, never by the scheduler.

## Core invariants

These are load-bearing. The system melted in production when the implementation
violated them (see "Post-mortem"); they are requirements, not guidelines.

1. **No coordination state in FILESTORE — no "annotations about annotations."**
   - "Is this analysis done?" → read the object's `Kind="analysis"` annotations.
   - "Is this job scheduled?" → the queue.
   - The scheduler MUST NOT persist `"publishing"`/`"enqueued"`/`"complete"`
     markers (or any bookkeeping) as annotations. FILESTORE write volume must be
     bounded by real work completed, never by scheduling activity.
2. **No O(corpus) operation in any routine path.** The target corpus is ~15M
   messages / ~0.5TB. Walking the store, peeking the whole queue, or re-deriving
   the world are forbidden in the hot path. Full scans are self-heal backstops
   only (bounded, idle-time) or explicit operator workflows.
3. **The scheduler self-limits on its own pressure.** A runaway must
   circuit-break itself; recovery must never require an operator to remove the
   config file to stop a storm.

## Hot path (event-driven)

1. A new object or completed annotation is written to FILESTORE. FILESTORE emits
   a change notice that **calls** the scheduler (`NotifyObjectsChanged`).
2. For each notified object the scheduler decides what analysis is still needed —
   a pure diff of the object's `Kind="analysis"` annotations against the
   configured analyzer set (`jobsForObject`). It enqueues the missing analyzers
   to RabbitMQ. This is the only normal driver of analysis work.
3. A worker **consumes** the queue, calls the analyzer (in-process or an external
   gRPC/model service), and writes exactly one `Kind="analysis"` annotation. The
   annotation is the done-record; ack happens only after the durable write.

### Dedup is not done at enqueue

A redundant job is cheap: the worker checks `HasAnalysisAnnotation` and skip-acks
already-satisfied work. The queue is the scheduled-state-of-record and is never
scanned in the hot path. To make the common "already complete" case free, the
scheduler keeps a small **in-process LRU of permanent-positive answers** to
"does this object have all its annotations?" — once true it is always true
(annotations never disappear), so caching the positive is sound and avoids the
annotation reload. The LRU is in-memory only; losing it on restart costs nothing
but a few reloads.

## Backpressure (the scheduler throttles ingest)

The scheduler watches its own pressure (unprocessed change-notices and analysis
queue depth). When the backlog is too deep it tells the source to **pause
backfill scheduling**, and resumes it when pressure drains below a threshold.
Ingest otherwise runs as fast as Gmail and disk allow — the source does not
self-regulate; the scheduler, which has the global pressure view, regulates it.

## Self-heal sweep (backstop, never operations)

A scheduler-owned sweep catches work missed by dropped change-notices. It is
**not** the normal driver.

- State is **in-memory only** (reset loses your place; that is fine).
- It walks via a **cursor with a random start point and wrap-around**, in bounded
  chunks, so it never holds the whole store.
- It is triggered by an admin call **or** an idle timer that arms **only after
  writes have occurred since the last sweep** — no writes means nothing to heal,
  so it does not run. The scheduler knows its own idleness from its pressure.

## Analysis Jobs

Analysis jobs are scoped by object digest, analyzer name, and analyzer version,
so retries stay granular: a failed summary call reschedules only the summary job,
not already-complete NER, categorization, embedding, header, or metadata output.
Retry, exponential backoff, and dead-letter routing live entirely in RabbitMQ —
never in FILESTORE markers.

## Source Import Boundary

Archive import does not use SCHEDULER-owned queues. The admin source import
command walks operator-supplied archive roots locally, parses messages, and
writes them directly to FILESTORE over gRPC. FILESTORE is the first durable
boundary for imported mail; the scheduler is involved only after FILESTORE emits
object-change notifications for stored or changed objects.

ANALYSIS workers consume scheduler-delivered jobs and ack only after durable
FILESTORE writes. If a worker crashes before ack, RabbitMQ redelivers. If a
worker crashes after the annotation write, the next attempt skips the completed
analyzer output by checking FILESTORE.

## Projection Jobs

FILESTORE object and annotation changes can require QUERY refresh. SCHEDULER
decides and calls QUERY over gRPC. QUERY owns PostgreSQL; SCHEDULER owns when
projection refresh should happen. The QUERY projection is a disposable derivation
of FILESTORE: wipe and rebuild it freely.

Projection refresh queue messages carry exact object digests. Runtime refresh
processing loads those named objects from FILESTORE and calls QUERY projection
for each object. It must not walk the FILESTORE root; full scans are explicit
operator workflows or the bounded self-heal sweep.

## Admin Operations

`gmeow-admin scheduler` exposes status, failed-job inspection, dead-letter
inspection, requeue, forced scheduling, and the explicit self-heal sweep.
Destructive or production-like operations require explicit instance confirmation.

## Post-mortem: the marker-churn meltdown (2026-05)

The shipped implementation ran a 30-second `WalkProjection` reconcile that, for
every object missing work, wrote `"publishing"` then `"enqueued"` marker
annotations into FILESTORE. On a corpus being firehosed in by a `mode=full`
Gmail backfill this produced:

- A metadata LSM bloated to ~2GB for ~61k objects (~33KB/object) and ~131GB of
  lifetime writes against a 4GB store — pure coordination write-amplification.
- A scheduler↔worker disagreement (`jobsForObject` said "missing", the worker's
  idempotency check said "present") that thrashed the queue without progress.
- An unkillable storm: systemd restarted the services and the reconcile re-armed,
  so only removing the config file stopped it.

Every one of these is a symptom of the scheduler owning durable state, polling
the whole corpus, and running blind to its own pressure. The invariants above
exist so this failure mode is impossible by construction, not merely tuned away.
