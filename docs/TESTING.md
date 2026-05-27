# Testing Architecture

## Immutable Rule

THE ONLY THING THAT MAY EVER BE MOCKED, FAKED, SIMULATED, OR OTHERWISE EMULATED
IN TESTING IS FULLY EXTERNAL APIS AND SERVICES.

This rule is primary and unassailable. It applies to unit tests, integration
tests, contract tests, workflow tests, CLI tests, server tests, and release
checks.

## What Must Not Be Mocked

Do not mock, fake, simulate, emulate, stub, or replace any Gmeow-owned component
or service boundary:

- FILESTORE
- QUERY
- SCHEDULER
- ANALYSIS
- SOURCE
- INTERFACE
- application services
- gRPC service implementations
- gRPC clients and service interfaces
- admin command behavior
- config loading and validation
- migrations and projection logic
- queue/retry/dead-letter behavior
- analyzer runtime behavior

Tests must use the real Gmeow implementation for these boundaries. If a test is
too hard to write without mocking an internal component, the implementation or
test seam is wrong and must be fixed.

## Component Seams

ALL seams between major components MUST use the already-present gRPC interfaces.
Internally calling code inside another component's server is BANNED.

This applies to production code and tests. A test for ANALYSIS reading
FILESTORE state must go through `FilestoreService`; a SOURCE workflow that
submits content must go through `FilestoreService`; application-service or
interface tests that cross QUERY or SCHEDULER boundaries must use the relevant
service interface rather than importing concrete server internals.

The only code allowed to call a component's internal implementation directly is
that component's own package-local tests for private behavior. Cross-component
tests must prove the deployed service boundary, not a shortcut around it.

## What May Be Mocked

Only fully external APIs and services may be mocked, faked, simulated, or
emulated. Examples:

- Google Gmail and Drive APIs
- OpenAI-compatible embedding or model endpoints
- external analyzer executables or model runtimes not implemented by Gmeow
- operating-system process execution when testing command construction
- network services not owned by Gmeow

Prefer protocol-faithful external test servers, such as `httptest.Server`, over
call-count mocks. External doubles must model the external protocol boundary,
not internal Gmeow behavior.

## Required Testing Shape

- FILESTORE tests use real temporary FILESTORE roots.
- QUERY tests use the real PostgreSQL implementation when QUERY behavior matters.
- SCHEDULER tests use the real RabbitMQ-backed broker when queue semantics
  matter.
- ANALYSIS tests use the real worker runtime and real FILESTORE gRPC service.
- SOURCE tests use real SOURCE adapters and the real FILESTORE gRPC service;
  only the remote provider API may be replaced.
- Gmail backfill tests must use the real Gmail SOURCE adapter and real FILESTORE
  gRPC service. The Google Gmail API may be replaced by a protocol-faithful fake
  that supports pagination, message hydrate, and history cursor expiry.
- INTERFACE tests use real application services underneath protocol handlers,
  and those services must cross component boundaries through gRPC clients.
- CLI tests execute real command paths and validate real side effects.
- Workflow tests compose real Gmeow components end to end.

## Contract Tests

Contract tests are still required, but they must run against real
implementations. They exist to enforce behavior across public interfaces, not to
justify replacement backends for Gmeow-owned code.

Required contract coverage:

- `filestore.Store`: object identity, facets, provenance, source lookup/claims,
  compound parts, annotations, overlays, verification, and projection walks.
- `query.Index`: projection, rebuild, changed projection, search filters,
  structure, relationships, graph, analyzer status, vector search, and source
  cursors.
- `scheduler.Broker`: publish, priority-visible status, failure routing,
  retry/dead-letter behavior, requeue, and projection refresh messages.
- gRPC services: typed protobuf conversion and streaming behavior.

## Workflow Tests

Workflow tests should prove operator-visible behavior:

- ingest bytes into FILESTORE, run ANALYSIS, rebuild QUERY, then search;
- force analysis through application services and observe real scheduler work;
- restore a FILESTORE root, verify it, rebuild QUERY, and retrieve content;
- route failed analysis jobs to retry/dead-letter and requeue them;
- serve through MCP stdio, MCP Streamable HTTP, REST, and IMAP while using real
  application services.

## Enforcement

Any new test double must identify the fully external API or service it replaces.
If the target is owned by Gmeow, the test double is forbidden.

The canonical local gate remains:

```bash
make check
```
