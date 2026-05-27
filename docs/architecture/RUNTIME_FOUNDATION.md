# Runtime Foundation And Contracts

This document describes the current foundation shared by all Go services.

## Binaries And Packages

The runtime is built from three Go binaries:

- `gmeow` for service servers and user-facing CLI workflows.
- `gmeow-admin` for operator/admin workflows.
- `gmeow-worker` for ANALYSIS workers.

Shared packages provide contracts, config loading, app services, service RPC
clients, observability, and component implementations. Public behavior should
enter through the binaries or typed gRPC services, not through ad hoc package
imports across ownership boundaries.

## Configuration

All Go binaries use the same `gmeow.toml` parser. Config selection is through
`--config`, `GMEOW_CONFIG`, or the default local file. Secondary config pointers
inside TOML are rejected.

Startup requires a SOPS age identity from `GMEOW_SOPS_UNLOCK_KEY` or the local
Gmeow key file. Referenced secret leaves must be SOPS-encrypted JSON leaf
envelopes. Components receive resolved values, not unresolved secret names.

Config validation is a startup gate. It validates service endpoints, loopback
listener addresses, RabbitMQ/PostgreSQL defaults, scheduler prefixes, source
capabilities, inbox/backfill runtime options, and explicit external analyzer
adapter commands.

## Contracts

The runtime uses typed Go contracts and protobuf services for internal service
communication:

- FILESTORE, QUERY, SCHEDULER, and SOURCE expose gRPC service contracts.
- INTERFACE uses application services backed by those gRPC clients.
- ANALYSIS workers consume scheduler work and write FILESTORE annotations.
- Dynamic metadata maps may use JSON leaf encoding, but service envelopes are
  typed protobuf/Go structures.

## Quality Gates

The canonical gate is `make check`. It formats Go, runs vet and Go tests,
builds Go binaries, tests and builds `gmeow-intel`, runs public release hygiene,
and checks whitespace.

Architecture tests in `internal/contracts/phase_gate_test.go` enforce package
ownership boundaries, retained external analyzer scope, public documentation
requirements, and anti-placeholder analyzer behavior.
