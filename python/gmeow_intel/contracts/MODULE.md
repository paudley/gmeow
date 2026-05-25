# Analysis Contracts

This package contains Python validation models for analyzer specs, analyzer jobs, and emitted
annotations. They mirror the Go Phase 00 contract shapes closely enough for package smoke tests
and future queue-bound worker code.

The models reject unknown fields. Contract expansion should happen intentionally on both the Go
and Python sides rather than by accepting arbitrary payload drift.
