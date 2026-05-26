# Python Package Tests

These tests validate the `gmeow-intel` external analyzer package. They focus on schema behavior,
importability, and deterministic adapter output for NER and categorization.

Queue consumption, dispatch, and FILESTORE result persistence are tested in Go because the Go worker
owns those runtime responsibilities.
