# Gmeow embedding backend

All Gmeow embedding callers use the EMBEDDING gRPC service. Workers, importers,
QUERY commands, and interfaces must never call an embedding HTTP endpoint
directly, and `gmeow.toml` must not contain embedding endpoint/model settings.

`gmeow embedding-serve` owns the embedding model backend. It supervises Ollama,
ensures the built-in model is present, stores model data under Gmeow state, and
exposes only the EMBEDDING gRPC API to the rest of Gmeow.

Systemd starts it as part of `gmeow-core.target`:

```sh
gmeow --config /etc/gmeow/gmeow.toml embedding-serve --state-file /var/lib/gmeow/embedding.state
```

The Docker Compose file in this directory is retained only as an operator
escape hatch for bringing up an Ollama-compatible backend during manual
diagnostics. It is not a Gmeow integration point and must not be referenced from
`gmeow.toml`.
