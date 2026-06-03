# gmeow embedding backend

Ingest-time contact resolution needs an embedding model (`nomic-embed-text`,
served OpenAI-style at `/v1/embeddings`). Rather than requiring you to run a
model server by hand, gmeow can **provide and control its own** via Ollama.
There are two ways, both auto-restarting so a model crash recovers instead of
stalling an import.

## Option A — gmeow supervises Ollama (no Docker)

Requires the `ollama` binary on `PATH`
([install](https://ollama.com/download) or
`curl -fsSL https://ollama.com/install.sh | sh`). Then:

```
gmeow embedding-serve --config gmeow.toml --manage-model --state-file ./data/resolution.state
```

gmeow spawns and supervises `ollama serve`, pulls `nomic-embed-text` on first
run, points its embedder at it, and restarts it if it dies. Flags:
`--ollama-host` (default `127.0.0.1:11434`), `--ollama-model`
(default `nomic-embed-text`), `--ollama-models-dir` (put weights under gmeow's
control).

## Option B — Docker Compose

Reproducible, pinned, isolated. From the repo root:

```
make embedding-up      # start Ollama + pull the model
make embedding-down    # stop
```

Then point gmeow at it (already the default in `gmeow.toml`):

```toml
[analysis.embeddings]
endpoint = "http://127.0.0.1:11434/v1/embeddings"
model    = "nomic-embed-text"
```

and run a plain `gmeow embedding-serve` (no `--manage-model`).

GPU: Ollama auto-detects accelerators. For NVIDIA-in-Docker, install
`nvidia-container-toolkit` and uncomment the `deploy.resources` block in
`docker-compose.yml`. CPU is the portable default — stable, just slower on the
one-time cold embed (the claim-vector cache makes re-runs near-instant).
