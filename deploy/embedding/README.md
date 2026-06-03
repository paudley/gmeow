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

## Option B — Docker Compose (the working path on Arch/CachyOS)

Reproducible, pinned, isolated, and — importantly — it ships a **complete**
runtime. The Arch/CachyOS repo `ollama` package (0.30.0) is only the Go CLI; it
carries no `llama-server` inference runner, so `--manage-model` against the
pacman binary fails with `error starting llama-server: binary not found`. The
Docker image (and the upstream `install.sh`) bundle the runner + GPU libs.

From the repo root:

```
make embedding-up      # start Ollama + pull the model
make embedding-down    # stop
```

Then point gmeow at it:

```toml
[analysis.embeddings]
endpoint = "http://127.0.0.1:11434/v1/embeddings"
model    = "nomic-embed-text"
```

and run `gmeow embedding-serve` (no `--manage-model`). Because gmeow controls
this backend, drive it hard: `--embed-batch-size 128 --embed-batch-tokens 6000
--embed-pace-ms 0`.

GPU: `docker-compose.yml` is wired for **AMD ROCm** — image `ollama/ollama:rocm`,
`/dev/kfd` + `/dev/dri` passed in, `render`/`video` groups joined, and
`HSA_OVERRIDE_GFX_VERSION=11.0.0` so ROCm accepts the new gfx1151 (Strix Halo /
Radeon 8060S) by treating it as gfx1100. For NVIDIA, switch to
`ollama/ollama:latest` + the nvidia runtime; for CPU-only, use `:latest` and drop
the `devices`/`group_add`/`HSA` block. CPU is stable, just slower on the one-time
cold embed (the claim-vector cache makes re-runs near-instant).
