-- +goose Up
CREATE TABLE IF NOT EXISTS public.jmap_bearer_tokens (
  token_hash BYTEA PRIMARY KEY,
  client_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jmap_bearer_tokens_client_id_idx
  ON public.jmap_bearer_tokens(client_id);

-- +goose Down
DROP INDEX IF EXISTS public.jmap_bearer_tokens_client_id_idx;
DROP TABLE IF EXISTS public.jmap_bearer_tokens;
