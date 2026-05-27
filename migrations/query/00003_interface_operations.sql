-- +goose Up
CREATE TABLE IF NOT EXISTS public.interface_operations (
  operation_id TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  name TEXT NOT NULL,
  request_json JSONB NOT NULL,
  status TEXT NOT NULL,
  progress_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  result_json JSONB,
  error_text TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS interface_operations_status_updated_idx
  ON public.interface_operations(status, updated_at);

CREATE UNIQUE INDEX IF NOT EXISTS interface_operations_running_request_hash_idx
  ON public.interface_operations(request_hash)
  WHERE status = 'running';

-- +goose Down
DROP INDEX IF EXISTS public.interface_operations_running_request_hash_idx;
DROP INDEX IF EXISTS public.interface_operations_status_updated_idx;
DROP TABLE IF EXISTS public.interface_operations;
