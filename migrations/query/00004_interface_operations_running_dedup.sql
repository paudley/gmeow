-- +goose Up
ALTER TABLE public.interface_operations
  DROP CONSTRAINT IF EXISTS interface_operations_request_hash_key;

CREATE UNIQUE INDEX IF NOT EXISTS interface_operations_running_request_hash_idx
  ON public.interface_operations(request_hash)
  WHERE status = 'running';

-- +goose Down
DROP INDEX IF EXISTS public.interface_operations_running_request_hash_idx;

ALTER TABLE public.interface_operations
  ADD CONSTRAINT interface_operations_request_hash_key UNIQUE (request_hash);
