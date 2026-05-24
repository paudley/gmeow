# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Alembic revision: resilience recovery tables.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0016
Revises: 20260523_0015
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0016"
down_revision = "20260523_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute("ALTER TABLE intelligence_jobs ADD COLUMN IF NOT EXISTS locked_by TEXT")
    op.execute("ALTER TABLE intelligence_jobs ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ")
    op.execute("ALTER TABLE intelligence_jobs ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ NOT NULL DEFAULT now()")
    op.execute("ALTER TABLE intelligence_jobs ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 5")
    op.execute("ALTER TABLE intelligence_jobs ADD COLUMN IF NOT EXISTS payload_json JSONB NOT NULL DEFAULT '{}'::jsonb")
    op.execute("ALTER TABLE intelligence_jobs ADD COLUMN IF NOT EXISTS dead_lettered_at TIMESTAMPTZ")
    op.execute("ALTER TABLE intelligence_jobs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ")
    op.execute("ALTER TABLE intelligence_jobs ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ")
    op.execute("CREATE INDEX IF NOT EXISTS intelligence_jobs_ready_idx ON intelligence_jobs(status, next_run_at, created_at, id)")
    op.execute("CREATE INDEX IF NOT EXISTS intelligence_jobs_locked_idx ON intelligence_jobs(status, locked_at) WHERE status = 'running'")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS dead_letter_jobs (
          id BIGSERIAL PRIMARY KEY,
          source_table TEXT NOT NULL,
          source_id BIGINT NOT NULL,
          kind TEXT NOT NULL,
          target_id TEXT NOT NULL,
          attempts INTEGER NOT NULL,
          last_error TEXT,
          payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          requeued_at TIMESTAMPTZ,
          cleared_at TIMESTAMPTZ,
          UNIQUE(source_table, source_id)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS dead_letter_jobs_open_idx ON dead_letter_jobs(created_at, id) "
        "WHERE requeued_at IS NULL AND cleared_at IS NULL"
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS operational_events (
          id BIGSERIAL PRIMARY KEY,
          event_type TEXT NOT NULL,
          severity TEXT NOT NULL DEFAULT 'info',
          component TEXT NOT NULL,
          subject_id TEXT,
          detail TEXT,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS operational_events_component_idx ON operational_events(component, created_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS operational_events_type_idx ON operational_events(event_type, created_at DESC)")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
          id BIGSERIAL PRIMARY KEY,
          run_kind TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'running',
          start_cursor TEXT,
          end_cursor TEXT,
          request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          error TEXT,
          started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          finished_at TIMESTAMPTZ
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS sync_runs_kind_started_idx ON sync_runs(run_kind, started_at DESC)")
    _comments()


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP TABLE IF EXISTS sync_runs")
    op.execute("DROP TABLE IF EXISTS operational_events")
    op.execute("DROP TABLE IF EXISTS dead_letter_jobs")
    op.execute("DROP INDEX IF EXISTS intelligence_jobs_locked_idx")
    op.execute("DROP INDEX IF EXISTS intelligence_jobs_ready_idx")
    for column in [
        "failed_at",
        "completed_at",
        "dead_lettered_at",
        "payload_json",
        "max_attempts",
        "next_run_at",
        "locked_at",
        "locked_by",
    ]:
        op.execute(f"ALTER TABLE intelligence_jobs DROP COLUMN IF EXISTS {column}")


def _comments() -> None:
    comments = {
        "TABLE dead_letter_jobs": "Terminal job failures retained for inspection and explicit requeue or clearing.",
        "COLUMN dead_letter_jobs.id": "Primary key for the dead-letter record.",
        "COLUMN dead_letter_jobs.source_table": "Source queue table that produced this dead-letter entry.",
        "COLUMN dead_letter_jobs.source_id": "Primary key of the source queue row.",
        "COLUMN dead_letter_jobs.kind": "Work kind copied from the source job.",
        "COLUMN dead_letter_jobs.target_id": "Message, attachment, or resource identifier copied from the source job.",
        "COLUMN dead_letter_jobs.attempts": "Number of attempts made before dead-lettering.",
        "COLUMN dead_letter_jobs.last_error": "Most recent error captured before dead-lettering.",
        "COLUMN dead_letter_jobs.payload_json": "Job payload retained for future requeue or operator inspection.",
        "COLUMN dead_letter_jobs.created_at": "Timestamp when the job entered the dead-letter table.",
        "COLUMN dead_letter_jobs.requeued_at": "Timestamp when this dead-letter entry was requeued.",
        "COLUMN dead_letter_jobs.cleared_at": "Timestamp when this dead-letter entry was explicitly cleared.",
        "TABLE operational_events": (
            "Append-only operational audit trail for sync, maintenance, recovery, repair, and degraded-mode events."
        ),
        "COLUMN operational_events.id": "Primary key for the operational event.",
        "COLUMN operational_events.event_type": "Machine-readable event type.",
        "COLUMN operational_events.severity": "Event severity such as info, warning, error, or critical.",
        "COLUMN operational_events.component": "Subsystem that emitted the event.",
        "COLUMN operational_events.subject_id": "Optional message, job, attachment, or service identifier related to the event.",
        "COLUMN operational_events.detail": "Human-readable event detail.",
        "COLUMN operational_events.metadata_json": "Structured event metadata.",
        "COLUMN operational_events.created_at": "Event creation timestamp.",
        "TABLE sync_runs": "Durable sync run checkpoints and outcomes for priority and Gmail history synchronization.",
        "COLUMN sync_runs.id": "Primary key for the sync run.",
        "COLUMN sync_runs.run_kind": "Sync run type such as priority or history.",
        "COLUMN sync_runs.status": "Run status: running, complete, failed, or skipped.",
        "COLUMN sync_runs.start_cursor": "History cursor or equivalent starting checkpoint.",
        "COLUMN sync_runs.end_cursor": "History cursor or equivalent final checkpoint when complete.",
        "COLUMN sync_runs.request_json": "Structured sync request parameters.",
        "COLUMN sync_runs.result_json": "Structured sync result summary.",
        "COLUMN sync_runs.error": "Failure detail when the run fails.",
        "COLUMN sync_runs.started_at": "Run start timestamp.",
        "COLUMN sync_runs.finished_at": "Run completion timestamp.",
        "COLUMN intelligence_jobs.locked_by": "Worker identifier that currently leases the job.",
        "COLUMN intelligence_jobs.locked_at": "Timestamp when the current job lease was acquired.",
        "COLUMN intelligence_jobs.next_run_at": "Earliest timestamp when a pending job may be claimed.",
        "COLUMN intelligence_jobs.max_attempts": "Maximum processing attempts before dead-lettering.",
        "COLUMN intelligence_jobs.payload_json": "Structured job payload for generic durable work.",
        "COLUMN intelligence_jobs.dead_lettered_at": "Timestamp when the job was moved to the dead-letter table.",
        "COLUMN intelligence_jobs.completed_at": "Timestamp when the job completed successfully.",
        "COLUMN intelligence_jobs.failed_at": "Timestamp of the latest failed processing attempt.",
        "INDEX intelligence_jobs_ready_idx": "Accelerates ready pending job claims by status and next run time.",
        "INDEX intelligence_jobs_locked_idx": "Accelerates stale running job lease recovery.",
        "INDEX dead_letter_jobs_open_idx": "Accelerates open dead-letter inspection.",
        "INDEX operational_events_component_idx": "Accelerates component-scoped operational event queries.",
        "INDEX operational_events_type_idx": "Accelerates event-type operational event queries.",
        "INDEX sync_runs_kind_started_idx": "Accelerates newest-first sync run inspection by kind.",
        "CONSTRAINT dead_letter_jobs_source_table_source_id_key ON dead_letter_jobs": "Ensures each source job is dead-lettered once.",
    }
    for target, text in comments.items():
        op.execute(f"COMMENT ON {target} IS '{text}'")
