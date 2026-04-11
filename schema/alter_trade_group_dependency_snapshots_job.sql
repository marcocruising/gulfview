-- Optional migration: job metadata for group-dependency snapshots (apply if table already exists without these columns).
-- Safe to re-run.

ALTER TABLE public.trade_group_dependency_snapshots
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;

ALTER TABLE public.trade_group_dependency_snapshots
    ADD COLUMN IF NOT EXISTS error_message TEXT;
