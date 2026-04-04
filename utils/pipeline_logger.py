from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from supabase import Client


def start_run(
    client: Client,
    script_name: str,
    source_label: str,
    parameters: dict[str, Any],
) -> int:
    """Insert a pipeline_runs row and return its id."""
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "script_name": script_name,
        "source_label": source_label,
        "parameters": parameters,
        "rows_written": 0,
        "status": "partial",
        "error_message": None,
        "started_at": now,
        "completed_at": None,
    }
    res = client.table("pipeline_runs").insert(row).execute()
    if not res.data:
        raise RuntimeError("pipeline_runs insert returned no data")
    return int(res.data[0]["id"])


def finish_run(
    client: Client,
    run_id: int,
    rows_written: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """Update pipeline_runs with completion status."""
    now = datetime.now(timezone.utc).isoformat()
    client.table("pipeline_runs").update(
        {
            "rows_written": rows_written,
            "status": status,
            "error_message": error_message,
            "completed_at": now,
        }
    ).eq("id", run_id).execute()
