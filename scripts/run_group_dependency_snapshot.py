#!/usr/bin/env python3
"""
Compute and persist a group-dependency snapshot (same logic as Streamlit **Group dependencies** save).

Requires SUPABASE_URL + service key in .env (see utils.supabase_client.get_client).

Params JSON shape (matches Streamlit `params_json`):
  version: 1
  data_year: int
  group_iso3: list[str]   # ISO3 codes
  hs_query_text: str
  limit_n_hs6: int
  import_hs6_code: str    # HS6 for importer exposure table
  limit_n_importers: int
  coverage_only: bool     # if true, drop HS6 rows with world_exporter_count < 10

Example:
  uv run python scripts/run_group_dependency_snapshot.py --params-json params.json
  uv run python scripts/run_group_dependency_snapshot.py --params-json params.json --force

Apply schema/alter_trade_group_dependency_snapshots_job.sql once if inserts fail on unknown columns.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from postgrest.exceptions import APIError

from utils import group_dependency_compute as gdc
from utils.supabase_client import get_client


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--params-json",
        type=Path,
        required=True,
        help="Path to JSON file with snapshot parameters",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Recompute even if a snapshot with the same params_hash exists",
    )
    args = ap.parse_args()
    raw = args.params_json.read_text(encoding="utf-8")
    params = json.loads(raw)
    required = (
        "data_year",
        "group_iso3",
        "limit_n_hs6",
        "import_hs6_code",
        "limit_n_importers",
    )
    for k in required:
        if k not in params:
            print(f"Missing key in params JSON: {k}", file=sys.stderr)
            return 2

    sb = get_client()
    cov = bool(params.get("coverage_only", True))
    try:
        export_df, imp_df = gdc.compute_export_and_importer_frames(
            sb,
            data_year=int(params["data_year"]),
            group_iso3=list(params["group_iso3"]),
            hs_query_text=str(params.get("hs_query_text") or ""),
            limit_n_hs6=int(params["limit_n_hs6"]),
            import_hs6_code=str(params["import_hs6_code"]),
            limit_n_importers=int(params["limit_n_importers"]),
            coverage_only=cov,
        )
    except APIError as e:
        err = e.message or str(e)
        if e.code == "57014" or "timeout" in err.lower():
            print(
                "Query timed out (statement_timeout). Try smaller limit_n_hs6, a narrower "
                "hs_query_text, or raise the function timeout in schema/rpc_trade_dashboards.sql "
                "(plpgsql wrappers) / Supabase statement_timeout.",
                file=sys.stderr,
            )
        raise
    if export_df.empty:
        print("No export rows after filters — nothing to save.", file=sys.stderr)
        return 1
    params_full = dict(params)
    params_full.setdefault("version", 1)
    params_full["import_hs6_code"] = str(params_full["import_hs6_code"]).strip()
    sid, ph = gdc.write_snapshot_and_rows(
        sb,
        params_json=params_full,
        export_rows=export_df,
        importer_rows=imp_df,
        force_recompute=bool(args.force),
    )
    print(f"snapshot_id={sid} params_hash={ph}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
