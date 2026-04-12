#!/usr/bin/env python3
"""
Call `rpc_trade_distinct_exporters` on the real Supabase project (uses `.env` + get_client).

Uses the same PostgREST timeout and retry behavior as the Streamlit app (via utils).

  uv run python scripts/probe_trade_distinct_remote.py

Exit 0 on success, non-zero on failure.
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.postgrest_retry import execute_with_retries
from utils.supabase_client import get_client


def main() -> int:
    try:
        sb = get_client()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 2
    try:
        res = execute_with_retries(lambda: sb.rpc("rpc_trade_distinct_exporters", {}).execute())
    except Exception as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        return 1
    rows = res.data or []
    codes = sorted({str(r.get("exporter_iso3", "")).strip().upper() for r in rows if r.get("exporter_iso3")})
    print(f"OK: {len(codes)} distinct exporters (sample: {codes[:8]}{'…' if len(codes) > 8 else ''})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
