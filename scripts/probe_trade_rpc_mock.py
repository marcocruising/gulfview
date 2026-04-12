#!/usr/bin/env python3
"""
Debug helper: prove the Supabase Python client can talk to a PostgREST-shaped HTTP server.

This does **not** use your real Supabase project. It starts a tiny mock on localhost,
runs the same RPC call as `rpc_trade_distinct_exporters` in `app/streamlit_app.py`,
and prints OK or the exception class.

Usage:
  uv run python scripts/probe_trade_rpc_mock.py
  uv run python scripts/probe_trade_rpc_mock.py --disconnect   # simulate server dropping RPC

For real remote issues (RemoteProtocolError, timeouts), compare:
  - this script OK → local stack / URL / key format fine; problem is network or Supabase.
  - this script OK but production fails → project URL, key, idle timeouts, or PostgREST crash.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supabase import create_client

from tests.postgrest_local_mock import FAKE_SERVICE_JWT, start_mock_postgrest


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--disconnect",
        action="store_true",
        help="Close the connection during RPC (mimics abrupt disconnect).",
    )
    args = p.parse_args()

    fail_mode = "reset_during_rpc" if args.disconnect else "none"
    base_url, thread, srv = start_mock_postgrest(fail_mode=fail_mode)
    try:
        c = create_client(base_url, FAKE_SERVICE_JWT)
        print(f"Mock PostgREST at {base_url}")
        try:
            res = c.rpc("rpc_trade_distinct_exporters", {}).execute()
            print("RPC OK:", res.data)
            return 0
        except Exception as e:
            print(f"RPC FAILED: {type(e).__name__}: {e}")
            return 1
    finally:
        srv.shutdown()
        srv.server_close()
        thread.join(timeout=5.0)


if __name__ == "__main__":
    raise SystemExit(main())
