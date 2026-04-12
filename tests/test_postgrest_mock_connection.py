"""Supabase client against a local PostgREST-shaped mock (no real network)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from supabase import create_client

from tests.postgrest_local_mock import FAKE_SERVICE_JWT, start_mock_postgrest


class TestMockPostgrestRpc(unittest.TestCase):
    def test_rpc_trade_distinct_exporters_returns_rows(self) -> None:
        base_url, thread, srv = start_mock_postgrest(
            exporters=[{"exporter_iso3": "ARE"}, {"exporter_iso3": "SAU"}],
        )
        self.addCleanup(srv.shutdown)
        self.addCleanup(srv.server_close)
        try:
            c = create_client(base_url, FAKE_SERVICE_JWT)
            res = c.rpc("rpc_trade_distinct_exporters", {}).execute()
            rows = res.data or []
            codes = sorted({str(r["exporter_iso3"]).upper() for r in rows})
            self.assertEqual(codes, ["ARE", "SAU"])
        finally:
            thread.join(timeout=2.0)

    def test_bilateral_trade_fallback_scan_path(self) -> None:
        """Same table read the app uses when collecting distinct exporter from rows."""
        rows = [{"exporter": "NLD"}, {"exporter": "NLD"}, {"exporter": "BEL"}]
        base_url, thread, srv = start_mock_postgrest(bilateral_rows=rows)
        self.addCleanup(srv.shutdown)
        self.addCleanup(srv.server_close)
        try:
            c = create_client(base_url, FAKE_SERVICE_JWT)
            r = (
                c.table("bilateral_trade")
                .select("exporter")
                .order("exporter")
                .range(0, 999)
                .execute()
            )
            data = r.data or []
            distinct = sorted({str(x["exporter"]).upper() for x in data})
            self.assertEqual(distinct, ["BEL", "NLD"])
        finally:
            thread.join(timeout=2.0)

    def test_rpc_connection_reset_triggers_client_error(self) -> None:
        """Simulates an abrupt close (similar class of failure to remote disconnect)."""
        base_url, thread, srv = start_mock_postgrest(fail_mode="reset_during_rpc")
        self.addCleanup(srv.shutdown)
        self.addCleanup(srv.server_close)
        try:
            c = create_client(base_url, FAKE_SERVICE_JWT)
            with self.assertRaises(Exception) as ctx:
                c.rpc("rpc_trade_distinct_exporters", {}).execute()
            # httpx may raise ReadError, RemoteProtocolError, etc. depending on version / OS.
            self.assertIsNotNone(ctx.exception)
        finally:
            thread.join(timeout=2.0)


class TestEnvIsolation(unittest.TestCase):
    """Ensure .env does not override patched URL when probing."""

    def test_get_client_uses_patched_env(self) -> None:
        base_url, thread, srv = start_mock_postgrest()
        self.addCleanup(srv.shutdown)
        self.addCleanup(srv.server_close)
        try:
            fake_env = {
                "SUPABASE_URL": base_url,
                "SUPABASE_SERVICE_ROLE_KEY": FAKE_SERVICE_JWT,
            }
            with patch.dict(os.environ, fake_env, clear=False):
                with patch("utils.supabase_client.load_dotenv", lambda *_args, **_kw: None):
                    from utils.supabase_client import get_client

                    c = get_client()
                    res = c.rpc("rpc_trade_distinct_exporters", {}).execute()
                    self.assertGreaterEqual(len(res.data or []), 1)
        finally:
            thread.join(timeout=2.0)


if __name__ == "__main__":
    unittest.main()
