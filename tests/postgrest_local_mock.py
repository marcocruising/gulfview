"""
Local HTTP mock for PostgREST-style endpoints used by supabase-py.

The real client issues e.g.:
  POST /rest/v1/rpc/rpc_trade_distinct_exporters
  GET  /rest/v1/bilateral_trade?select=exporter&order=exporter.asc&offset=0&limit=1000

Use this to verify the Python + httpx stack without hitting Supabase, and to
simulate failures (connection drop) similar to production disconnects.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Literal
from urllib.parse import urlparse

# Minimal JWT-shaped string; supabase-py sends it as apikey / Bearer (not validated here).
FAKE_SERVICE_JWT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJyb2xlIjoic2VydmljZV9yb2xlIn0."
    "mock_sig"
)

FailMode = Literal["none", "reset_during_rpc"]


class _MockPostgrestServer(HTTPServer):
    """Carries mock config on the server instance (handler reads self.server.*)."""

    mock_exporters: list[dict[str, str]]
    mock_bilateral_rows: list[dict[str, Any]]
    fail_mode: FailMode


class MockPostgrestHandler(BaseHTTPRequestHandler):
    server_version = "MockPostgrest/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _read_body(self) -> bytes:
        n = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(n) if n else b""

    def _send_json(self, status: int, payload: Any) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:
        srv = self.server
        assert isinstance(srv, _MockPostgrestServer)
        if srv.fail_mode == "reset_during_rpc" and "/rest/v1/rpc/" in self.path:
            self._read_body()
            try:
                self.connection.shutdown(2)
            except OSError:
                pass
            return

        if self.path.rstrip("/") == "/rest/v1/rpc/rpc_trade_distinct_exporters":
            self._read_body()
            self._send_json(200, srv.mock_exporters)
            return

        self._send_json(
            404,
            {
                "message": f"mock: no handler for POST {self.path}",
                "hint": "Expected /rest/v1/rpc/rpc_trade_distinct_exporters",
            },
        )

    def do_GET(self) -> None:
        srv = self.server
        assert isinstance(srv, _MockPostgrestServer)
        parsed = urlparse(self.path)
        if not parsed.path.rstrip("/").endswith("/bilateral_trade"):
            self._send_json(404, {"message": f"mock: unknown GET {self.path}"})
            return

        qs = parsed.query
        # supabase-py uses offset/limit query params (not always Range header).
        offset = 0
        limit = 1000
        for part in qs.split("&"):
            if part.startswith("offset="):
                try:
                    offset = int(part.split("=", 1)[1])
                except ValueError:
                    pass
            if part.startswith("limit="):
                try:
                    limit = int(part.split("=", 1)[1])
                except ValueError:
                    pass
        chunk = srv.mock_bilateral_rows[offset : offset + limit]
        self._send_json(200, chunk)


def start_mock_postgrest(
    *,
    exporters: list[dict[str, str]] | None = None,
    bilateral_rows: list[dict[str, Any]] | None = None,
    fail_mode: FailMode = "none",
) -> tuple[str, threading.Thread, _MockPostgrestServer]:
    """
    Start a background thread serving mock PostgREST.

    Returns:
        (base_url, thread, server) — call server.shutdown() then server.server_close() to stop.
    """
    srv = _MockPostgrestServer(("127.0.0.1", 0), MockPostgrestHandler)
    srv.mock_exporters = list(exporters or [{"exporter_iso3": "ARE"}, {"exporter_iso3": "DEU"}])
    srv.mock_bilateral_rows = list(bilateral_rows or [])
    srv.fail_mode = fail_mode

    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    host, port = srv.server_address
    base_url = f"http://{host}:{port}"
    return base_url, thread, srv
