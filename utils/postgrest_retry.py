"""Shared PostgREST retry logic for transient httpx / 5xx failures (used by Streamlit trade RPCs)."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


def is_transient_rpc_failure(exc: BaseException) -> bool:
    try:
        from postgrest.exceptions import APIError

        if isinstance(exc, APIError):
            code = str(getattr(exc, "code", "") or "")
            return len(code) == 3 and code.startswith("5")
    except Exception:
        pass
    return type(exc).__name__ in (
        "RemoteProtocolError",
        "ReadError",
        "ConnectError",
        "ConnectTimeout",
        "ReadTimeout",
        "WriteTimeout",
        "LocalProtocolError",
        "PoolTimeout",
    )


def execute_with_retries(fn: Callable[[], T], *, attempts: int = 3) -> T:
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            if i < attempts - 1 and is_transient_rpc_failure(e):
                time.sleep(0.75 * (2**i))
                continue
            raise
