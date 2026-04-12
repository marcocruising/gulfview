"""Unit tests for utils.postgrest_retry."""
from __future__ import annotations

import unittest

from utils.postgrest_retry import execute_with_retries, is_transient_rpc_failure


class TestTransient(unittest.TestCase):
    def test_remote_protocol_name(self) -> None:
        class RemoteProtocolError(Exception):
            pass

        self.assertTrue(is_transient_rpc_failure(RemoteProtocolError("x")))

    def test_value_error_not_transient(self) -> None:
        self.assertFalse(is_transient_rpc_failure(ValueError("bad")))


class TestExecuteWithRetries(unittest.TestCase):
    def test_succeeds_first_call(self) -> None:
        n = {"c": 0}

        def fn():
            n["c"] += 1
            return 42

        self.assertEqual(execute_with_retries(fn, attempts=3), 42)
        self.assertEqual(n["c"], 1)

    def test_retries_then_success(self) -> None:
        class RemoteProtocolError(Exception):
            pass

        n = {"c": 0}

        def fn():
            n["c"] += 1
            if n["c"] < 2:
                raise RemoteProtocolError("drop")
            return "ok"

        self.assertEqual(execute_with_retries(fn, attempts=3), "ok")
        self.assertEqual(n["c"], 2)


if __name__ == "__main__":
    unittest.main()
