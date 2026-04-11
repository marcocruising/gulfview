"""Unit tests for utils/group_dependency_compute.py (no Supabase calls)."""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from utils import group_dependency_compute as gdc


class TestParamsHash(unittest.TestCase):
    def test_params_hash_stable(self) -> None:
        p = {
            "version": 1,
            "data_year": 2024,
            "group_iso3": ["SAU", "ARE"],
            "hs_query_text": "",
            "limit_n_hs6": 50,
            "import_hs6_code": "270900",
            "limit_n_importers": 25,
        }
        h1 = gdc.params_hash(p)
        h2 = gdc.params_hash(p)
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)

    def test_key_order_invariant(self) -> None:
        a = {"data_year": 2024, "version": 1}
        b = {"version": 1, "data_year": 2024}
        self.assertEqual(gdc.params_hash(a), gdc.params_hash(b))

    def test_group_order_matters(self) -> None:
        p1 = {"group_iso3": ["ARE", "SAU"], "data_year": 2024}
        p2 = {"group_iso3": ["SAU", "ARE"], "data_year": 2024}
        self.assertNotEqual(gdc.params_hash(p1), gdc.params_hash(p2))


class TestCanonicalJson(unittest.TestCase):
    def test_unicode_preserved_in_string(self) -> None:
        s = gdc.canonical_params_json({"note": "é"})
        self.assertIn("é", s)


class TestWorldShareRpcPayload(unittest.TestCase):
    """PostgREST PGRST202 if JSON keys do not match Postgres parameter names."""

    def test_uses_p_data_year_not_data_year(self) -> None:
        sb = MagicMock()
        sb.rpc.return_value.execute.return_value.data = []
        gdc.rpc_trade_group_world_share_by_hs6(sb, 2024, ["SAU"], "x", 10)
        payload = sb.rpc.call_args[0][1]
        self.assertEqual(payload["p_data_year"], 2024)
        self.assertNotIn("data_year", payload)


class TestImporterExposureRpcPayload(unittest.TestCase):
    def test_uses_p_prefixed_year_and_hs6(self) -> None:
        sb = MagicMock()
        sb.rpc.return_value.execute.return_value.data = []
        gdc.rpc_trade_group_importer_exposure_for_hs6(sb, 2024, "270900", ["SAU"], 25)
        payload = sb.rpc.call_args[0][1]
        self.assertEqual(payload["p_data_year"], 2024)
        self.assertEqual(payload["p_hs6_code"], "270900")
        self.assertNotIn("data_year", payload)
        self.assertNotIn("hs6_code", payload)


if __name__ == "__main__":
    unittest.main()
