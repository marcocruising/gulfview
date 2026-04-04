# ============================================================
# SCRIPT:  pull_usda_psd.py
# SOURCE:  USDA FAS PSD
# URL:     https://apps.fas.usda.gov/psdonline/
# API KEY: required — USDA_FAS_API_KEY in .env (https://api.fas.usda.gov)
# WRITES:  crop_production
# REFRESH: monthly
# NOTES:   PSD returns attributeId (not names on rows). Maps countries via /countries gencCode (ISO3).
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS = [2020, 2021, 2022, 2023, 2024]
HS_CODES = ["270900", "271100"]
COUNTRIES = None
MARKETING_YEARS = [2020, 2021, 2022, 2023, 2024]
CROP_HINTS = (
    ("wheat", "wheat"),
    ("corn", "corn"),
    ("rice", "rice"),
    ("soybean", "soybeans"),
    ("cotton", "cotton"),
)
# Attribute IDs from /commodityAttributes — core supply/demand lines for row crops
ATTR_PRODUCTION = 28
ATTR_IMPORTS = 57
ATTR_EXPORTS = 88
ATTR_TO_METRIC = {
    ATTR_PRODUCTION: "production",
    ATTR_IMPORTS: "imports",
    ATTR_EXPORTS: "exports",
}
# -------------------------------------------------------------

import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "pull_usda_psd"
SOURCE_LABEL = "USDA FAS PSD Open Data API"
UPSERT_BATCH = 500
API_ROOT = "https://api.fas.usda.gov/api/psd"


def _sess() -> requests.Session:
    s = requests.Session()
    s.mount(
        "https://",
        HTTPAdapter(max_retries=Retry(total=3, status_forcelist=(502, 503, 504))),
    )
    return s


def _unit_to_tonnes_multiplier(unit_desc: str) -> float | None:
    """Map PSD unitDescription to multiplier so value * mult = metric tonnes."""
    d = (unit_desc or "").upper()
    if re.search(r"1000.*MT", d) or re.search(r"\(1000 MT", d):
        return 1000.0
    if "MT" in d and "1000" not in d:
        return 1.0
    return None


def main() -> int:
    load_dotenv(_ROOT / ".env")
    key = os.environ.get("USDA_FAS_API_KEY")
    if not key:
        print("Set USDA_FAS_API_KEY in .env (https://api.fas.usda.gov)", file=sys.stderr)
        return 1

    client = get_client()
    run_id = start_run(
        client,
        SCRIPT_NAME,
        SOURCE_LABEL,
        {"years": YEARS, "marketing_years": MARKETING_YEARS},
    )
    try:
        s = _sess()

        ur = s.get(f"{API_ROOT}/unitsOfMeasure", params={"api_key": key}, timeout=120)
        ur.raise_for_status()
        unit_mult: dict[int, float] = {}
        for u in ur.json():
            uid = u.get("unitId")
            desc = u.get("unitDescription") or ""
            m = _unit_to_tonnes_multiplier(desc)
            if uid is not None and m is not None:
                unit_mult[int(uid)] = m

        cr = s.get(f"{API_ROOT}/countries", params={"api_key": key}, timeout=120)
        cr.raise_for_status()
        country_iso3: dict[str, str] = {}
        for row in cr.json():
            cc = row.get("countryCode")
            g3 = row.get("gencCode")
            if cc and g3 and len(str(g3).strip()) == 3:
                country_iso3[str(cc).strip()] = str(g3).strip().upper()

        com_r = s.get(f"{API_ROOT}/commodities", params={"api_key": key}, timeout=120)
        com_r.raise_for_status()
        commodities = com_r.json()
        if not isinstance(commodities, list):
            commodities = commodities.get("commodities") or commodities.get("data") or []

        picked: list[tuple[str, str]] = []
        for row in commodities:
            nm = str(row.get("commodityName") or row.get("name") or "").lower()
            code = row.get("commodityCode") or row.get("code")
            if not code:
                continue
            for hint, slug in CROP_HINTS:
                if hint in nm:
                    picked.append((str(code), slug))
                    break

        if not picked:
            finish_run(client, run_id, 0, "error", "No PSD commodities matched CROP_HINTS.")
            return 1

        pulled_at = datetime.now(timezone.utc).isoformat()

        def _cal_sort(rec: dict[str, Any]) -> tuple[int, int]:
            try:
                return int(rec.get("calendarYear") or 0), int(rec.get("month") or 0)
            except (TypeError, ValueError):
                return 0, 0

        # One row per (country, crop, metric, data_year): keep latest calendarYear/month.
        best: dict[tuple[str, str, str, int], tuple[tuple[int, int], dict[str, Any]]] = {}

        for code, crop in picked:
            for my in MARKETING_YEARS:
                url = f"{API_ROOT}/commodity/{code}/country/all/year/{my}"
                r = s.get(url, params={"api_key": key}, timeout=120)
                if r.status_code != 200:
                    continue
                body = r.json()
                recs = body if isinstance(body, list) else body.get("data") or []
                for rec in recs:
                    if not isinstance(rec, dict):
                        continue
                    aid = rec.get("attributeId")
                    if aid not in ATTR_TO_METRIC:
                        continue
                    metric = ATTR_TO_METRIC[int(aid)]
                    cc = rec.get("countryCode")
                    if not cc:
                        continue
                    country = country_iso3.get(str(cc).strip())
                    if not country:
                        continue
                    uid = rec.get("unitId")
                    mult = unit_mult.get(int(uid)) if uid is not None else None
                    if mult is None:
                        continue
                    try:
                        val = float(rec.get("value") or 0)
                    except (TypeError, ValueError):
                        continue
                    if abs(val) < 1e-9:
                        continue
                    tonnes = val * mult
                    try:
                        my_val = int(str(rec.get("marketYear") or my))
                    except (TypeError, ValueError):
                        my_val = int(my)
                    if my_val not in YEARS:
                        continue
                    row = {
                        "country": country,
                        "crop": crop,
                        "metric": metric,
                        "value": tonnes,
                        "unit": "tonnes",
                        "data_year": my_val,
                        "source": SCRIPT_NAME,
                        "pulled_at": pulled_at,
                    }
                    ukey = (country, crop, metric, my_val)
                    t = _cal_sort(rec)
                    if ukey not in best or t > best[ukey][0]:
                        best[ukey] = (t, row)

        out = [pair[1] for pair in best.values()]

        if not out:
            finish_run(
                client,
                run_id,
                0,
                "partial",
                "PSD returned no rows after mapping attributeId, gencCode, and (1000 MT) units.",
            )
            return 0

        for i in range(0, len(out), UPSERT_BATCH):
            client.table("crop_production").upsert(
                out[i : i + UPSERT_BATCH],
                on_conflict="country,crop,metric,data_year",
            ).execute()

        finish_run(client, run_id, len(out), "success", None)
        print(f"Upserted {len(out)} crop_production rows.")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        err = err.replace(key, "<api_key>")
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
