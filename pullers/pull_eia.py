# ============================================================
# SCRIPT:  pull_eia.py
# SOURCE:  U.S. Energy Information Administration
# URL:     https://www.eia.gov/opendata/
# API KEY: required — set EIA_API_KEY in .env
# WRITES:  energy_trade_flows
# REFRESH: monthly
# NOTES:   EIA retired wimcli-style routes; use v2 petroleum/move/impcus/data/ only.
#          facets[product][]=EPC0 limits to crude (else all petroleum products mix in).
#          value is thousand barrels per month; value_kbd = value / calendar.monthrange(y,m)[1].
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS = [2020, 2021, 2022, 2023, 2024]
HS_CODES = ["270900", "271100"]
COUNTRIES = None
START_PERIOD = "2020-01"
END_PERIOD = "2025-12"
# -------------------------------------------------------------

import calendar
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pycountry
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "pull_eia"
SOURCE_LABEL = "EIA Open Data API v2 (petroleum/move/impcus, crude EPC0)"
UPSERT_BATCH = 500
EIA_DATA_URL = "https://api.eia.gov/v2/petroleum/move/impcus/data/"
EIA_CRUDE_PRODUCT = "EPC0"

ORIGIN_TO_ISO3: dict[str, str] = {
    "SAR": "SAU",
    "IRQ": "IRQ",
    "KWT": "KWT",
    "ARE": "ARE",
    "QAT": "QAT",
    "IRN": "IRN",
    "NGA": "NGA",
    "MEX": "MEX",
    "COL": "COL",
    "CAN": "CAN",
    "RUS": "RUS",
    "NOR": "NOR",
    "GBR": "GBR",
    "BRA": "BRA",
    "ECU": "ECU",
    "AGO": "AGO",
    "DZA": "DZA",
    "LBY": "LBY",
    "NLD": "NLD",
    "BEL": "BEL",
    "FRA": "FRA",
    "DEU": "DEU",
    "ITA": "ITA",
    "ESP": "ESP",
    "USA": "USA",
    "U.S.": "USA",
}


def _session() -> requests.Session:
    s = requests.Session()
    s.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=3, backoff_factor=1.0, status_forcelist=(502, 503, 504)
            )
        ),
    )
    return s


def _origin_to_iso3(origin: str) -> str | None:
    if not origin or not str(origin).strip():
        return None
    o = str(origin).strip().upper()
    if o in ORIGIN_TO_ISO3:
        return ORIGIN_TO_ISO3[o]
    if len(o) == 3 and o.isalpha():
        try:
            pycountry.countries.get(alpha_3=o)
            return o
        except (LookupError, TypeError):
            pass
    if len(o) == 2:
        c = pycountry.countries.get(alpha_2=o)
        return c.alpha_3 if c else None
    return None


def _parse_period(p: str) -> tuple[int, int] | None:
    try:
        y, m = p.split("-", 1)
        return int(y), int(m)
    except (ValueError, AttributeError):
        return None


def main() -> int:
    load_dotenv(_ROOT / ".env")
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        print(
            "Set EIA_API_KEY in .env (https://www.eia.gov/opendata/register.php)",
            file=sys.stderr,
        )
        return 1

    client = get_client()
    run_id = start_run(
        client,
        SCRIPT_NAME,
        SOURCE_LABEL,
        {
            "years": YEARS,
            "start": START_PERIOD,
            "end": END_PERIOD,
            "route": EIA_DATA_URL,
            "product_facet": EIA_CRUDE_PRODUCT,
        },
    )

    try:
        sess = _session()
        q = {
            "api_key": api_key,
            "frequency": "monthly",
            "data[0]": "value",
            "facets[product][]": EIA_CRUDE_PRODUCT,
            "start": START_PERIOD,
            "end": END_PERIOD,
            "length": 5000,
        }
        r = sess.get(EIA_DATA_URL, params=q, timeout=120)
        if r.status_code in (401, 403):
            finish_run(client, run_id, 0, "error", "EIA API rejected the key (401/403).")
            return 1
        if not r.ok:
            snippet = (r.text or "")[:500].replace(api_key, "<api_key>")
            finish_run(
                client,
                run_id,
                0,
                "error",
                f"EIA HTTP {r.status_code}: {snippet}",
            )
            print(
                f"EIA HTTP {r.status_code} (details in pipeline_runs; key redacted).",
                file=sys.stderr,
            )
            return 1
        body = r.json()
        data = body.get("response", {}).get("data") or []
        if not data:
            finish_run(
                client,
                run_id,
                0,
                "partial",
                "EIA returned no impcus crude rows for this range.",
            )
            return 0

        pulled_at = datetime.now(timezone.utc).isoformat()
        rows: list[dict[str, Any]] = []
        skipped = 0
        for row in data:
            desc = str(row.get("series-description") or "")
            if "per Day" in desc:
                continue
            period = row.get("period")
            pr = _parse_period(str(period)) if period else None
            if not pr:
                continue
            y, mth = pr
            if y not in YEARS:
                continue
            origin = row.get("area-name") or row.get("origin") or row.get("originId")
            if not origin or str(origin).strip().upper() in ("NA", "N/A", ""):
                skipped += 1
                continue
            reporter = _origin_to_iso3(str(origin))
            if not reporter:
                skipped += 1
                continue
            try:
                value_kb_per_month = float(row.get("value", 0))
            except (TypeError, ValueError):
                continue
            days_in_month = calendar.monthrange(y, mth)[1]
            # kbd = thousand bbl/month ÷ days (not /30 — Feb vs 31-day months)
            kbd = (
                value_kb_per_month / days_in_month
                if days_in_month
                else value_kb_per_month
            )

            rows.append(
                {
                    "reporter": reporter,
                    "flow_type": "import",
                    "product": "crude_oil",
                    "value_kbd": kbd,
                    "data_year": y,
                    "data_month": mth,
                    "source": SCRIPT_NAME,
                    "pulled_at": pulled_at,
                }
            )

        if not rows:
            msg = f"No mappable rows (skipped origins={skipped}). Extend ORIGIN_TO_ISO3 in pull_eia.py."
            finish_run(client, run_id, 0, "partial", msg)
            print(msg, file=sys.stderr)
            return 0

        for i in range(0, len(rows), UPSERT_BATCH):
            client.table("energy_trade_flows").upsert(
                rows[i : i + UPSERT_BATCH],
                on_conflict="reporter,flow_type,product,data_year,data_month",
            ).execute()

        note = f"Skipped {skipped} rows (NA/unknown origin)." if skipped else None
        finish_run(client, run_id, len(rows), "partial" if note else "success", note)
        print(f"Upserted {len(rows)} rows.")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        err = err.replace(api_key, "<api_key>")
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
