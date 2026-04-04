# ============================================================
# SCRIPT:  pull_worldbank_wdi.py
# SOURCE:  World Bank World Development Indicators (open API)
# URL:     https://data.worldbank.org/ — API https://api.worldbank.org/v2/
# API KEY: not required
# WRITES:  country_macro_indicators
# REFRESH: annual (re-run when new year data is published)
# NOTES:   One HTTP request per indicator (v2 does not accept multiple indicators in one path).
#          Rows restricted to ISO 3166-1 alpha-3 members via pycountry (excludes WB regions).
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS = [2020, 2021, 2022, 2023, 2024]
# World Bank indicator id -> (stored indicator slug, unit label)
WDI_INDICATORS: dict[str, tuple[str, str]] = {
    "SP.POP.TOTL": ("population", "persons"),
    "NY.GDP.MKTP.CD": ("gdp_current_usd", "current_usd"),
    "NY.GDP.PCAP.CD": ("gdp_per_capita_current_usd", "current_usd"),
}
HS_CODES = ["270900", "271100"]
COUNTRIES = None
# -------------------------------------------------------------

import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from datetime import datetime, timezone

import pycountry
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "pull_worldbank_wdi"
SOURCE_LABEL = "World Bank WDI API (population, GDP, GDP per capita)"
UPSERT_BATCH = 500
API_BASE = "https://api.worldbank.org/v2/country/all/indicator"
PER_PAGE = 20_000


def _http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=(502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _is_country_iso3(code: str | None) -> bool:
    if not code or len(code) != 3 or not code.isalpha():
        return False
    return pycountry.countries.get(alpha_3=code.upper()) is not None


def _fetch_indicator_pages(
    session: requests.Session, wb_indicator_id: str, year_lo: int, year_hi: int
) -> list[dict[str, Any]]:
    """Return flat list of observation dicts from all pages."""
    date_range = f"{year_lo}:{year_hi}"
    out: list[dict[str, Any]] = []
    page = 1
    while True:
        url = (
            f"{API_BASE}/{wb_indicator_id}"
            f"?date={date_range}&format=json&per_page={PER_PAGE}&page={page}"
        )
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or len(data) < 2:
            break
        meta, rows = data[0], data[1]
        if not isinstance(rows, list):
            break
        out.extend(rows)
        pages = int(meta.get("pages", 1)) if isinstance(meta, dict) else 1
        if page >= pages:
            break
        page += 1
    return out


def main() -> int:
    if not YEARS:
        print("YEARS is empty.", file=sys.stderr)
        return 1
    year_lo, year_hi = min(YEARS), max(YEARS)
    client = get_client()
    params: dict[str, Any] = {
        "years": YEARS,
        "indicators": list(WDI_INDICATORS.keys()),
        "per_page": PER_PAGE,
    }
    run_id = start_run(client, SCRIPT_NAME, SOURCE_LABEL, params)
    pulled_at = datetime.now(timezone.utc).isoformat()
    session = _http_session()
    rows: list[dict[str, Any]] = []

    try:
        for wb_id, (slug, unit) in WDI_INDICATORS.items():
            observations = _fetch_indicator_pages(session, wb_id, year_lo, year_hi)
            for obs in observations:
                iso3 = obs.get("countryiso3code")
                if not _is_country_iso3(iso3):
                    continue
                date_s = obs.get("date")
                if date_s is None:
                    continue
                try:
                    y = int(str(date_s).strip())
                except ValueError:
                    continue
                if y not in YEARS:
                    continue
                val = obs.get("value")
                if val is None:
                    continue
                try:
                    v = float(val)
                except (TypeError, ValueError):
                    continue
                rows.append(
                    {
                        "country": iso3.upper(),
                        "indicator": slug,
                        "value": v,
                        "unit": unit,
                        "data_year": y,
                        "source": SCRIPT_NAME,
                        "pulled_at": pulled_at,
                    }
                )

        for i in range(0, len(rows), UPSERT_BATCH):
            batch = rows[i : i + UPSERT_BATCH]
            client.table("country_macro_indicators").upsert(
                batch, on_conflict="country,indicator,data_year"
            ).execute()

        finish_run(client, run_id, len(rows), "success", None)
        print(f"Upserted {len(rows)} country macro indicator rows.")
        return 0

    except requests.HTTPError as e:
        err = f"HTTP error fetching WDI: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
