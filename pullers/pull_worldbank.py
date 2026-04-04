# ============================================================
# SCRIPT:  pull_worldbank.py
# SOURCE:  World Bank Commodity Markets (Pink Sheet)
# URL:     https://www.worldbank.org/en/research/commodity-markets
# API KEY: not required
# WRITES:  commodity_prices
# REFRESH: monthly
# NOTES:   Monthly historical XLSX; doc hash in download URL changes when WB republishes — see PINK_SHEET_MONTHLY_XLSX_URL and 404 handling below.
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
HS_CODES = ["270900", "271100"]  # not used — keep per template
COUNTRIES = None  # None = all available countries
# -------------------------------------------------------------

import io
import sys
from pathlib import Path

# Running as `uv run python pullers/pull_worldbank.py` puts `pullers/` on sys.path first.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

PINK_SHEET_MONTHLY_XLSX_URL = (
    "https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/"
    "related/CMO-Historical-Data-Monthly.xlsx"
)

MONTHLY_PRICES_SHEET = "Monthly Prices"
# Row index (0-based) used as pandas `header=` — commodity names row in current Pink Sheet layout.
EXCEL_HEADER_ROW = 4

# Logical series: try Excel headers in order (Pink Sheet names vary; strip() applied to file columns).
COMMODITY_COLUMN_ALIASES: dict[str, list[str]] = {
    # README “Crude oil” → Pink Sheet publishes “Crude oil, average” in current files.
    "crude_oil": ["Crude oil", "Crude oil, average"],
    "crude_oil_brent": ["Brent", "Crude oil, Brent"],
    "urea": ["Urea"],
    "dap": ["DAP"],
    "ammonia": ["Ammonia"],
    "wheat": ["Wheat, US HRW"],
    "rice": ["Rice, Thai 5%"],
    "corn": ["Maize"],
}

UNIT_BY_COMMODITY: dict[str, str] = {
    "crude_oil": "usd_per_barrel",
    "crude_oil_brent": "usd_per_barrel",
    "urea": "usd_per_tonne",
    "dap": "usd_per_tonne",
    "ammonia": "usd_per_tonne",
    "wheat": "usd_per_tonne",
    "rice": "usd_per_tonne",
    "corn": "usd_per_tonne",
}

SCRIPT_NAME = "pull_worldbank"
SOURCE_LABEL = "World Bank Pink Sheet (CMO Historical Monthly)"
UPSERT_BATCH = 500

STALE_URL_ERROR = (
    "Pink Sheet XLSX URL returned HTTP 404. The document hash in PINK_SHEET_MONTHLY_XLSX_URL "
    "is stale: World Bank republished the bundle. Open the World Bank commodities / Pink Sheet "
    "documentation page, download the new CMO-Historical-Data-Monthly.xlsx link, and update "
    "PINK_SHEET_MONTHLY_XLSX_URL in pullers/pull_worldbank.py."
)


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
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def _parse_pink_sheet_period(val: Any) -> tuple[int, int] | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    upper = s.upper()
    if "M" in upper:
        parts = upper.split("M", 1)
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            y, m = int(parts[0]), int(parts[1])
            if 1 <= m <= 12:
                return y, m
    ts = pd.to_datetime(val, errors="coerce")
    if pd.notna(ts):
        return int(ts.year), int(ts.month)
    return None


def _resolve_columns(df: pd.DataFrame) -> tuple[dict[str, str], list[str]]:
    """Map commodity slug → actual column name in df. Returns (resolved, missing_slugs)."""
    resolved: dict[str, str] = {}
    missing: list[str] = []
    for slug, aliases in COMMODITY_COLUMN_ALIASES.items():
        found: str | None = None
        for name in aliases:
            if name in df.columns:
                found = name
                break
        if found is not None:
            resolved[slug] = found
        else:
            missing.append(slug)
    return resolved, missing


def _load_monthly_prices(content: bytes) -> pd.DataFrame:
    return pd.read_excel(
        io.BytesIO(content),
        engine="openpyxl",
        sheet_name=MONTHLY_PRICES_SHEET,
        header=EXCEL_HEADER_ROW,
    )


def main() -> int:
    client = get_client()
    params: dict[str, Any] = {
        "years": YEARS,
        "pink_sheet_url": PINK_SHEET_MONTHLY_XLSX_URL,
        "sheet": MONTHLY_PRICES_SHEET,
        "excel_header_row": EXCEL_HEADER_ROW,
        "commodity_aliases": COMMODITY_COLUMN_ALIASES,
    }
    run_id = start_run(client, SCRIPT_NAME, SOURCE_LABEL, params)

    try:
        session = _http_session()
        resp = session.get(PINK_SHEET_MONTHLY_XLSX_URL, timeout=120)
        if resp.status_code == 404:
            finish_run(client, run_id, 0, "error", STALE_URL_ERROR)
            print(STALE_URL_ERROR, file=sys.stderr)
            return 1
        resp.raise_for_status()

        df = _load_monthly_prices(resp.content)
        df.columns = [str(c).strip() for c in df.columns]

        if df.empty or len(df.columns) < 2:
            finish_run(
                client,
                run_id,
                0,
                "error",
                "Pink Sheet workbook parsed empty or has no commodity columns.",
            )
            return 1

        period_col = df.columns[0]
        resolved, missing_slugs = _resolve_columns(df)
        if not resolved:
            finish_run(
                client,
                run_id,
                0,
                "error",
                "No configured commodity columns found in Monthly Prices sheet. "
                f"Expected one of per slug: {COMMODITY_COLUMN_ALIASES}",
            )
            return 1

        value_cols = list(resolved.values())
        long = df.melt(
            id_vars=[period_col],
            value_vars=value_cols,
            var_name="_excel_col",
            value_name="price",
        )
        rev_map = {v: k for k, v in resolved.items()}
        long["commodity"] = long["_excel_col"].map(rev_map)

        periods = long[period_col].map(_parse_pink_sheet_period)
        long["data_year"] = periods.map(lambda p: p[0] if p else None)
        long["data_month"] = periods.map(lambda p: p[1] if p else None)
        long = long.dropna(subset=["data_year", "data_month", "commodity"])
        long["data_year"] = long["data_year"].astype(int)
        long["data_month"] = long["data_month"].astype(int)
        long = long[long["data_year"].isin(YEARS)]
        long = long[pd.notna(long["price"])]

        pulled_at = datetime.now(timezone.utc).isoformat()
        rows: list[dict[str, Any]] = []
        for _, r in long.iterrows():
            price = r["price"]
            try:
                price_f = float(price)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "commodity": r["commodity"],
                    "price": price_f,
                    "unit": UNIT_BY_COMMODITY[str(r["commodity"])],
                    "data_year": int(r["data_year"]),
                    "data_month": int(r["data_month"]),
                    "source": SCRIPT_NAME,
                    "pulled_at": pulled_at,
                }
            )

        for i in range(0, len(rows), UPSERT_BATCH):
            batch = rows[i : i + UPSERT_BATCH]
            client.table("commodity_prices").upsert(
                batch, on_conflict="commodity,data_year,data_month"
            ).execute()

        status = "success" if not missing_slugs else "partial"
        msg = None
        if missing_slugs:
            msg = (
                "Some commodity columns were not found in the workbook (no upsert for these slugs): "
                + ", ".join(missing_slugs)
                + ". Check Pink Sheet column renames or CMO-Historical-Data-Monthly layout."
            )
        finish_run(client, run_id, len(rows), status, msg)
        if msg:
            print(msg, file=sys.stderr)
        return 0

    except requests.HTTPError as e:
        err = f"HTTP error downloading Pink Sheet: {e}"
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
