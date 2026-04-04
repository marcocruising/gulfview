# ============================================================
# SCRIPT:  pull_faostat.py
# SOURCE:  UN FAO FAOSTAT
# URL:     https://www.fao.org/faostat/
# API KEY: Crops: none (bulk ZIP). Fertilizer: FAOSTAT API token or user/pass (faostat pkg v2).
# WRITES:  crop_production, fertilizer_production
# REFRESH: annual
# NOTES:   FAOSTAT_ZIP_PATH skips crop bulk download. Fertilizer uses faostat.get_data_df (not bulk URL).
#          Run: --dataset crops | fertilizer | all
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS = [2020, 2021, 2022, 2023, 2024]
HS_CODES = ["270900", "271100"]
COUNTRIES = None
FAOSTAT_PRODUCTION_ZIP = (
    "https://bulks-faostat.fao.org/production/"
    "Production_Crops_Livestock_E_All_Data_(Normalized).zip"
)
# FAOSTAT API dataset code (env FAOSTAT_FERTILIZER_API_CODE). RFB = by product; RFN = by nutrient (N/P/K).
FAOSTAT_FERTILIZER_API_CODE_DEFAULT = "RFB"
FAOSTAT_API_PAGE_LIMIT_DEFAULT = 50_000
FAOSTAT_API_AREA_CHUNK_DEFAULT = 40
CHUNK_ROWS = 80000
# -------------------------------------------------------------

import argparse
import io
import os
import re
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pycountry
import requests
from tqdm import tqdm

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "pull_faostat"
SOURCE_LABEL = "FAOSTAT crops (bulk ZIP) + fertilizers (faostat API)"
UPSERT_BATCH = 800
ITEM_TO_CROP = {15: "wheat", 27: "rice", 56: "maize", 236: "soybeans", 328: "cotton"}
ELEMENT_MAP = {
    5312: ("area_harvested", "hectares", None),
    5510: ("production", "tonnes", None),
    5412: ("yield", "tonnes_per_hectare", lambda v: float(v) / 1000.0),
}


def _m49_to_iso3(raw: object) -> str | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip().strip("'\"")
    if not s or not s.replace(" ", "").isdigit():
        return None
    s3 = s.replace(" ", "").zfill(3)
    try:
        c = pycountry.countries.get(numeric=s3)
        return c.alpha_3 if c else None
    except (LookupError, KeyError, TypeError, AttributeError):
        return None


# FAOSTAT composite / special area labels that pycountry cannot map cleanly — skip row.
_FAO_AREA_LABEL_SKIP = frozenset(
    {
        "belgium-luxembourg",
        "china, hong kong sar",
        "china, macao sar",
        "melanesia",
        "micronesia",
        "polynesia",
    }
)


def _area_label_to_iso3(label: object) -> str | None:
    """Map FAOSTAT `Area` column (country name) to ISO3."""
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return None
    s = str(label).strip()
    if not s:
        return None
    low = s.lower()
    if low in _FAO_AREA_LABEL_SKIP:
        return None
    try:
        c = pycountry.countries.get(name=s)
        if c:
            return str(c.alpha_3)
    except (KeyError, LookupError, TypeError, AttributeError):
        pass
    try:
        return str(pycountry.countries.search_fuzzy(s)[0].alpha_3)
    except LookupError:
        pass
    if "," in s:
        return _area_label_to_iso3(s.split(",")[0].strip())
    return None



def _load_zip_bytes(url: str, env_path_var: str) -> bytes:
    p = os.environ.get(env_path_var)
    if p and Path(p).is_file():
        return Path(p).read_bytes()
    r = requests.get(
        url,
        timeout=300,
        headers={"User-Agent": "hormuz-supply-chain/1.0"},
    )
    r.raise_for_status()
    return r.content


def _element_label_to_metric(label: str) -> str | None:
    """Map FAOSTAT Element label to fertilizer_production.metric."""
    el = label.strip().lower()
    if not el:
        return None
    if "value" in el and ("usd" in el or "slc" in el or "lcu" in el or "int $" in el):
        return None
    if "import" in el:
        if "quantity" in el or "qty" in el or el == "import":
            return "import"
        return None
    if "export" in el:
        if "quantity" in el or "qty" in el or el == "export":
            return "export"
        return None
    if el == "production" or el.startswith("production "):
        return "production"
    if "agricultural use" in el or "fertilizer use" in el:
        return "consumption"
    if "consumption" in el and "feed" not in el:
        return "consumption"
    return None


def _item_to_fertilizer_type(item: str) -> str | None:
    """Map FAOSTAT Item label to schema fertilizer_type (narrow V1 set)."""
    s = item.strip().lower()
    if not s:
        return None
    if "ammonium nitrate" in s or "ammonium sulphate" in s or "ammonium sulfate" in s:
        return None
    if "npk" in s or "nitrogen, phosphate and potash" in s:
        return "npk"
    if "diammonium" in s or re.search(r"\bdap\b", s):
        return "dap"
    if "monoammonium" in s or ("map" in s and "phosphate" in s):
        return "map"
    if "urea" in s:
        return "urea"
    if "ammonia" in s:
        return "ammonia"
    return None


def _value_to_tonnes(value: float, unit_raw: object) -> float | None:
    if unit_raw is None or (isinstance(unit_raw, float) and pd.isna(unit_raw)):
        return value
    u = str(unit_raw).lower().strip()
    if not u:
        return value
    if "1000" in u or "thousand" in u or "10^3" in u:
        return float(value) * 1000.0
    if "million" in u or "10^6" in u:
        return float(value) * 1_000_000.0
    if "kg" in u and "1000" not in u:
        return float(value) / 1000.0
    if any(x in u for x in ("tonne", "metric ton", "mt ", " m t", " t", "tons")):
        return float(value)
    if u in ("t",):
        return float(value)
    return None


def _aggregate_fertilizer_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Sum value_tonnes for duplicate natural keys (multiple FAOSTAT items per type)."""
    sums: dict[tuple[str, str, str, int], float] = defaultdict(float)
    meta: dict[tuple[str, str, str, int], tuple[str, str]] = {}
    for r in rows:
        key = (r["country"], r["fertilizer_type"], r["metric"], r["data_year"])
        v = r.get("value_tonnes")
        if v is None:
            continue
        sums[key] += float(v)
        meta[key] = (r["source"], r["pulled_at"])
    out: list[dict[str, Any]] = []
    for key, total in sums.items():
        src, pulled = meta[key]
        out.append(
            {
                "country": key[0],
                "fertilizer_type": key[1],
                "metric": key[2],
                "value_tonnes": total,
                "data_year": key[3],
                "source": src,
                "pulled_at": pulled,
            }
        )
    return out


def _canon_columns(df: pd.DataFrame) -> dict[str, str]:
    return {str(c).lower().strip(): str(c) for c in df.columns}


def _pick_col(canon: dict[str, str], *names: str) -> str | None:
    for n in names:
        k = n.lower().strip()
        if k in canon:
            return canon[k]
    return None


def _configure_faostat_api() -> None:
    """faostat v2 requires JWT or username/password before any API call."""
    import faostat

    token = (os.environ.get("FAOSTAT_API_TOKEN") or "").strip()
    user = (os.environ.get("FAOSTAT_USERNAME") or "").strip()
    pw = (os.environ.get("FAOSTAT_PASSWORD") or "").strip()
    if token:
        faostat.set_requests_args(token=token)
    elif user and pw:
        faostat.set_requests_args(username=user, password=pw)
    else:
        raise RuntimeError(
            "Fertilizer pull requires FAOSTAT API credentials (faostat package v2): "
            "set FAOSTAT_API_TOKEN or FAOSTAT_USERNAME and FAOSTAT_PASSWORD in .env. "
            "Bulk fertilizer ZIPs are often CloudFront/geo blocked; the API avoids that path."
        )


def _fertilizer_fao_area_codes(dataset_code: str) -> list[str]:
    """FAOSTAT `area` parameter values (FAO geographic codes), not the string 'all'."""
    import faostat

    raw = (os.environ.get("FAOSTAT_API_AREAS") or "").strip()
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()]
    adf = faostat.get_par_df(dataset_code, "area")
    countries = adf[adf["aggregate_type"] == "0"]
    return [str(c).strip() for c in countries["code"].tolist()]


def _fetch_fertilizer_api_df() -> pd.DataFrame:
    """
    Query fertilizer dataset in chunks: pars={'area': [...], 'year': [...]}.
    The API does not treat area='all' as valid — it returns an empty frame.
    """
    import faostat

    code = (
        os.environ.get("FAOSTAT_FERTILIZER_API_CODE") or FAOSTAT_FERTILIZER_API_CODE_DEFAULT
    ).strip()
    raw_limit = os.environ.get("FAOSTAT_API_PAGE_LIMIT")
    limit = int(raw_limit) if raw_limit else FAOSTAT_API_PAGE_LIMIT_DEFAULT
    chunk_env = os.environ.get("FAOSTAT_API_AREA_CHUNK")
    chunk_sz = int(chunk_env) if chunk_env else FAOSTAT_API_AREA_CHUNK_DEFAULT
    sleep_s = float(os.environ.get("FAOSTAT_API_SLEEP_SEC", "0.2"))

    years = [str(y) for y in YEARS]
    areas = _fertilizer_fao_area_codes(code)
    if not areas:
        raise RuntimeError("No FAO area codes resolved for fertilizer API (empty FAOSTAT_API_AREAS?).")

    parts: list[pd.DataFrame] = []
    n_chunks = (len(areas) + chunk_sz - 1) // chunk_sz
    for i in tqdm(range(0, len(areas), chunk_sz), desc="FAOSTAT fertilizer API", total=n_chunks):
        chunk = areas[i : i + chunk_sz]
        df = faostat.get_data_df(
            code,
            pars={"area": chunk, "year": years},
            strval=False,
            limit=limit,
        )
        if not df.empty:
            parts.append(df)
        if sleep_s > 0 and i + chunk_sz < len(areas):
            time.sleep(sleep_s)

    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _process_fertilizer_api_df(df: pd.DataFrame, pulled_at: str) -> list[dict[str, Any]]:
    if df.empty:
        return []
    canon = _canon_columns(df)
    m49c = _pick_col(canon, "area code (m49)")
    area_c = _pick_col(canon, "area")
    item_c = _pick_col(canon, "item")
    elem_c = _pick_col(canon, "element")
    year_c = _pick_col(canon, "year")
    value_c = _pick_col(canon, "value")
    unit_c = _pick_col(canon, "unit")
    missing = [n for n, c in [
        ("Item", item_c),
        ("Element", elem_c),
        ("Year", year_c),
        ("Value", value_c),
    ] if c is None]
    if missing:
        raise RuntimeError(
            "Unexpected FAOSTAT API columns; missing: "
            f"{missing}. Got columns: {list(df.columns)}"
        )
    if not m49c and not area_c:
        raise RuntimeError(
            "Unexpected FAOSTAT API columns: need 'Area' or 'Area Code (M49)'. "
            f"Got: {list(df.columns)}"
        )

    work = df.copy()
    work[year_c] = pd.to_numeric(work[year_c], errors="coerce")
    work = work[work[year_c].isin(YEARS)]
    fert_rows: list[dict[str, Any]] = []
    for _, r in work.iterrows():
        iso3: str | None = None
        if m49c:
            iso3 = _m49_to_iso3(r.get(m49c))
        if not iso3 and area_c:
            iso3 = _area_label_to_iso3(r.get(area_c))
        if not iso3:
            continue
        item = r.get(item_c)
        if pd.isna(item):
            continue
        ftype = _item_to_fertilizer_type(str(item))
        if not ftype:
            continue
        el_label = str(r.get(elem_c) or "")
        metric = _element_label_to_metric(el_label)
        if not metric:
            continue
        val = r.get(value_c)
        if pd.isna(val):
            continue
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        unit_val = r.get(unit_c) if unit_c else None
        tonnes = _value_to_tonnes(v, unit_val)
        if tonnes is None:
            continue
        fert_rows.append(
            {
                "country": iso3,
                "fertilizer_type": ftype,
                "metric": metric,
                "value_tonnes": tonnes,
                "data_year": int(r[year_c]),
                "source": SCRIPT_NAME,
                "pulled_at": pulled_at,
            }
        )
    return _aggregate_fertilizer_rows(fert_rows)


def _process_crops_zip(zf: zipfile.ZipFile, pulled_at: str) -> list[dict[str, Any]]:
    csv_name = next(n for n in zf.namelist() if n.endswith(".csv") and "Normalized" in n)
    crop_rows: list[dict[str, Any]] = []
    with zf.open(csv_name) as zcsv:
        for chunk in tqdm(
            pd.read_csv(zcsv, chunksize=CHUNK_ROWS, encoding="utf-8", low_memory=False),
            desc="FAOSTAT crops",
        ):
            sub = chunk[
                chunk["Item Code"].isin(ITEM_TO_CROP.keys())
                & chunk["Element Code"].isin(ELEMENT_MAP.keys())
                & chunk["Year"].isin(YEARS)
            ]
            for _, r in sub.iterrows():
                iso3 = _m49_to_iso3(r.get("Area Code (M49)"))
                if not iso3:
                    continue
                crop = ITEM_TO_CROP[int(r["Item Code"])]
                ec = int(r["Element Code"])
                metric, unit, fn = ELEMENT_MAP[ec]
                val = r.get("Value")
                if pd.isna(val):
                    continue
                try:
                    v = float(val)
                except (TypeError, ValueError):
                    continue
                if fn:
                    v = fn(v)
                crop_rows.append(
                    {
                        "country": iso3,
                        "crop": crop,
                        "metric": metric,
                        "value": v,
                        "unit": unit,
                        "data_year": int(r["Year"]),
                        "source": SCRIPT_NAME,
                        "pulled_at": pulled_at,
                    }
                )
    return crop_rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Load FAOSTAT crops and/or fertilizers into Supabase.")
    ap.add_argument(
        "--dataset",
        choices=("crops", "fertilizer", "all"),
        default="all",
        help="crops=bulk ZIP only; fertilizer=faostat API only; all=both (default).",
    )
    args = ap.parse_args()

    client = get_client()
    pulled_at = datetime.now(timezone.utc).isoformat()
    crop_rows: list[dict[str, Any]] = []
    fert_rows: list[dict[str, Any]] = []
    fert_error: str | None = None

    code = (
        os.environ.get("FAOSTAT_FERTILIZER_API_CODE") or FAOSTAT_FERTILIZER_API_CODE_DEFAULT
    ).strip()
    chunk_sz = int(os.environ["FAOSTAT_API_AREA_CHUNK"]) if os.environ.get("FAOSTAT_API_AREA_CHUNK") else FAOSTAT_API_AREA_CHUNK_DEFAULT
    run_id = start_run(
        client,
        SCRIPT_NAME,
        SOURCE_LABEL,
        {
            "years": YEARS,
            "dataset": args.dataset,
            "crop_zip": FAOSTAT_PRODUCTION_ZIP,
            "fertilizer_api_code": code if args.dataset in ("fertilizer", "all") else None,
            "fertilizer_area_chunk": chunk_sz if args.dataset in ("fertilizer", "all") else None,
        },
    )

    if args.dataset in ("crops", "all"):
        try:
            raw_crops = _load_zip_bytes(FAOSTAT_PRODUCTION_ZIP, "FAOSTAT_ZIP_PATH")
            zc = zipfile.ZipFile(io.BytesIO(raw_crops))
            crop_rows = _process_crops_zip(zc, pulled_at)
        except Exception as e:
            finish_run(client, run_id, 0, "error", f"Crops bulk failed: {type(e).__name__}: {e}")
            print(f"Crops bulk failed: {e}", file=sys.stderr)
            return 1
        if not crop_rows:
            finish_run(client, run_id, 0, "error", "No FAOSTAT crop rows after filters.")
            return 1

    if args.dataset in ("fertilizer", "all"):
        try:
            _configure_faostat_api()
            fdf = _fetch_fertilizer_api_df()
            fert_rows = _process_fertilizer_api_df(fdf, pulled_at)
        except Exception as e:
            fert_error = f"Fertilizer API failed: {type(e).__name__}: {e}"
            fert_rows = []
        else:
            if not fert_rows:
                fert_error = (
                    "Fertilizer API returned no rows after V1 filters (item/element/year/units). "
                    "If using RFN, try FAOSTAT_FERTILIZER_API_CODE=RFB; widen YEARS or check FAOSTAT layout."
                )

    if crop_rows:
        for i in range(0, len(crop_rows), UPSERT_BATCH):
            client.table("crop_production").upsert(
                crop_rows[i : i + UPSERT_BATCH],
                on_conflict="country,crop,metric,data_year",
            ).execute()

    if fert_rows:
        for i in range(0, len(fert_rows), UPSERT_BATCH):
            client.table("fertilizer_production").upsert(
                fert_rows[i : i + UPSERT_BATCH],
                on_conflict="country,fertilizer_type,metric,data_year",
            ).execute()

    n_c, n_f = len(crop_rows), len(fert_rows)
    total_written = n_c + n_f

    if args.dataset == "crops":
        finish_run(client, run_id, n_c, "success", None)
        print(f"Upserted {n_c} crop rows.")
        return 0

    if args.dataset == "fertilizer":
        if fert_error and not fert_rows:
            finish_run(client, run_id, 0, "partial", fert_error)
            print(fert_error, file=sys.stderr)
            return 0
        if fert_error:
            finish_run(client, run_id, n_f, "partial", fert_error)
            print(f"Upserted {n_f} fertilizer rows. {fert_error}")
            return 0
        finish_run(client, run_id, n_f, "success", None)
        print(f"Upserted {n_f} fertilizer rows.")
        return 0

    if fert_error and not fert_rows:
        msg = f"{fert_error} | Upserted {n_c} crop rows; fertilizer_production unchanged."
        finish_run(client, run_id, n_c, "partial", msg)
        print(f"Upserted {n_c} crop rows. {msg}")
        return 0
    if fert_error:
        msg = f"{fert_error} | Crops {n_c}, fertilizer {n_f} rows."
        finish_run(client, run_id, total_written, "partial", msg)
        print(f"Upserted {n_c} crop and {n_f} fertilizer rows. {msg}")
        return 0

    finish_run(client, run_id, total_written, "success", None)
    print(f"Upserted {n_c} crop rows and {n_f} fertilizer rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
