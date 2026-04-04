# ============================================================
# SCRIPT:  pull_faostat.py
# SOURCE:  UN FAO FAOSTAT
# URL:     https://www.fao.org/faostat/
# API KEY: not required (bulk ZIP)
# WRITES:  crop_production
# REFRESH: annual
# NOTES:   FAOSTAT_ZIP_PATH env skips download. Fertilizer not in this dataset.
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS = [2020, 2021, 2022, 2023, 2024]
HS_CODES = ["270900", "271100"]
COUNTRIES = None
FAOSTAT_PRODUCTION_ZIP = (
    "https://bulks-faostat.fao.org/production/"
    "Production_Crops_Livestock_E_All_Data_(Normalized).zip"
)
CHUNK_ROWS = 80000
# -------------------------------------------------------------

import io
import os
import sys
import zipfile
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
SOURCE_LABEL = "FAOSTAT Production (bulk normalized)"
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


def _load_zip_bytes() -> bytes:
    p = os.environ.get("FAOSTAT_ZIP_PATH")
    if p and Path(p).is_file():
        return Path(p).read_bytes()
    r = requests.get(
        FAOSTAT_PRODUCTION_ZIP,
        timeout=300,
        headers={"User-Agent": "hormuz-supply-chain/1.0"},
    )
    r.raise_for_status()
    return r.content


def main() -> int:
    client = get_client()
    run_id = start_run(
        client,
        SCRIPT_NAME,
        SOURCE_LABEL,
        {"years": YEARS, "zip": FAOSTAT_PRODUCTION_ZIP},
    )
    try:
        raw = _load_zip_bytes()
        zf = zipfile.ZipFile(io.BytesIO(raw))
        csv_name = next(n for n in zf.namelist() if n.endswith(".csv") and "Normalized" in n)
        pulled_at = datetime.now(timezone.utc).isoformat()
        crop_rows: list[dict[str, Any]] = []

        with zf.open(csv_name) as zcsv:
            for chunk in tqdm(
                pd.read_csv(zcsv, chunksize=CHUNK_ROWS, encoding="utf-8", low_memory=False),
                desc="FAOSTAT",
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

        if not crop_rows:
            finish_run(client, run_id, 0, "error", "No FAOSTAT rows after filters.")
            return 1

        for i in range(0, len(crop_rows), UPSERT_BATCH):
            client.table("crop_production").upsert(
                crop_rows[i : i + UPSERT_BATCH],
                on_conflict="country,crop,metric,data_year",
            ).execute()

        msg = "fertilizer_production not loaded (separate FAOSTAT inputs dataset)."
        finish_run(client, run_id, len(crop_rows), "partial", msg)
        print(f"Upserted {len(crop_rows)} crop rows. {msg}")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
