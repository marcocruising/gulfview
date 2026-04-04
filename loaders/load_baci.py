# ============================================================
# SCRIPT:  load_baci.py
# SOURCE:  CEPII BACI bilateral trade
# URL:     http://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37
# API KEY: not required (manual CSV download)
# WRITES:  bilateral_trade
# REFRESH: annual
# NOTES:   Place BACI_HS*_Y*.csv files under data/baci/
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS = [2020, 2021, 2022, 2023, 2024]
HS_CODES = [
    "2709", "2711", "2710", "2814", "3102", "3103", "3104", "3105",
    "1001", "1006", "1005", "1201", "5201",
]
COUNTRIES = None
# -------------------------------------------------------------

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pycountry

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "load_baci"
SOURCE_LABEL = "CEPII BACI (HS bilateral trade)"
BACI_DIR = _ROOT / "data" / "baci"
BACI_GLOB = "BACI_HS*_Y*.csv"
UPSERT_BATCH = 800


def _baci_numeric_to_iso3(code: int | float) -> str | None:
    try:
        n = int(float(code))
    except (TypeError, ValueError):
        return None
    s = str(n).zfill(3)
    try:
        c = pycountry.countries.get(numeric=s)
        return c.alpha_3 if c else None
    except (LookupError, KeyError, AttributeError):
        return None


def _hs_allowed(hs6: str) -> bool:
    s = str(hs6).strip()
    if not s:
        return False
    for h in HS_CODES:
        h = str(h).strip()
        if len(h) == 6:
            if s == h or s.startswith(h):
                return True
        else:
            if s.startswith(h):
                return True
    return False


def _discover_files(year: int | None) -> list[Path]:
    if not BACI_DIR.is_dir():
        return []
    paths = sorted(BACI_DIR.glob(BACI_GLOB))
    if year is not None:
        needle = f"_Y{year}"
        paths = [p for p in paths if needle in p.name]
    return paths


def _load_baci_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"k": str, "i": int, "j": int, "t": int}, low_memory=False)


def _year_from_filename(path: Path) -> int | None:
    m = re.search(r"_Y(\d{4})", path.name)
    return int(m.group(1)) if m else None


def main() -> int:
    ap = argparse.ArgumentParser(description="Load BACI CSV files into bilateral_trade.")
    ap.add_argument("--year", type=int, help="Only files for this reference year")
    ap.add_argument("--all", action="store_true", help="Load every BACI CSV under data/baci/")
    args = ap.parse_args()
    if not args.all and args.year is None:
        ap.error("Specify --year YYYY or --all")

    try:
        client = get_client()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    params: dict[str, Any] = {
        "years": YEARS,
        "hs_codes": HS_CODES,
        "year_arg": args.year,
        "all": args.all,
    }
    try:
        run_id = start_run(client, SCRIPT_NAME, SOURCE_LABEL, params)
    except Exception as e:
        print(f"Could not log pipeline run (check Supabase keys): {e}", file=sys.stderr)
        return 1

    try:
        paths = _discover_files(None if args.all else args.year)
        if not paths:
            msg = (
                f"No BACI files matching {BACI_GLOB} in {BACI_DIR}. "
                "Download from CEPII and unzip into data/baci/."
            )
            finish_run(client, run_id, 0, "error", msg)
            print(msg, file=sys.stderr)
            return 1

        pulled_at = datetime.now(timezone.utc).isoformat()
        rows: list[dict[str, Any]] = []
        files_used: list[str] = []

        for path in paths:
            data_year = _year_from_filename(path)
            if data_year is None or data_year not in YEARS:
                continue
            try:
                df = _load_baci_csv(path)
            except Exception as e:
                finish_run(client, run_id, 0, "error", f"Failed reading {path.name}: {e}")
                print(f"Failed reading {path}: {e}", file=sys.stderr)
                return 1

            if not {"i", "j", "k", "t", "v"}.issubset(df.columns):
                finish_run(
                    client,
                    run_id,
                    0,
                    "error",
                    f"{path.name} missing expected columns i,j,k,t,v (BACI format).",
                )
                return 1

            files_used.append(path.name)
            sub = df[df["k"].map(_hs_allowed)].copy()
            qcol = "q" if "q" in sub.columns else None
            for _, r in sub.iterrows():
                exp = _baci_numeric_to_iso3(r["i"])
                imp = _baci_numeric_to_iso3(r["j"])
                if not exp or not imp:
                    continue
                qty = None
                if qcol and pd.notna(r[qcol]):
                    try:
                        qty = float(r[qcol])
                    except (TypeError, ValueError):
                        qty = None
                try:
                    val = float(r["v"])
                except (TypeError, ValueError):
                    continue
                hs6 = str(r["k"]).strip().zfill(6)[:6]
                rows.append(
                    {
                        "exporter": exp,
                        "importer": imp,
                        "hs6_code": hs6,
                        "hs_description": None,
                        "trade_value_usd": val,
                        "quantity_tonnes": qty,
                        "data_year": int(r["t"]),
                        "source": f"baci_{path.stem}",
                        "pulled_at": pulled_at,
                    }
                )

        if not rows:
            msg = "No rows after HS/year filters (check YEARS and HS_CODES vs file contents)."
            finish_run(client, run_id, 0, "partial", msg)
            print(msg, file=sys.stderr)
            return 0

        for i in range(0, len(rows), UPSERT_BATCH):
            batch = rows[i : i + UPSERT_BATCH]
            client.table("bilateral_trade").upsert(
                batch, on_conflict="exporter,importer,hs6_code,data_year"
            ).execute()

        finish_run(client, run_id, len(rows), "success", None)
        print(f"Loaded {len(rows)} rows from {len(files_used)} file(s).")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
