# ============================================================
# SCRIPT:  load_jodi.py
# SOURCE:  JODI (Joint Organisations Data Initiative) CSV exports
# URL:     https://www.jodidata.org/ (manual download)
# API KEY: not required
# WRITES:  jodi_energy_observations
# REFRESH: monthly (re-download CSVs when JODI updates)
# NOTES:   Place flat *.csv under data/jodi/ (same column layout for oil/gas).
#          OBS_VALUE may be numeric, "-", or "x" (suppressed); non-numeric → obs_value NULL.
#          Gas exports (STAGING_world_NewFormat.csv) span ~2009–present; default MIN_DATA_YEAR
#          keeps recent periods only (~40% of rows for 2020+). Use --all-years for full history.
# ============================================================

# --- CONFIGURATION — edit these values before running --------
# Glob under data/jodi/ — only *.csv files are read.
JODI_GLOB = "*.csv"
CHUNK_ROWS = 50_000
UPSERT_BATCH = 800
# Only load rows with data_year >= this (inclusive). None = no filter.
# Default 2020 aligns with BACI YEARS in load_baci.py; gas file has ~18k rows/year at steady state.
MIN_DATA_YEAR = 2020
# -------------------------------------------------------------

import argparse
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

SCRIPT_NAME = "load_jodi"
SOURCE_LABEL = "JODI (Joint Organisations Data Initiative) CSV"
JODI_DIR = _ROOT / "data" / "jodi"

EXPECTED_COLS = frozenset(
    {
        "REF_AREA",
        "TIME_PERIOD",
        "ENERGY_PRODUCT",
        "FLOW_BREAKDOWN",
        "UNIT_MEASURE",
        "OBS_VALUE",
        "ASSESSMENT_CODE",
    }
)


def _iso2_to_iso3(alpha2: str) -> str | None:
    s = str(alpha2).strip().upper()
    if len(s) != 2:
        return None
    try:
        c = pycountry.countries.get(alpha_2=s)
        return c.alpha_3 if c else None
    except (LookupError, KeyError, AttributeError, TypeError):
        return None


def _chunk_to_rows(
    df: pd.DataFrame,
    source_file: str,
    pulled_at: str,
    source_tag: str,
    min_data_year: int | None,
) -> list[dict[str, Any]]:
    missing = EXPECTED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {sorted(missing)}")

    work = df.copy()
    work["ref_area"] = work["REF_AREA"].astype(str).str.strip().str.upper()
    tp = work["TIME_PERIOD"].astype(str).str.strip()
    parts = tp.str.split("-", n=1, expand=True)
    work["data_year"] = pd.to_numeric(parts[0], errors="coerce")
    work["data_month"] = pd.to_numeric(parts[1], errors="coerce")
    work = work[work["data_year"].notna() & work["data_month"].notna()]
    work["data_year"] = work["data_year"].astype(int)
    work["data_month"] = work["data_month"].astype(int)
    work = work[(work["data_month"] >= 1) & (work["data_month"] <= 12)]
    if min_data_year is not None:
        work = work[work["data_year"] >= min_data_year]

    work["country"] = work["ref_area"].map(_iso2_to_iso3)

    raw_obs = work["OBS_VALUE"].astype(str).str.strip()
    num = pd.to_numeric(raw_obs, errors="coerce")
    work["obs_value"] = num
    work["obs_value_raw"] = raw_obs.mask(raw_obs.eq(""))

    ac = pd.to_numeric(work["ASSESSMENT_CODE"], errors="coerce")
    work["assessment_code"] = ac.astype("Int64")

    work["energy_product"] = work["ENERGY_PRODUCT"].astype(str).str.strip()
    work["flow_breakdown"] = work["FLOW_BREAKDOWN"].astype(str).str.strip()
    work["unit_measure"] = work["UNIT_MEASURE"].astype(str).str.strip()

    cols = [
        "ref_area",
        "country",
        "data_year",
        "data_month",
        "energy_product",
        "flow_breakdown",
        "unit_measure",
        "obs_value",
        "obs_value_raw",
        "assessment_code",
    ]
    recs = work[cols].to_dict(orient="records")
    out: list[dict[str, Any]] = []
    for r in recs:
        nv = r["obs_value"]
        val = float(nv) if nv is not None and pd.notna(nv) else None
        acv = r["assessment_code"]
        ac_int = int(acv) if acv is not None and pd.notna(acv) else None
        country = r["country"]
        raw = r["obs_value_raw"]
        raw_out = None if raw is None or (isinstance(raw, float) and pd.isna(raw)) or raw == "" else str(raw)
        out.append(
            {
                "ref_area": r["ref_area"],
                "country": country if country is not None and pd.notna(country) else None,
                "data_year": int(r["data_year"]),
                "data_month": int(r["data_month"]),
                "energy_product": r["energy_product"],
                "flow_breakdown": r["flow_breakdown"],
                "unit_measure": r["unit_measure"],
                "obs_value": val,
                "obs_value_raw": raw_out,
                "assessment_code": ac_int,
                "source_file": source_file,
                "source": source_tag,
                "pulled_at": pulled_at,
            }
        )
    return out


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Last row wins per natural key (same-batch duplicate guard for Supabase upsert)."""
    key = (
        lambda r: (
            r["ref_area"],
            r["data_year"],
            r["data_month"],
            r["energy_product"],
            r["flow_breakdown"],
            r["unit_measure"],
        )
    )
    by_k: dict[tuple[Any, ...], dict[str, Any]] = {}
    for r in rows:
        by_k[key(r)] = r
    return list(by_k.values())


def _discover_files() -> list[Path]:
    if not JODI_DIR.is_dir():
        return []
    return sorted(JODI_DIR.glob(JODI_GLOB))


def main() -> int:
    ap = argparse.ArgumentParser(description="Load JODI CSV files into jodi_energy_observations.")
    ap.add_argument(
        "--file",
        type=str,
        help="Single CSV filename under data/jodi/ (default: all *.csv)",
    )
    ap.add_argument(
        "--min-year",
        type=int,
        default=None,
        metavar="YYYY",
        help=f"Only load TIME_PERIOD rows with year >= YYYY (default: MIN_DATA_YEAR={MIN_DATA_YEAR} in script)",
    )
    ap.add_argument(
        "--all-years",
        action="store_true",
        help="Load every year in each CSV (ignore MIN_DATA_YEAR and --min-year)",
    )
    args = ap.parse_args()

    if args.all_years:
        min_data_year: int | None = None
    elif args.min_year is not None:
        min_data_year = args.min_year
    else:
        min_data_year = MIN_DATA_YEAR

    try:
        client = get_client()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    params: dict[str, Any] = {
        "jodi_glob": JODI_GLOB,
        "file": args.file,
        "min_data_year": min_data_year,
        "all_years": args.all_years,
    }
    try:
        run_id = start_run(client, SCRIPT_NAME, SOURCE_LABEL, params)
    except Exception as e:
        print(f"Could not log pipeline run (check Supabase keys): {e}", file=sys.stderr)
        return 1

    paths = _discover_files()
    if args.file:
        p = JODI_DIR / args.file
        if not p.is_file():
            msg = f"File not found: {p}"
            finish_run(client, run_id, 0, "error", msg)
            print(msg, file=sys.stderr)
            return 1
        paths = [p]

    if not paths:
        msg = f"No CSV files matching {JODI_GLOB} in {JODI_DIR}."
        finish_run(client, run_id, 0, "error", msg)
        print(msg, file=sys.stderr)
        return 1

    pulled_at = datetime.now(timezone.utc).isoformat()
    source_tag = SCRIPT_NAME
    total_written = 0
    files_used: list[str] = []

    try:
        for path in paths:
            files_used.append(path.name)
            try:
                reader = pd.read_csv(
                    path,
                    chunksize=CHUNK_ROWS,
                    dtype=str,
                    keep_default_na=False,
                )
            except Exception as e:
                finish_run(client, run_id, total_written, "error", f"Failed reading {path.name}: {e}")
                print(f"Failed reading {path}: {e}", file=sys.stderr)
                return 1

            for chunk in reader:
                chunk.columns = [c.strip() for c in chunk.columns]
                try:
                    rows = _chunk_to_rows(
                        chunk, path.name, pulled_at, source_tag, min_data_year
                    )
                except ValueError as e:
                    finish_run(client, run_id, total_written, "error", f"{path.name}: {e}")
                    print(f"{path.name}: {e}", file=sys.stderr)
                    return 1

                rows = _dedupe_rows(rows)
                if not rows:
                    continue

                for i in range(0, len(rows), UPSERT_BATCH):
                    batch = rows[i : i + UPSERT_BATCH]
                    client.table("jodi_energy_observations").upsert(
                        batch,
                        on_conflict="ref_area,data_year,data_month,energy_product,flow_breakdown,unit_measure",
                    ).execute()
                    total_written += len(batch)

        finish_run(client, run_id, total_written, "success", None)
        print(f"Upserted {total_written} rows from {len(files_used)} file(s): {', '.join(files_used)}")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, total_written, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
