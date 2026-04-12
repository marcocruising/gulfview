# ============================================================
# SCRIPT:  load_baci.py
# SOURCE:  CEPII BACI bilateral trade
# URL:     http://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37
# API KEY: not required (manual CSV download)
# WRITES:  bilateral_trade, bilateral_trade_data_years (year cache for RPC dropdowns)
# REFRESH: annual
# NOTES:   Place BACI_HS*_Y*.csv files under data/baci/
#          Optional --exporter-full-hs (repeat per country) / --importer-full-hs load all HS6
#          for those legs so the app can show full supplier concentration (importer × HS6).
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS = [2020, 2021, 2022, 2023, 2024]
# HS chapter / prefix allowlist (e.g. "2709", "1001"). Empty = include every HS6 in the BACI file (very large).
HS_CODES: list[str] = []
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
HS6_ALLOWLIST: set[str] = set()


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
    s6 = s.zfill(6)[:6]
    if len(s6) != 6 or not s6.isdigit():
        return False
    if HS6_ALLOWLIST and s6 in HS6_ALLOWLIST:
        return True
    if not HS_CODES:
        # No chapter filter: all HS6, unless CLI set --hs6-codes only (then only allowlist matches above).
        if HS6_ALLOWLIST:
            return False
        return True
    for h in HS_CODES:
        h = str(h).strip()
        if len(h) == 6:
            if s6 == h or s6.startswith(h):
                return True
        else:
            if s6.startswith(h):
                return True
    return False


def _discover_files(year: int | None) -> list[Path]:
    if not BACI_DIR.is_dir():
        return []
    # CEPII zips often unpack into a versioned subfolder; rglob finds nested CSVs.
    paths = sorted(BACI_DIR.rglob(BACI_GLOB))
    if year is not None:
        needle = f"_Y{year}"
        paths = [p for p in paths if needle in p.name]
    return paths


def _load_baci_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"k": str, "i": int, "j": int, "t": int}, low_memory=False)


def _year_from_filename(path: Path) -> int | None:
    m = re.search(r"_Y(\d{4})", path.name)
    return int(m.group(1)) if m else None


def _norm_iso3(token: str) -> str | None:
    s = str(token).strip().upper()
    if len(s) != 3 or not s.isalpha():
        return None
    return s


def _include_row_mask(
    df: pd.DataFrame,
    *,
    exporters_full_hs: frozenset[str],
    importers_full_hs: frozenset[str],
) -> pd.Series:
    """True if row should be loaded: HS filter (or all HS6 if HS_CODES empty) OR full-HS exporter/importer match."""
    exp = df["i"].map(_baci_numeric_to_iso3)
    imp = df["j"].map(_baci_numeric_to_iso3)
    k = df["k"].astype(str)
    hs_ok = k.map(_hs_allowed)
    m = hs_ok
    if exporters_full_hs:
        m = m | exp.isin(exporters_full_hs)
    if importers_full_hs:
        m = m | imp.isin(set(importers_full_hs))
    return m


def main() -> int:
    ap = argparse.ArgumentParser(description="Load BACI CSV files into bilateral_trade.")
    ap.add_argument("--year", type=int, help="Only files for this reference year")
    ap.add_argument("--all", action="store_true", help="Load every BACI CSV under data/baci/")
    ap.add_argument(
        "--hs6-codes",
        default="",
        help="Comma-separated six-digit HS6 codes to additionally include globally (loads all exporters/importers for these codes).",
    )
    ap.add_argument(
        "--hs6-codes-file",
        default="",
        help="Path to a text file listing HS6 codes (one per line) to additionally include globally.",
    )
    ap.add_argument(
        "--exporter-full-hs",
        action="append",
        default=[],
        metavar="ISO3",
        dest="exporter_full_hs_list",
        help="Also load all HS6 lines where exporter is this ISO3. Repeat for each country "
        "(e.g. Gulf group analysis: pass once per member). Same as legacy single use.",
    )
    ap.add_argument(
        "--importer-full-hs",
        action="append",
        default=[],
        metavar="ISO3",
        help="Also load all HS6 lines where importer is this ISO3 (repeat for each partner). "
        "Needed for best-fidelity supplier concentration in the app.",
    )
    args = ap.parse_args()
    if not args.all and args.year is None:
        ap.error("Specify --year YYYY or --all")

    global HS6_ALLOWLIST
    HS6_ALLOWLIST = set()
    if args.hs6_codes:
        for tok in str(args.hs6_codes).split(","):
            s = str(tok).strip()
            if not s:
                continue
            if not s.isdigit():
                print(f"Invalid HS6 code in --hs6-codes (digits only): {s!r}", file=sys.stderr)
                return 1
            HS6_ALLOWLIST.add(s.zfill(6)[:6])
    if args.hs6_codes_file:
        p = Path(str(args.hs6_codes_file)).expanduser()
        if not p.is_file():
            print(f"--hs6-codes-file not found: {str(p)!r}", file=sys.stderr)
            return 1
        try:
            for line in p.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s:
                    continue
                if not s.isdigit():
                    print(f"Invalid HS6 code in --hs6-codes-file (digits only): {s!r}", file=sys.stderr)
                    return 1
                HS6_ALLOWLIST.add(s.zfill(6)[:6])
        except Exception as e:
            print(f"Failed reading --hs6-codes-file: {e}", file=sys.stderr)
            return 1

    exporters_full: set[str] = set()
    for raw in args.exporter_full_hs_list or []:
        iso = _norm_iso3(str(raw))
        if not iso:
            print(f"Invalid --exporter-full-hs (need 3-letter ISO3): {raw!r}", file=sys.stderr)
            return 1
        exporters_full.add(iso)
    exporters_full_hs = frozenset(exporters_full)
    imp_set: set[str] = set()
    for raw in args.importer_full_hs or []:
        iso = _norm_iso3(raw)
        if not iso:
            print(f"Invalid --importer-full-hs (need 3-letter ISO3): {raw!r}", file=sys.stderr)
            return 1
        imp_set.add(iso)
    importers_full_hs = frozenset(imp_set)

    try:
        client = get_client()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    params: dict[str, Any] = {
        "years": YEARS,
        "hs_codes": HS_CODES,
        "hs6_allowlist": sorted(HS6_ALLOWLIST),
        "year_arg": args.year,
        "all": args.all,
        "exporter_full_hs": sorted(exporters_full),
        "importer_full_hs": sorted(imp_set),
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
        files_used: list[str] = []
        rows_written = 0
        years_touched: set[int] = set()

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
            mask = _include_row_mask(
                df,
                exporters_full_hs=exporters_full_hs,
                importers_full_hs=importers_full_hs,
            )
            sub = df.loc[mask, :].copy()
            if sub.empty:
                continue

            # Map numeric country codes to ISO3 using a small lookup dict (fast; avoids per-row pycountry calls).
            i_vals = pd.Series(sub["i"].unique())
            j_vals = pd.Series(sub["j"].unique())
            i_map = {int(v): _baci_numeric_to_iso3(v) for v in i_vals.dropna().tolist()}
            j_map = {int(v): _baci_numeric_to_iso3(v) for v in j_vals.dropna().tolist()}

            sub["exporter"] = sub["i"].map(i_map)
            sub["importer"] = sub["j"].map(j_map)
            sub["hs6_code"] = sub["k"].astype(str).str.strip().str.zfill(6).str.slice(0, 6)
            sub["trade_value_usd"] = pd.to_numeric(sub.get("v"), errors="coerce")
            if "q" in sub.columns:
                sub["quantity_tonnes"] = pd.to_numeric(sub.get("q"), errors="coerce")
            else:
                sub["quantity_tonnes"] = None

            keep = sub["exporter"].notna() & sub["importer"].notna() & sub["trade_value_usd"].notna()
            sub = sub.loc[keep, ["exporter", "importer", "hs6_code", "trade_value_usd", "quantity_tonnes", "t"]]
            if sub.empty:
                continue

            sub = sub.rename(columns={"t": "data_year"})
            sub["hs_description"] = None
            sub["source"] = f"baci_{path.stem}"
            sub["pulled_at"] = pulled_at
            # Supabase JSON encoder rejects NaN/Inf. Convert float columns to object so None sticks.
            sub = sub.astype(object).where(pd.notna(sub), None)

            # Upsert in chunks to avoid large payloads.
            records = sub.to_dict(orient="records")
            for i in range(0, len(records), UPSERT_BATCH):
                batch = records[i : i + UPSERT_BATCH]
                client.table("bilateral_trade").upsert(
                    batch, on_conflict="exporter,importer,hs6_code,data_year"
                ).execute()
            rows_written += len(records)
            years_touched.add(int(data_year))

        if rows_written <= 0:
            msg = "No rows after HS/year filters (check YEARS and HS_CODES / HS6 allowlist vs file contents)."
            finish_run(client, run_id, 0, "partial", msg)
            print(msg, file=sys.stderr)
            return 0

        for y in sorted(years_touched):
            client.table("bilateral_trade_data_years").upsert(
                [{"data_year": y}],
                on_conflict="data_year",
            ).execute()

        finish_run(client, run_id, int(rows_written), "success", None)
        print(f"Loaded {rows_written} rows from {len(files_used)} file(s).")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
