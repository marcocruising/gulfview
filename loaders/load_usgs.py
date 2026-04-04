# ============================================================
# SCRIPT:  load_usgs.py
# SOURCE:  USGS Mineral Commodity Summaries (MCS) data CSV
# URL:     https://www.usgs.gov/centers/national-minerals-information-center
# API KEY: not required (manual download)
# WRITES:  usgs_mineral_statistics
# REFRESH: annual when USGS publishes new MCS tables
# NOTES:   Place MCS*Commodities_Data.csv (or similar) under data/usgs/.
#          Read with latin-1. Source has duplicate logical keys; upsert uses
#          record_fingerprint (hash of row fields including Value + Notes).
#          myb3-*.xlsx yearbooks: use subcommand `facilities` when files exist
#          (see README); schema usgs_country_mineral_facilities reserved.
# ============================================================

# --- CONFIGURATION — edit these values before running --------
MCS_GLOB = "MCS*_Commodities_Data.csv"
UPSERT_BATCH = 500
# -------------------------------------------------------------

import argparse
import hashlib
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pycountry

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

USGS_DIR = _ROOT / "data" / "usgs"

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "load_usgs"
SOURCE_LABEL = "USGS Mineral Commodity Summaries (MCS) CSV"

# Aggregates and multi-country rows — no single ISO3.
COUNTRY_NAME_TO_ISO3: dict[str, str | None] = {
    "World total": None,
    "Other countries": None,
    "China, Germany, and Russia": None,
    "United States": "USA",
    "United Kingdom": "GBR",
    "Korea, Republic of": "KOR",
    "Korea, North": "PRK",
    "Burma": "MMR",
    "Congo (Kinshasa)": "COD",
    "The Bahamas": "BHS",
    "Taiwan": "TWN",
    "Russia": "RUS",
    "Iran": "IRN",
    "Vietnam": "VNM",
    "Laos": "LAO",
    "Côte d'Ivoire": "CIV",
}

EXPECTED_MCS_COLS = [
    "MCS chapter",
    "Section",
    "Commodity",
    "Country",
    "Statistics",
    "Statistics_detail",
    "Unit",
    "Year",
    "Value",
    "Notes",
    "Is critical mineral 2025",
    "Other notes",
]


def _norm_text(x: object) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    return unicodedata.normalize("NFKC", s)


def _country_to_iso3(name: str) -> str | None:
    key = _norm_text(name)
    if not key:
        return None
    # CP1252 apostrophe vs ASCII in country names
    key_alt = key.replace("\u0092", "'").replace("\u2019", "'")
    if key in COUNTRY_NAME_TO_ISO3:
        return COUNTRY_NAME_TO_ISO3[key]
    if key_alt in COUNTRY_NAME_TO_ISO3:
        return COUNTRY_NAME_TO_ISO3[key_alt]
    if "ivoire" in key.lower():
        return "CIV"
    try:
        c = pycountry.countries.get(name=key)
        if c:
            return c.alpha_3
    except (LookupError, KeyError, AttributeError, TypeError):
        pass
    try:
        matches = pycountry.countries.search_fuzzy(key)
        if matches:
            return matches[0].alpha_3
    except (LookupError, AttributeError):
        pass
    return None


def _parse_critical(val: object) -> bool | None:
    s = _norm_text(val).lower()
    if s in ("yes", "y", "true", "1"):
        return True
    if s in ("no", "n", "false", "0"):
        return False
    return None


def _parse_value_numeric(raw: str) -> float | None:
    s = _norm_text(raw)
    if not s:
        return None
    # Pure integer / decimal with optional thousands commas
    cleaned = s.replace(",", "").strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", cleaned):
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _parse_year(val: object) -> tuple[int | None, str]:
    """Return (data_year, year_as_reported). data_year is None if unparseable."""
    s = _norm_text(val)
    if not s:
        return None, ""
    if re.fullmatch(r"\d{4}", s):
        return int(s), s
    m = re.match(r"(\d{4})\s*[-–]\s*(\d{2}|\d{4})", s)
    if m:
        return int(m.group(1)), s
    return None, s


def _record_fingerprint(
    chapter: str,
    section: str,
    commodity: str,
    country: str,
    statistics: str,
    detail: str,
    unit: str,
    year_as_reported: str,
    value_raw: str,
    notes: str,
) -> str:
    parts = [
        chapter,
        section,
        commodity,
        country,
        statistics,
        detail,
        unit,
        year_as_reported,
        value_raw,
        notes,
    ]
    joined = "\x1e".join(_norm_text(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _discover_mcs_files() -> list[Path]:
    if not USGS_DIR.is_dir():
        return []
    return sorted(USGS_DIR.glob(MCS_GLOB))


def _mcs_dataframe_to_rows(df: pd.DataFrame, source_file: str, pulled_at: str) -> list[dict[str, Any]]:
    cols = [c.strip() for c in df.columns]
    df = df.copy()
    df.columns = cols
    missing = [c for c in EXPECTED_MCS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing MCS columns: {missing}")

    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        chapter = _norm_text(r["MCS chapter"])
        section = _norm_text(r["Section"])
        commodity = _norm_text(r["Commodity"])
        country_name = _norm_text(r["Country"])
        statistics = _norm_text(r["Statistics"])
        detail = _norm_text(r["Statistics_detail"])
        unit = _norm_text(r["Unit"])
        notes = _norm_text(r["Notes"])
        other = _norm_text(r["Other notes"])
        value_raw = _norm_text(r["Value"])
        data_year, year_as_reported = _parse_year(r["Year"])
        if data_year is None:
            continue

        fp = _record_fingerprint(
            chapter,
            section,
            commodity,
            country_name,
            statistics,
            detail,
            unit,
            year_as_reported,
            value_raw,
            notes,
        )
        iso3 = _country_to_iso3(country_name)
        crit = _parse_critical(r["Is critical mineral 2025"])
        num = _parse_value_numeric(value_raw)

        rows.append(
            {
                "record_fingerprint": fp,
                "mcs_chapter": chapter,
                "section": section,
                "commodity": commodity,
                "country_name": country_name,
                "country_iso3": iso3,
                "statistics": statistics,
                "statistics_detail": detail,
                "unit": unit,
                "data_year": data_year,
                "year_as_reported": year_as_reported,
                "value_numeric": num,
                "value_raw": value_raw if value_raw else None,
                "notes": notes if notes else None,
                "other_notes": other if other else None,
                "is_critical_mineral_2025": crit,
                "source_file": source_file,
                "source": SCRIPT_NAME,
                "pulled_at": pulled_at,
            }
        )
    return rows


def cmd_mcs(client: Any, run_id: int, file_arg: str | None) -> int:
    paths = _discover_mcs_files()
    if file_arg:
        p = USGS_DIR / file_arg
        if not p.is_file():
            msg = f"MCS file not found: {p}"
            finish_run(client, run_id, 0, "error", msg)
            print(msg, file=sys.stderr)
            return 1
        paths = [p]

    if not paths:
        msg = (
            f"No MCS CSV matching {MCS_GLOB} in {USGS_DIR}. "
            "Download MCS commodities data from USGS NMIC."
        )
        finish_run(client, run_id, 0, "error", msg)
        print(msg, file=sys.stderr)
        return 1

    pulled_at = datetime.now(timezone.utc).isoformat()
    total = 0
    try:
        for path in paths:
            try:
                df = pd.read_csv(path, encoding="cp1252", dtype=str, keep_default_na=False)
            except UnicodeDecodeError:
                df = pd.read_csv(path, encoding="latin-1", dtype=str, keep_default_na=False)
            rows = _mcs_dataframe_to_rows(df, path.name, pulled_at)
            if not rows:
                finish_run(client, run_id, 0, "partial", f"No rows parsed from {path.name}")
                print(f"No rows parsed from {path.name}", file=sys.stderr)
                return 1

            for i in range(0, len(rows), UPSERT_BATCH):
                batch = rows[i : i + UPSERT_BATCH]
                client.table("usgs_mineral_statistics").upsert(
                    batch, on_conflict="record_fingerprint"
                ).execute()
                total += len(batch)

        finish_run(client, run_id, total, "success", None)
        print(f"Upserted {total} MCS rows from {len(paths)} file(s).")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, total, "error", err)
        print(err, file=sys.stderr)
        return 1


def cmd_facilities(client: Any, run_id: int) -> int:
    glob_paths = sorted(USGS_DIR.glob("myb3*.xlsx")) if USGS_DIR.is_dir() else []
    if not glob_paths:
        msg = (
            f"No myb3*.xlsx under {USGS_DIR}. Add USGS Minerals Yearbook country xlsx files; "
            "loader not yet implemented — table usgs_country_mineral_facilities is reserved."
        )
        finish_run(client, run_id, 0, "partial", msg)
        print(msg, file=sys.stderr)
        return 0
    msg = (
        "myb3*.xlsx files found but facilities ingest is not implemented yet. "
        "Inspect sheet layout and extend load_usgs.py facilities leg."
    )
    finish_run(client, run_id, 0, "partial", msg)
    print(msg, file=sys.stderr)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Load USGS files into Supabase.")
    ap.add_argument(
        "command",
        nargs="?",
        default="mcs",
        choices=("mcs", "facilities"),
        help="mcs = MCS commodities CSV (default); facilities = reserved for myb3 xlsx",
    )
    ap.add_argument(
        "--file",
        type=str,
        help=f"MCS filename under data/usgs/ (mcs only; default: all {MCS_GLOB})",
    )

    args = ap.parse_args()

    try:
        client = get_client()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    params: dict[str, Any] = {"command": args.command, "file": args.file}

    try:
        run_id = start_run(client, SCRIPT_NAME, SOURCE_LABEL, params)
    except Exception as e:
        print(f"Could not log pipeline run (check Supabase keys): {e}", file=sys.stderr)
        return 1

    if args.command == "mcs":
        return cmd_mcs(client, run_id, args.file)
    if args.command == "facilities":
        return cmd_facilities(client, run_id)
    finish_run(client, run_id, 0, "error", f"Unknown command {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
