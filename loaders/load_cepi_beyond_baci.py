# ============================================================
# SCRIPT:  load_cepi_beyond_baci.py
# SOURCE:  CEPII databases other than BACI (manual CSV download)
# URL:     https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele.asp
# API KEY: not required
# WRITES:  cepii_protee_hs6, cepii_geodep_import_dependence
# REFRESH: occasional (when CEPII republishes files)
# NOTES:   Place files under data/cepi/ — see module docstring below.
# ============================================================
#
# ---------------------------------------------------------------------------
# What each dataset is — and how it differs from BACI / existing Supabase data
# ---------------------------------------------------------------------------
#
# 1) BACI (already in `bilateral_trade` via load_baci.py)
#    - **What it is:** Reconciled **bilateral trade flows**: who exports what to whom,
#      at HS6, in **value (USD thousands)** and **quantity (tonnes)** per calendar year.
#    - **Use:** “How much urea/wheat/crude moved between country A and B?”
#    - **Revision in your files:** HS 2022 (BACI_HS22_*).
#
# 2) ProTEE 0.1 → table `cepii_protee_hs6`
#    - **What it is:** One row per **HS6 product** with **estimated import-demand
#      (trade) elasticities** from tariff variation (structural gravity). The CSV
#      columns are `zero`, `positive`, and `sigma` (CEPII’s published elasticity
#      value; their web text also calls this “Elasticity”).
#    - **What it is NOT:** Not trade volumes, not prices, not bilateral partners.
#      It is a **parameter** for “how sensitive imports are to trade costs” at the
#      product level — useful for simulation / shock analysis, not for ranking flows.
#    - **CEPII caveat:** In the public file, HS6 estimates that were **non-significant
#      at 1%** are flagged (`zero` = 1); **positive significant** elasticities are
#      flagged (`positive` = 1). For those products CEPII **substitutes the HS4 sector
#      average** into the published elasticity column — so read `zero`/`positive`
#      before interpreting `trade_elasticity`.
#    - **HS revision:** CEPII documents **HS 2007** for this file — not the same
#      HS vintage as your BACI HS22 flows. Joining ProTEE to `bilateral_trade.hs6_code`
#      is approximate unless you apply a concordance.
#    - **Difference vs existing tables:** Unlike `bilateral_trade` (facts) or
#      `commodity_prices` (prices), this is **econometric metadata per HS6**.
#
# 3) GeoDep → table `cepii_geodep_import_dependence`
#    - **What it is:** **Country × HS6 × year (2019–2022)** indicators of whether
#      a product is **“import dependent”** in CEPII’s sense, built from **BACI** flows.
#      Columns include **Herfindahl–Hirschman** measures (supplier concentration and
#      world export concentration), an **import-to-export ratio** proxy for domestic
#      substitutability, a **persistence** flag (criteria satisfied in a multi-year
#      window), **strategic sector** dummies (agrifood, chemicals, pharma, steel,
#      defense, transport, electronics, other), and the **leading foreign supplier**
#      code with its **share** of imports.
#    - **What it is NOT:** Not bilateral trade rows (use `bilateral_trade`). Not a
#      full product panel for all years like BACI. It answers “is this country’s
#      imports of this HS6 **concentrated and structurally dependent**, and on whom?”
#    - **Methodology (CEPII):** Criterion 1 — HHI of import sources; > 0.4 ⇒
#      concentrated. Criterion 2 — HHI of world exports; > 0.4 ⇒ concentrated.
#      Criterion 3 — imports/exports for the country-product; > 1 ⇒ hard to substitute
#      with domestic supply; **9999** in source data when exports are null.
#      Criterion 4 — persistence: the three conditions hold in at least two years
#      of a three-year window (`flag_persistent_criteria` / CSV `c4`).
#    - **Column `import_dpt` in CSV → `import_value`:** CEPII lists this alongside
#      the criteria; in the raw file it behaves as a **large skewed magnitude**
#      (trade value scale in CEPII’s construction — treat as **nominal import
#      exposure**, not an HHI). Keep in mind units are as in the CEPII release.
#    - **Leading partner codes (`first_odpt`):** Values like `EUN`, `USA` are **CEPII /
#      CHELEM-style partner codes**, not always ISO3 — join carefully.
#    - **Difference vs `bilateral_trade`:** BACI gives **each exporter–importer flow**;
#      GeoDep gives **one diagnostic row per importer–product–year** summarizing
#      dependence and top supplier.
#
# 4) WTFC + CHELEM zips under data/cepi/ (NOT loaded by this script)
#    - **WTFC (World Trade Flows Characterization):** Reconciled **unit values** at
#      **exporter–importer–HS6–year**, plus **trade type** (e.g. one-way vs intra-
#      industry) and **price range** (low/mid/high vs other flows of the same product).
#      **HS96** vintage in your zip — older nomenclature than BACI HS22.
#    - **CHELEM “price_range” / “type” zips:** Same WTFC methodology expressed on
#      **CHELEM product categories** (71 sectors etc.), not raw HS6 BACI lines.
#    - **Difference vs BACI:** BACI prioritizes **consistent values and quantities**
#      for macro trade; WTFC prioritizes **unit-value comparability and flow
#      typology** — complementary, not a duplicate.
#    - **Why no loader yet:** Each yearly CSV inside `WTFC_HS96_*.zip` is **hundreds
#      of MB**; loading everything would dwarf current DB size. Add a dedicated
#      streaming loader + optional HS/year filters if you need WTFC in Supabase.
#
# ---------------------------------------------------------------------------
# Files expected
# ---------------------------------------------------------------------------
#   data/cepi/ProTEE_0_1.csv
#   data/cepi/geodep_data.csv
#
# ---------------------------------------------------------------------------

from __future__ import annotations

# --- CONFIGURATION — optional HS6 prefix filter for GeoDep (None = all products) --
GEODEP_HS_PREFIXES: list[str] | None = None  # e.g. ["2709", "3102"] to limit rows
GEODEP_READ_CHUNKSIZE = 50_000
UPSERT_BATCH = 1_200
# ----------------------------------------------------------------------------------

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "load_cepi_beyond_baci"


def _upsert_batches(
    client: Any,
    table: str,
    rows: list[dict[str, Any]],
    on_conflict: str,
    *,
    batch_size: int = UPSERT_BATCH,
    max_attempts: int = 4,
) -> None:
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        delay = 1.0
        for attempt in range(max_attempts):
            try:
                client.table(table).upsert(batch, on_conflict=on_conflict).execute()
                break
            except Exception:
                if attempt == max_attempts - 1:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
CEPI_DIR = _ROOT / "data" / "cepi"
PROTEE_CSV = CEPI_DIR / "ProTEE_0_1.csv"
GEODEP_CSV = CEPI_DIR / "geodep_data.csv"


def _hs_prefix_allowed(hs6: str) -> bool:
    if GEODEP_HS_PREFIXES is None:
        return True
    s = str(hs6).strip().zfill(6)[:6]
    for h in GEODEP_HS_PREFIXES:
        h = str(h).strip()
        if len(h) == 6:
            if s == h or s.startswith(h):
                return True
        else:
            if s.startswith(h):
                return True
    return False


def _as_bool(v: Any) -> bool:
    if pd.isna(v):
        return False
    try:
        return int(float(v)) == 1
    except (TypeError, ValueError):
        return False


def load_protee(client: Any, run_id: int) -> int:
    try:
        if not PROTEE_CSV.is_file():
            finish_run(client, run_id, 0, "error", f"Missing {PROTEE_CSV}")
            print(f"Missing {PROTEE_CSV}", file=sys.stderr)
            return 1

        pulled_at = datetime.now(timezone.utc).isoformat()
        source = "cepi_ProTEE_0_1"
        df = pd.read_csv(PROTEE_CSV, dtype={"HS6": str}, low_memory=False)
        if not {"HS6", "zero", "positive"}.issubset(df.columns):
            finish_run(
                client,
                run_id,
                0,
                "error",
                "ProTEE CSV missing HS6, zero, or positive columns.",
            )
            return 1

        sigma_col = "sigma" if "sigma" in df.columns else None
        rows: list[dict[str, Any]] = []
        for _, r in df.iterrows():
            hs = str(r["HS6"]).strip().zfill(6)[:6]
            if not hs.isdigit():
                continue
            elas = None
            if sigma_col and pd.notna(r[sigma_col]) and str(r[sigma_col]).strip() != "":
                try:
                    elas = float(r[sigma_col])
                except (TypeError, ValueError):
                    elas = None
            rows.append(
                {
                    "hs6_code": hs,
                    "flag_nonsignificant_at_1pct": _as_bool(r["zero"]),
                    "flag_positive_significant": _as_bool(r["positive"]),
                    "trade_elasticity": elas,
                    "hs_revision": "HS2007",
                    "source": source,
                    "pulled_at": pulled_at,
                }
            )

        _upsert_batches(client, "cepii_protee_hs6", rows, "hs6_code")

        finish_run(client, run_id, len(rows), "success", None)
        print(f"ProTEE: upserted {len(rows)} HS6 rows into cepii_protee_hs6.")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


def load_geodep(client: Any, run_id: int) -> int:
    try:
        if not GEODEP_CSV.is_file():
            finish_run(client, run_id, 0, "error", f"Missing {GEODEP_CSV}")
            print(f"Missing {GEODEP_CSV}", file=sys.stderr)
            return 1

        pulled_at = datetime.now(timezone.utc).isoformat()
        source = "cepi_geodep_data"
        total = 0

        reader = pd.read_csv(
            GEODEP_CSV,
            dtype={"iso_d": str, "hs6": str},
            chunksize=GEODEP_READ_CHUNKSIZE,
            low_memory=False,
        )
        first = next(reader, None)
        if first is None:
            finish_run(client, run_id, 0, "error", "GeoDep CSV is empty.")
            return 1
        missing = [
            c
            for c in (
                "iso_d",
                "hs6",
                "year",
                "import_dpt",
                "c1",
                "c2",
                "c3",
                "c4",
                "dependent",
                "sect_agrifood",
                "sect_chemicals",
                "sect_pharmaceuticals",
                "sect_steel",
                "sect_defense",
                "sect_transport",
                "sect_electronics",
                "sect_other",
                "first_odpt",
                "share_odpt",
            )
            if c not in first.columns
        ]
        if missing:
            finish_run(
                client,
                run_id,
                0,
                "error",
                f"GeoDep CSV missing columns: {', '.join(missing)}",
            )
            return 1

        batch: list[dict[str, Any]] = []

        def flush() -> None:
            nonlocal batch, total
            if not batch:
                return
            _upsert_batches(
                client,
                "cepii_geodep_import_dependence",
                batch,
                "country,hs6_code,data_year",
            )
            total += len(batch)
            batch = []

        def process_chunk(chunk: pd.DataFrame) -> None:
            for _, r in chunk.iterrows():
                hs = str(r["hs6"]).strip().zfill(6)[:6]
                if not hs.isdigit() or not _hs_prefix_allowed(hs):
                    continue
                country = str(r["iso_d"]).strip().upper()
                if len(country) != 3:
                    continue
                try:
                    year = int(r["year"])
                except (TypeError, ValueError):
                    continue

                def num(col: str) -> float | None:
                    if col not in r.index or pd.isna(r[col]):
                        return None
                    try:
                        return float(r[col])
                    except (TypeError, ValueError):
                        return None

                batch.append(
                    {
                        "country": country,
                        "hs6_code": hs,
                        "data_year": year,
                        "import_value": num("import_dpt"),
                        "hhi_import_concentration": num("c1"),
                        "hhi_world_export_concentration": num("c2"),
                        "import_to_export_ratio": num("c3"),
                        "flag_persistent_criteria": _as_bool(r["c4"]),
                        "flag_import_dependent": _as_bool(r["dependent"]),
                        "sector_strategic_agrifood": _as_bool(r["sect_agrifood"]),
                        "sector_strategic_chemicals": _as_bool(r["sect_chemicals"]),
                        "sector_strategic_pharmaceuticals": _as_bool(
                            r["sect_pharmaceuticals"]
                        ),
                        "sector_strategic_steel": _as_bool(r["sect_steel"]),
                        "sector_strategic_defense": _as_bool(r["sect_defense"]),
                        "sector_strategic_transport": _as_bool(r["sect_transport"]),
                        "sector_strategic_electronics": _as_bool(r["sect_electronics"]),
                        "sector_strategic_other": _as_bool(r["sect_other"]),
                        "leading_exporter_code": None
                        if pd.isna(r["first_odpt"])
                        else str(r["first_odpt"]).strip(),
                        "leading_exporter_share_pct": num("share_odpt"),
                        "source": source,
                        "pulled_at": pulled_at,
                    }
                )
                if len(batch) >= GEODEP_READ_CHUNKSIZE:
                    flush()

        process_chunk(first)
        for chunk in reader:
            process_chunk(chunk)

        flush()

        if total == 0:
            msg = "GeoDep: no rows after filters (check file and GEODEP_HS_PREFIXES)."
            finish_run(client, run_id, 0, "partial", msg)
            print(msg, file=sys.stderr)
            return 0

        finish_run(client, run_id, total, "success", None)
        print(f"GeoDep: upserted {total} rows into cepii_geodep_import_dependence.")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Load CEPII ProTEE and GeoDep CSVs from data/cepi/ into Supabase."
    )
    ap.add_argument(
        "dataset",
        choices=["protee", "geodep", "all"],
        help="Which dataset to load",
    )
    args = ap.parse_args()

    try:
        client = get_client()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    params: dict[str, Any] = {"dataset": args.dataset, "geodep_hs_prefixes": GEODEP_HS_PREFIXES}
    try:
        run_id = start_run(client, SCRIPT_NAME, "CEPII (ProTEE / GeoDep)", params)
    except Exception as e:
        print(f"Could not log pipeline run: {e}", file=sys.stderr)
        return 1

    if args.dataset == "protee":
        return load_protee(client, run_id)
    if args.dataset == "geodep":
        return load_geodep(client, run_id)
    # all — separate pipeline_runs rows per leg
    r1 = load_protee(client, run_id)
    if r1 != 0:
        return r1
    try:
        run_id2 = start_run(
            client,
            SCRIPT_NAME,
            "CEPII (ProTEE / GeoDep)",
            {**params, "leg": "geodep"},
        )
    except Exception as e:
        print(f"Could not log GeoDep pipeline run: {e}", file=sys.stderr)
        return 1
    return load_geodep(client, run_id2)


if __name__ == "__main__":
    raise SystemExit(main())
