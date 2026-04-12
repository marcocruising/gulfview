"""JODI `jodi_energy_observations` — display labels and Streamlit column help.

Wire CSV columns are defined in ``loaders/load_jodi.py`` (``EXPECTED_COLS``).
Official short-name guides: JODI-Oil / JODI-Gas manuals and item-name PDFs on jodidata.org.
Oil and gas share the same column layout but not always the same codelist values.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
import streamlit as st

try:
    import plotly.graph_objects as go
except ImportError:  # pragma: no cover
    go = None  # type: ignore[misc, assignment]

# JODI-Gas item names (and overlaps with oil); unknown codes fall back to generic text in UI.
FLOW_BREAKDOWN_LABELS: dict[str, str] = {
    # Gas (JODI-Gas short names guide)
    "INDPROD": "Production",
    "OSOURCES": "Receipts from other sources",
    "TOTIMPSB": "Imports (total)",
    "IMPLNG": "Imports — LNG",
    "IMPPIP": "Imports — pipeline",
    "TOTEXPSB": "Exports (total)",
    "EXPLNG": "Exports — LNG",
    "EXPPIP": "Exports — pipeline",
    "STOCKCH": "Stock change",
    "TOTDEMC": "Gross inland deliveries (calculated)",
    "STATDIFF": "Statistical difference",
    "TOTDEMO": "Gross inland deliveries (observed)",
    "MAINTOT": "Of which: electricity and heat generation",
    "CLOSTLV": "Closing stocks",
    "CONVER": "Conversion factor (m³/tonne)",
    # Common oil / shared-style codes seen in JODI-Oil exports
    "TOTPROD": "Production (total)",
    "TOTIMP": "Imports (total)",
    "TOTEXP": "Exports (total)",
    "TOTDEM": "Demand (total)",
    "STOCKS": "Stocks / stock change",
    "CLOSST": "Closing stocks",
    "OPENST": "Opening stocks",
    # JODI-Oil extended questionnaire (primary / secondary; exact codes vary by export)
    "RECEIPTS": "Primary receipts (observed)",
    "RECEIPT": "Receipts",
    "TOTREC": "Receipts (total)",
    "REFININ": "Refinery intake",
    "REFNTI": "Refinery intake",
    "REFININT": "Refinery intake",
    "TOTREFIN": "Refinery intake (total)",
    "REFINOUT": "Refinery output",
    "REFOUT": "Refinery output",
    "REFNTO": "Refinery output",
    "PRTRANS": "Product transfers",
    "TOTTRANS": "Product transfers (total)",
    "BACKFLO": "Backflows",
    "BACKFL": "Backflows",
    "TBACKF": "Backflows (total)",
    "TOTBACKF": "Backflows (total)",
    "TOTTRCB": "Transfers and backflows (total)",
    "BLENDING": "Blending",
    "DIRECTU": "Direct use",
    "TOTDIR": "Direct use (total)",
    "STATDIF": "Statistical difference",
    "STOCKCHA": "Stock change",
    "STOCKVL": "Stock levels",
    "LNGIMP": "Imports — LNG",
    "LNGEXP": "Exports — LNG",
    "PIPEIMP": "Imports — pipeline",
    "PIPEEXP": "Exports — pipeline",
}

# Typical ENERGY_PRODUCT codes (gas + oil); extend as needed when new exports appear.
ENERGY_PRODUCT_LABELS: dict[str, str] = {
    "NATGAS": "Natural gas",
    "CRUDEOIL": "Crude oil",
    "NGL": "Natural gas liquids",
    "LPG": "LPG",
    "NONCRUDE": "Non-crude (other)",
    "TOTAL": "Total products",
    "TOTPROD": "Total oil products",
}

UNIT_MEASURE_LABELS: dict[str, str] = {
    "M3": "Million m³ (15 °C, 760 mm Hg) — gas",
    "TJ": "Terajoules",
    "KTONS": "Thousand metric tons (e.g. LNG)",
    "KBD": "Thousand barrels per day",
    "T": "Metric tons",
    "MT": "Metric tons",
    "BBL": "Barrels",
}

# JODI CSV ASSESSMENT_CODE: 1 = blue, 2 = yellow, 3 = white (JODI-Oil / JODI-Gas downloads).
ASSESSMENT_CODE_LABELS: dict[int, str] = {
    1: "Reasonable comparability (blue)",
    2: "Use with caution — consult metadata (yellow)",
    3: "Not assessed (white)",
}

# Database column → header tooltip (Streamlit ``help=``).
JODI_COLUMN_HELP: dict[str, str] = {
    "id": "Surrogate key from Postgres.",
    "ref_area": "JODI CSV ``REF_AREA``: reporting economy as ISO 3166-1 alpha-2 (two-letter).",
    "country": "ISO 3166-1 alpha-3 derived from ``REF_AREA`` for joins to other tables in this app.",
    "country_name": "Resolved English country name (and code) for readability.",
    "data_year": "Calendar year, parsed from CSV ``TIME_PERIOD`` (``YYYY-MM``).",
    "data_month": "Month 1–12, parsed from CSV ``TIME_PERIOD``.",
    "date": "First day of the month for charting (derived in the app).",
    "energy_product": "JODI CSV ``ENERGY_PRODUCT``: SDMX product code (e.g. ``NATGAS``, ``CRUDEOIL``). Oil vs gas product lists differ — use JODI short/long name PDFs.",
    "product_label": "Plain-language label when the code is known; otherwise see **Product code**.",
    "flow_breakdown": "JODI CSV ``FLOW_BREAKDOWN``: balance / flow dimension (e.g. production, imports). Exact strings must match the export.",
    "flow_label": "Plain-language label when the code is known; otherwise see **Flow code**.",
    "unit_measure": "JODI CSV ``UNIT_MEASURE``: unit code for ``obs_value`` (e.g. ``M3``, ``TJ``, ``KBD``).",
    "unit_label": "Plain-language unit when the code is known.",
    "obs_value": "Numeric value parsed from CSV ``OBS_VALUE``. Empty when the source is “-”, “x”, or non-numeric.",
    "obs_value_raw": "Original ``OBS_VALUE`` text from the CSV (includes “-” or “x” where published).",
    "assessment_code": "JODI CSV ``ASSESSMENT_CODE``: data confidence colour code as an integer (typically 1 / 2 / 3).",
    "assessment_label": "Readable assessment category derived from **Assessment code**.",
    "source_file": "CSV filename under ``data/jodi/`` used when loading this row.",
    "source": "Loader script name that inserted the row.",
    "pulled_at": "UTC timestamp when the loader batch ran.",
}

# Shorter column titles for headers (keep codes visible in separate columns).
JODI_COLUMN_TITLE: dict[str, str] = {
    "id": "Row id",
    "ref_area": "Reporter (ISO2)",
    "country": "Reporter (ISO3)",
    "country_name": "Country",
    "data_year": "Year",
    "data_month": "Month",
    "date": "Period (date)",
    "energy_product": "Product code",
    "product_label": "Product",
    "flow_breakdown": "Flow code",
    "flow_label": "Flow",
    "unit_measure": "Unit code",
    "unit_label": "Unit",
    "obs_value": "Value",
    "obs_value_raw": "Value (raw)",
    "assessment_code": "Assessment code",
    "assessment_label": "Assessment",
    "source_file": "Source file",
    "source": "Loader",
    "pulled_at": "Loaded at",
}

# Default column order for the main data table (codes + labels + values).
JODI_PRIMARY_COLUMN_ORDER: tuple[str, ...] = (
    "date",
    "data_year",
    "data_month",
    "ref_area",
    "country",
    "country_name",
    "energy_product",
    "product_label",
    "flow_breakdown",
    "flow_label",
    "unit_measure",
    "unit_label",
    "obs_value",
    "obs_value_raw",
    "assessment_code",
    "assessment_label",
    "source_file",
)

JODI_TECHNICAL_COLUMN_ORDER: tuple[str, ...] = ("id", "source", "pulled_at")

# Markdown body for Streamlit expander — national balance semantics (not geographic routes).
JODI_FLOW_MEANINGS_EXPANDER_BODY = """
Each **`flow_breakdown`** value is one **line in the reporter country’s national oil/gas questionnaire**
for that month, product, and unit. Rows are **balance-sheet concepts** (supply, trade, stocks, demand, etc.),
not physical routes between countries. Compare rows only when they share the same **`unit_measure`**.

- **Official prose definitions** (flows and products): [Data Available in the JODI-Oil World Database](https://www.jodidata.org/oil/support/user-guide/data-available-in-the-jodi-oil-world-database.aspx)
- **Manuals and methodology**: [JODI Manuals](https://www.jodidata.org/capacity-building/jodi-manuals.aspx)
- **Short code ↔ full name** (PDF): download from [JODI-Oil data downloads](https://www.jodidata.org/oil/database/data-downloads.aspx) (*List of short names…*).
"""

# Sankey: classify questionnaire lines as supply-side (left) vs disposition-side (right). Stock *change* uses sign.
_JODI_SANKEY_SKIP: frozenset[str] = frozenset({"CLOSST", "OPENST", "CLOSTLV"})
_JODI_SANKEY_LEFT: frozenset[str] = frozenset(
    {
        "INDPROD",
        "TOTPROD",
        "OSOURCES",
        "TOTIMP",
        "TOTIMPSB",
        "IMPLNG",
        "IMPPIP",
        "LNGIMP",
        "PIPEIMP",
        "RECEIPTS",
        "RECEIPT",
        "TOTREC",
        "REFINOUT",
        "REFOUT",
        "REFNTO",
        "PRTRANS",
        "TOTTRANS",
        "BLENDING",
    }
)
_JODI_SANKEY_RIGHT: frozenset[str] = frozenset(
    {
        "TOTEXP",
        "TOTEXPSB",
        "EXPLNG",
        "EXPPIP",
        "LNGEXP",
        "PIPEEXP",
        "TOTDEM",
        "TOTDEMO",
        "TOTDEMC",
        "REFININ",
        "REFNTI",
        "REFININT",
        "TOTREFIN",
        "BACKFLO",
        "BACKFL",
        "TBACKF",
        "TOTBACKF",
        "STATDIFF",
        "STATDIF",
        "MAINTOT",
        "DIRECTU",
        "TOTDIR",
        "CONVER",
    }
)
_JODI_SANKEY_STOCK_CHANGE: frozenset[str] = frozenset({"STOCKCH", "STOCKS", "STOCKCHA", "STOCKVL"})


def jodi_flow_sankey_side(flow_breakdown: object, obs_value: object) -> str | None:
    """Return ``\"left\"`` (supply-oriented), ``\"right\"`` (disposition), or ``None`` to omit from Sankey."""
    if flow_breakdown is None or (isinstance(flow_breakdown, float) and pd.isna(flow_breakdown)):
        return None
    code = str(flow_breakdown).strip().upper()
    if code in _JODI_SANKEY_SKIP:
        return None
    try:
        v = float(obs_value) if obs_value is not None and not (isinstance(obs_value, float) and pd.isna(obs_value)) else 0.0
    except (TypeError, ValueError):
        v = 0.0
    if code in _JODI_SANKEY_STOCK_CHANGE:
        if v < 0:
            return "left"
        if v > 0:
            return "right"
        return None
    if code in _JODI_SANKEY_LEFT:
        return "left"
    if code in _JODI_SANKEY_RIGHT:
        return "right"
    if code.startswith("IMP") or ("IMP" in code and not code.startswith("TOTDEM")):
        return "left"
    if code.startswith("EXP") or code.startswith("EXPLNG") or code.startswith("EXPPIP"):
        return "right"
    if "DEM" in code or "DEMO" in code:
        return "right"
    if "PROD" in code or code.endswith("PROD"):
        return "left"
    return "left"


def build_jodi_balance_sankey_figure(
    df: pd.DataFrame,
    *,
    hub_label: str = "Illustrative balance (selected month)",
    title: str | None = None,
) -> Any | None:
    """Plotly Sankey for one month × one unit: supply-oriented flows → hub → disposition-oriented flows.

    Link widths use **absolute** values; hover shows signed values. Not a closed material balance.
    Returns ``None`` if Plotly is unavailable or there is nothing to draw.
    """
    if go is None:
        return None
    need = {"flow_breakdown", "obs_value"}
    if df.empty or not need.issubset(df.columns):
        return None
    work = df.dropna(subset=["obs_value"]).copy()
    if work.empty:
        return None
    work["obs_value"] = pd.to_numeric(work["obs_value"], errors="coerce")
    work = work.dropna(subset=["obs_value"])
    if work.empty:
        return None
    if "flow_label" not in work.columns:
        work["flow_label"] = work["flow_breakdown"].map(_flow_label)
    work = (
        work.groupby(["flow_breakdown", "flow_label"], dropna=False)["obs_value"]
        .sum()
        .reset_index()
    )

    left_rows: list[tuple[str, str, float]] = []
    right_rows: list[tuple[str, str, float]] = []
    for _, r in work.iterrows():
        code = r["flow_breakdown"]
        lbl = str(r["flow_label"]).strip() or str(code)
        val = float(r["obs_value"])
        side = jodi_flow_sankey_side(code, val)
        if side is None:
            continue
        tup = (str(code), lbl, val)
        if side == "left":
            left_rows.append(tup)
        else:
            right_rows.append(tup)

    if not left_rows and not right_rows:
        return None

    node_labels: list[str] = [hub_label]
    for code, lbl, _ in left_rows:
        node_labels.append(f"{lbl} [{code}]")
    n_left = len(left_rows)
    base_right = 1 + n_left
    for code, lbl, _ in right_rows:
        node_labels.append(f"{lbl} [{code}]")

    sources: list[int] = []
    targets: list[int] = []
    values: list[float] = []
    customdata: list[str] = []

    for i, (_, _c, val) in enumerate(left_rows):
        idx = 1 + i
        w = abs(val)
        if w <= 0:
            continue
        sources.append(idx)
        targets.append(0)
        values.append(w)
        customdata.append(f"Signed: {val:,.6g}")

    for j, (_, _c, val) in enumerate(right_rows):
        idx = base_right + j
        w = abs(val)
        if w <= 0:
            continue
        sources.append(0)
        targets.append(idx)
        values.append(w)
        customdata.append(f"Signed: {val:,.6g}")

    if not values:
        return None

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    label=node_labels,
                    pad=18,
                    thickness=12,
                    line=dict(color="rgba(0,0,0,0.35)", width=0.35),
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    customdata=customdata,
                    hovertemplate="%{customdata}<extra></extra>",
                ),
            )
        ]
    )
    fig.update_layout(
        title=title or "",
        font=dict(size=11),
        height=max(420, 60 * max(len(left_rows), len(right_rows), 4)),
        margin=dict(l=20, r=20, t=50 if title else 30, b=20),
    )
    return fig


def _flow_label(code: object) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ""
    s = str(code).strip()
    return FLOW_BREAKDOWN_LABELS.get(s, "— (see JODI manual for this code)")


def _product_label(code: object) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ""
    s = str(code).strip()
    return ENERGY_PRODUCT_LABELS.get(s, "— (see JODI manual for this code)")


def _unit_label(code: object) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ""
    s = str(code).strip()
    return UNIT_MEASURE_LABELS.get(s, "— (see JODI manual for this code)")


def _assessment_label(code: object) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ""
    try:
        i = int(code)
    except (TypeError, ValueError):
        return f"— (unknown code {code!r})"
    return ASSESSMENT_CODE_LABELS.get(i, f"— (code {i}; see JODI assessment guide)")


def prepare_jodi_display_df(
    df: pd.DataFrame,
    *,
    country_label_fn: Callable[[object], str] | None = None,
    include_date: bool = False,
    energy_date_fn: Any | None = None,
) -> pd.DataFrame:
    """Add human-readable columns; optional ``date`` via ``energy_date_fn`` (e.g. app’s ``_energy_date_column``)."""
    out = df.copy()
    if include_date and energy_date_fn is not None and not out.empty:
        out["date"] = energy_date_fn(out)
    if "flow_breakdown" in out.columns:
        out["flow_label"] = out["flow_breakdown"].map(_flow_label)
    if "energy_product" in out.columns:
        out["product_label"] = out["energy_product"].map(_product_label)
    if "unit_measure" in out.columns:
        out["unit_label"] = out["unit_measure"].map(_unit_label)
    if "assessment_code" in out.columns:
        out["assessment_label"] = out["assessment_code"].map(_assessment_label)
    if country_label_fn is not None and "country" in out.columns:
        out["country_name"] = out["country"].map(
            lambda x: country_label_fn(x) if pd.notna(x) and str(x).strip() else ""
        )
    return _reorder_jodi_columns(out)


def _reorder_jodi_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Put known display columns first; keep any others at the end."""
    preferred = [c for c in JODI_PRIMARY_COLUMN_ORDER if c in df.columns]
    rest = [c for c in df.columns if c not in preferred]
    return df[preferred + rest]


def jodi_columns_for_view(df: pd.DataFrame, *, technical: bool) -> list[str]:
    """Column order for ``st.dataframe``. When ``technical`` is False, omit loader metadata columns."""
    primary = [c for c in JODI_PRIMARY_COLUMN_ORDER if c in df.columns]
    tech = [c for c in JODI_TECHNICAL_COLUMN_ORDER if c in df.columns]
    rest = [c for c in df.columns if c not in primary and c not in tech]
    if technical:
        return primary + tech + rest
    return primary + rest


def build_jodi_column_config(df: pd.DataFrame, columns: list[str] | None = None) -> dict[str, Any]:
    """``column_config`` for ``st.dataframe`` — tooltips and titles."""
    cols = columns if columns is not None else list(df.columns)
    cfg: dict[str, Any] = {}
    for col in cols:
        if col not in df.columns:
            continue
        title = JODI_COLUMN_TITLE.get(col, col.replace("_", " ").title())
        help_text = JODI_COLUMN_HELP.get(col, "")
        h = help_text or None
        s = df[col]
        if col == "date" or pd.api.types.is_datetime64_any_dtype(s):
            cfg[col] = st.column_config.DatetimeColumn(title, help=h, format="YYYY-MM-DD")
        elif col in ("obs_value", "assessment_code", "data_year", "data_month", "id") or pd.api.types.is_numeric_dtype(
            s
        ):
            intfmt = col in ("assessment_code", "data_year", "data_month", "id")
            cfg[col] = st.column_config.NumberColumn(
                title,
                help=h,
                format="%d" if intfmt else "%.6g",
            )
        else:
            cfg[col] = st.column_config.TextColumn(title, help=h)
    return cfg
