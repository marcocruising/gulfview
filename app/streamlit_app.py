"""Hormuz supply chain — read-only Supabase exploration dashboard."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import altair as alt
import pandas as pd
import pycountry
import streamlit as st
from supabase import Client

from utils import group_dependency_compute as gdc
from utils.supabase_client import get_client, get_read_client

PAGE_SIZE = 1000
# Cap row scans for distinct lists / large slices (full BACI can be huge).
BILATERAL_DISTINCT_SCAN_CAP = 120_000

# Sidebar navigation: Streamlit `st.tabs` runs *every* tab’s code on each rerun (any widget anywhere),
# which reloads the whole app. We render one section at a time.
_APP_SECTIONS: tuple[str, ...] = (
    "Prices over time",
    "Who trades what",
    "Country profile",
    "Exporter & partners",
    "Group dependencies",
    "Crop production",
    "Pipeline status",
    "Explore more",
)
BILATERAL_EXPORT_DRILLDOWN_CAP = 500_000

# Exporter drill-down tab: Gulf countries first (ISO3), then any other exporters found in BACI.
GULF_EXPORTER_ISO3_ORDER: tuple[str, ...] = ("SAU", "OMN", "KWT", "QAT", "IRQ", "BHR", "ARE", "IRN")


def _st_altair_bar_h_by_value(
    df: pd.DataFrame,
    value_col: str,
    label_col: str,
    *,
    x_title: str | None = None,
    row_height: int = 30,
    sort_by: str | None = None,
    value_format: str = ",.0f",
) -> None:
    """Horizontal bars with categories ordered by value (desc). `st.bar_chart` sorts the y-axis A–Z."""
    sort_field = sort_by or value_col
    need = {label_col, value_col, sort_field}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"_st_altair_bar_h_by_value: missing columns {missing}")
    d = df[list(need)].copy()
    h = max(200, row_height * len(d))
    xt = x_title if x_title is not None else value_col
    # Vega-Lite hides alternating labels by default when it thinks they overlap; disable that
    # and allow long "Country (XXX)" strings so every row is labeled.
    y_axis = alt.Axis(
        labelOverlap=False,
        labelLimit=900,
        labelPadding=6,
        title=None,
    )
    if sort_field == value_col:
        y_enc: Any = alt.Y(f"{label_col}:N", sort="-x", axis=y_axis)
    else:
        y_enc = alt.Y(
            f"{label_col}:N",
            sort=alt.EncodingSortField(field=sort_field, op="max", order="descending"),
            axis=y_axis,
        )
    chart = (
        alt.Chart(d)
        .mark_bar()
        .encode(
            x=alt.X(f"{value_col}:Q", title=xt),
            y=y_enc,
            tooltip=[
                alt.Tooltip(f"{label_col}:N", title=""),
                alt.Tooltip(f"{value_col}:Q", format=value_format, title=xt),
            ],
        )
        .properties(height=h, padding={"left": 24})
        .configure_axisY(labelAlign="right", labelBaseline="middle")
    )
    st.altair_chart(chart, width="stretch")


def _client() -> Client:
    """Prefer server key when present so RLS does not hide rows (e.g. table_catalog with RLS, no anon policy)."""
    try:
        return get_client()
    except RuntimeError:
        return get_read_client()


@st.cache_resource
def supabase() -> Client:
    return _client()


def fetch_all_pages(
    sb: Client,
    table: str,
    select: str = "*",
    eq_filters: dict[str, Any] | None = None,
    order_by: str = "id",
) -> list[dict[str, Any]]:
    """Read all rows matching optional equality filters using range pagination (PostgREST cap)."""
    eq_filters = eq_filters or {}
    offset = 0
    rows: list[dict[str, Any]] = []
    while True:
        q = sb.table(table).select(select).order(order_by)
        for k, v in eq_filters.items():
            q = q.eq(k, v)
        res = q.range(offset, offset + PAGE_SIZE - 1).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def fetch_bilateral_pages_limited(
    sb: Client,
    *,
    columns: str,
    eq_filters: dict[str, Any],
    max_rows: int,
    order_by: str = "id",
) -> list[dict[str, Any]]:
    """Paginate bilateral_trade with equality filters until max_rows or exhaustion."""
    offset = 0
    rows: list[dict[str, Any]] = []
    while len(rows) < max_rows:
        q = sb.table("bilateral_trade").select(columns).order(order_by)
        for k, v in eq_filters.items():
            q = q.eq(k, v)
        take = min(PAGE_SIZE, max_rows - len(rows))
        res = q.range(offset, offset + take - 1).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < take:
            break
        offset += take
    return rows


@st.cache_data(ttl=300, show_spinner="Running trade RPC…")
def rpc_trade_exporter_hs6_totals(
    exporter_iso3: str,
    year: int,
    hs_query_text: str,
    limit_n: int,
) -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.rpc(
            "rpc_trade_exporter_hs6_totals",
            {
                "exporter_iso3": str(exporter_iso3).strip().upper(),
                "p_data_year": int(year),
                "hs_query_text": hs_query_text,
                "limit_n": int(limit_n),
            },
        )
        .execute()
    )
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Running trade RPC…")
def rpc_trade_exporter_partner_totals(
    exporter_iso3: str,
    year: int,
    hs6_code: str,
    limit_n: int,
) -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.rpc(
            "rpc_trade_exporter_partner_totals",
            {
                "exporter_iso3": str(exporter_iso3).strip().upper(),
                "p_data_year": int(year),
                "p_hs6_code": str(hs6_code).strip(),
                "limit_n": int(limit_n),
            },
        )
        .execute()
    )
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Running trade RPC…")
def rpc_trade_importer_supplier_breakdown(
    importer_iso3: str,
    year: int,
    hs6_code: str,
    limit_n: int,
) -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.rpc(
            "rpc_trade_importer_supplier_breakdown",
            {
                "importer_iso3": str(importer_iso3).strip().upper(),
                "p_data_year": int(year),
                "p_hs6_code": str(hs6_code).strip(),
                "limit_n": int(limit_n),
            },
        )
        .execute()
    )
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Running trade RPC…")
def rpc_trade_importer_supplier_metrics(
    importer_iso3: str,
    year: int,
    hs6_code: str,
) -> dict[str, Any]:
    sb = supabase()
    res = (
        sb.rpc(
            "rpc_trade_importer_supplier_metrics",
            {
                "importer_iso3": str(importer_iso3).strip().upper(),
                "p_data_year": int(year),
                "p_hs6_code": str(hs6_code).strip(),
            },
        )
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else {}


@st.cache_data(ttl=3600, show_spinner=False)
def rpc_trade_distinct_exporters() -> tuple[list[str], bool]:
    """
    Prefer the DB RPC (complete list). If the RPC isn't deployed yet, fall back to a capped scan.
    Returns (exporters, used_fallback_scan).
    """
    sb = supabase()
    try:
        res = sb.rpc("rpc_trade_distinct_exporters", {}).execute()
        rows = res.data or []
        out: list[str] = []
        for r in rows:
            v = r.get("exporter_iso3")
            if v is not None and str(v).strip():
                out.append(str(v).strip().upper())
        return sorted(set(out)), False
    except Exception:
        # Fallback: capped scan (may miss exporters if table is huge).
        exporters = bilateral_distinct_column_values(
            "exporter",
            (),
            max_rows=BILATERAL_DISTINCT_SCAN_CAP,
        )
        normed: list[str] = []
        for v in exporters:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip().upper()
            if s:
                normed.append(s)
        return sorted(set(normed)), True


@st.cache_data(ttl=3600, show_spinner=False)
def rpc_trade_years_for_exporter(exporter_iso3: str) -> tuple[list[int], bool]:
    """
    Prefer the DB RPC (complete list). If the RPC isn't deployed yet, fall back to a capped scan.
    Returns (years, used_fallback_scan).
    """
    sb = supabase()
    exp = str(exporter_iso3).strip().upper()
    try:
        res = sb.rpc("rpc_trade_years_for_exporter", {"exporter_iso3": exp}).execute()
        rows = res.data or []
        out: list[int] = []
        for r in rows:
            y = r.get("data_year")
            if y is None:
                continue
            try:
                out.append(int(y))
            except (TypeError, ValueError):
                continue
        return sorted(set(out)), False
    except Exception:
        years = bilateral_distinct_column_values(
            "data_year",
            (("exporter", exp),),
            max_rows=BILATERAL_DISTINCT_SCAN_CAP,
        )
        out: list[int] = []
        for y in years:
            try:
                out.append(int(y))
            except (TypeError, ValueError):
                continue
        return sorted(set(out)), True


@st.cache_data(ttl=3600, show_spinner=False)
def rpc_trade_distinct_exporters_for_year(year: int) -> tuple[list[str], bool]:
    """
    Prefer the DB RPC (complete list). If the RPC isn't deployed yet, fall back to a capped scan.
    Returns (exporters, used_fallback_scan).
    """
    sb = supabase()
    try:
        res = sb.rpc("rpc_trade_distinct_exporters_for_year", {"p_data_year": int(year)}).execute()
        rows = res.data or []
        out: list[str] = []
        for r in rows:
            v = r.get("exporter_iso3")
            if v is not None and str(v).strip():
                out.append(str(v).strip().upper())
        return sorted(set(out)), False
    except Exception:
        exporters, truncated = bilateral_exporters_for_year(int(year), BILATERAL_DISTINCT_SCAN_CAP)
        # If we hit the cap, that implies potential incompleteness.
        return exporters, bool(truncated)

@st.cache_data(ttl=300, show_spinner="Running trade RPC…")
def rpc_trade_group_world_share_by_hs6(
    year: int,
    group_iso3: list[str],
    hs_query_text: str,
    limit_n: int,
) -> pd.DataFrame:
    # Same payload shape as other trade RPCs in this file — PostgREST keys must match Postgres
    # names (p_data_year, not data_year). Snapshot params_json uses data_year for storage only.
    sb = supabase()
    res = (
        sb.rpc(
            "rpc_trade_group_world_share_by_hs6",
            {
                "p_data_year": int(year),
                "group_iso3": [str(x).strip().upper() for x in group_iso3 if str(x).strip()],
                "hs_query_text": hs_query_text,
                "limit_n": int(limit_n),
            },
        )
        .execute()
    )
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Running trade RPC…")
def rpc_trade_group_member_breakdown_for_hs6(
    year: int,
    hs6_code: str,
    group_iso3: list[str],
) -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.rpc(
            "rpc_trade_group_member_breakdown_for_hs6",
            {
                "p_data_year": int(year),
                "p_hs6_code": str(hs6_code).strip(),
                "group_iso3": [str(x).strip().upper() for x in group_iso3 if str(x).strip()],
            },
        )
        .execute()
    )
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Running trade RPC…")
def rpc_trade_group_importer_exposure_for_hs6(
    year: int,
    hs6_code: str,
    group_iso3: list[str],
    limit_n: int,
) -> pd.DataFrame:
    # PostgREST keys must match Postgres: p_data_year, p_hs6_code (not data_year / hs6_code).
    sb = supabase()
    res = (
        sb.rpc(
            "rpc_trade_group_importer_exposure_for_hs6",
            {
                "p_data_year": int(year),
                "p_hs6_code": str(hs6_code).strip(),
                "group_iso3": [str(x).strip().upper() for x in group_iso3 if str(x).strip()],
                "limit_n": int(limit_n),
            },
        )
        .execute()
    )
    return pd.DataFrame(res.data or [])


def _snapshot_list(limit_n: int = 50) -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.table("trade_group_dependency_snapshots")
        .select("id,created_at,computed_at,data_year,group_iso3,params_hash,status,row_counts,params_json")
        .order("computed_at", desc=True)
        .limit(int(limit_n))
        .execute()
    )
    return pd.DataFrame(res.data or [])


def _snapshot_rows_export(snapshot_id: int, limit_n: int = 5000) -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.table("trade_group_dependency_rows")
        .select("*")
        .eq("snapshot_id", int(snapshot_id))
        .eq("view_type", "export_world_share")
        .order("group_share_pct", desc=True)
        .limit(int(limit_n))
        .execute()
    )
    return pd.DataFrame(res.data or [])


def _snapshot_rows_importer(snapshot_id: int, hs6_code: str, limit_n: int = 5000) -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.table("trade_group_dependency_rows")
        .select("*")
        .eq("snapshot_id", int(snapshot_id))
        .eq("view_type", "importer_exposure")
        .eq("hs6_code", str(hs6_code).strip())
        .order("exposure_pct", desc=True)
        .limit(int(limit_n))
        .execute()
    )
    return pd.DataFrame(res.data or [])


def _write_snapshot_and_rows(
    *,
    params_json: dict[str, Any],
    export_rows: pd.DataFrame,
    importer_rows: pd.DataFrame,
    force_recompute: bool,
) -> tuple[int, str]:
    return gdc.write_snapshot_and_rows(
        supabase(),
        params_json=params_json,
        export_rows=export_rows,
        importer_rows=importer_rows,
        force_recompute=force_recompute,
    )

@st.cache_data(ttl=300, show_spinner="Scanning bilateral_trade…")
def bilateral_distinct_column_values(
    column: str,
    eq_filters_key: tuple[tuple[str, Any], ...],
    max_rows: int,
) -> list[Any]:
    """Collect unique values for one column (paginated scan, capped)."""
    sb = supabase()
    eq_filters = dict(eq_filters_key)
    rows = fetch_bilateral_pages_limited(
        sb,
        columns=column,
        eq_filters=eq_filters,
        max_rows=max_rows,
        order_by=column,
    )
    out: set[Any] = set()
    for r in rows:
        v = r.get(column)
        if v is not None and (not isinstance(v, float) or not pd.isna(v)):
            out.add(v)
    return sorted(out)


@st.cache_data(ttl=300, show_spinner="Loading bilateral slice…")
def bilateral_hs6_codes_for_year(year: int, max_scan: int) -> tuple[list[str], bool]:
    sb = supabase()
    rows = fetch_bilateral_pages_limited(
        sb,
        columns="hs6_code",
        eq_filters={"data_year": year},
        max_rows=max_scan,
        order_by="hs6_code",
    )
    codes = sorted({str(r["hs6_code"]) for r in rows if r.get("hs6_code") is not None})
    truncated = len(rows) >= max_scan
    return codes, truncated


@st.cache_data(ttl=300, show_spinner="Loading bilateral slice…")
def bilateral_exporters_for_year(year: int, max_scan: int) -> tuple[list[str], bool]:
    sb = supabase()
    rows = fetch_bilateral_pages_limited(
        sb,
        columns="exporter",
        eq_filters={"data_year": year},
        max_rows=max_scan,
        order_by="exporter",
    )
    seen: set[str] = set()
    for r in rows:
        v = r.get("exporter")
        if v is not None and str(v).strip():
            seen.add(str(v).strip().upper())
    truncated = len(rows) >= max_scan
    return sorted(seen), truncated


@st.cache_data(ttl=300, show_spinner="Loading bilateral slice…")
def bilateral_country_codes_for_year(year: int, max_scan: int) -> tuple[list[str], bool]:
    sb = supabase()
    rows = fetch_bilateral_pages_limited(
        sb,
        columns="exporter,importer",
        eq_filters={"data_year": year},
        max_rows=max_scan,
        order_by="id",
    )
    seen: set[str] = set()
    for r in rows:
        for k in ("exporter", "importer"):
            v = r.get(k)
            if v is not None and str(v).strip():
                seen.add(str(v).strip())
    truncated = len(rows) >= max_scan
    return sorted(seen), truncated


@st.cache_data(ttl=300, show_spinner="Loading bilateral slice…")
def bilateral_rows_hs6_year(hs6: str, year: int, max_rows: int) -> pd.DataFrame:
    sb = supabase()
    rows = fetch_bilateral_pages_limited(
        sb,
        columns="id,exporter,importer,hs6_code,trade_value_usd,data_year",
        eq_filters={"hs6_code": str(hs6).strip(), "data_year": int(year)},
        max_rows=max_rows,
        order_by="id",
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner="Loading bilateral slice…")
def bilateral_rows_country_year(country: str, year: int, max_rows: int) -> pd.DataFrame:
    """All rows where country is exporter OR importer (union via two queries)."""
    sb = supabase()
    c = str(country).strip().upper()
    y = int(year)
    lim = max_rows // 2 + 1
    r1 = fetch_bilateral_pages_limited(
        sb,
        columns="id,exporter,importer,hs6_code,trade_value_usd,data_year",
        eq_filters={"exporter": c, "data_year": y},
        max_rows=lim,
        order_by="id",
    )
    r2 = fetch_bilateral_pages_limited(
        sb,
        columns="id,exporter,importer,hs6_code,trade_value_usd,data_year",
        eq_filters={"importer": c, "data_year": y},
        max_rows=lim,
        order_by="id",
    )
    return pd.DataFrame(r1 + r2)


@st.cache_data(ttl=300, show_spinner="Loading exporter flows…")
def bilateral_exporter_export_rows(exporter_iso3: str, year: int, max_rows: int) -> pd.DataFrame:
    sb = supabase()
    exp = str(exporter_iso3).strip().upper()
    rows = fetch_bilateral_pages_limited(
        sb,
        columns="exporter,importer,hs6_code,trade_value_usd,data_year",
        eq_filters={"exporter": exp, "data_year": int(year)},
        max_rows=max_rows,
        order_by="id",
    )
    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner="Loading import suppliers…")
def bilateral_importer_product_suppliers(importer: str, hs6: str, year: int, max_rows: int) -> pd.DataFrame:
    """Best fidelity: every exporter → importer for one HS6 and year."""
    sb = supabase()
    rows = fetch_bilateral_pages_limited(
        sb,
        columns="exporter,trade_value_usd",
        eq_filters={
            "importer": str(importer).strip().upper(),
            "hs6_code": str(hs6).strip(),
            "data_year": int(year),
        },
        max_rows=max_rows,
        order_by="exporter",
    )
    return pd.DataFrame(rows)


def supplier_concentration_metrics(values: pd.Series) -> dict[str, Any]:
    """HHI and concentration ratios on positive trade values (same units as BACI)."""
    v = pd.to_numeric(values, errors="coerce").fillna(0.0).astype(float)
    v = v[v > 0]
    total = float(v.sum())
    if total <= 0:
        return {
            "total_usd_k": 0.0,
            "n_suppliers": 0,
            "hhi": None,
            "cr1_pct": None,
            "cr3_pct": None,
        }
    s = (v / total).sort_values(ascending=False)
    hhi = float((s**2).sum())
    cr1 = float(s.iloc[0] * 100) if len(s) else 0.0
    cr3 = float(s.head(min(3, len(s))).sum() * 100)
    return {
        "total_usd_k": total,
        "n_suppliers": int(len(s)),
        "hhi": hhi,
        "cr1_pct": cr1,
        "cr3_pct": cr3,
    }


@st.cache_data(ttl=300, show_spinner="Loading commodity prices…")
def load_commodity_prices() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "commodity_prices", "*", order_by="id"))


@st.cache_data(ttl=300, show_spinner="Loading crop production…")
def load_crop_production() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "crop_production", "*", order_by="id"))


@st.cache_data(ttl=300, show_spinner="Loading pipeline runs…")
def load_pipeline_runs() -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.table("pipeline_runs")
        .select("*")
        .order("completed_at", desc=True)
        .limit(500)
        .execute()
    )
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Loading HS code lookup…")
def load_hs_lookup() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "hs_code_lookup", "*", order_by="hs6_code"))


@st.cache_data(ttl=300)
def hs6_description_map() -> dict[str, str]:
    """HS6 code → English description from `hs_code_lookup` (Comtrade reference)."""
    df = load_hs_lookup()
    if df.empty or "hs6_code" not in df.columns or "description" not in df.columns:
        return {}
    out: dict[str, str] = {}
    for _, r in df.iterrows():
        code = str(r["hs6_code"]).strip()
        d = r.get("description")
        if not code:
            continue
        out[code] = str(d).strip() if pd.notna(d) and str(d).strip() else ""
    return out


def hs6_full_description(code: object) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ""
    return hs6_description_map().get(str(code).strip(), "")


_HS6_SELECT_DESC_MAX = 88


def hs6_select_label(code: object, *, max_desc: int = _HS6_SELECT_DESC_MAX) -> str:
    """Selectbox: `270900 — Crude oils…` (truncated)."""
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ""
    c = str(code).strip()
    if not c:
        return ""
    d = hs6_description_map().get(c, "")
    if not d:
        return c
    if len(d) > max_desc:
        d = d[: max_desc - 1] + "…"
    return f"{c} — {d}"


def hs6_chart_label(code: object, *, max_total: int = 72) -> str:
    """Chart axis: compact `code — desc`."""
    lab = hs6_select_label(code, max_desc=max(24, max_total - len(str(code).strip()) - 3))
    if len(lab) > max_total:
        return lab[: max_total - 1] + "…"
    return lab


def filter_hs6_codes_by_search(codes: list[str], query: str) -> list[str]:
    """Keep codes where the query appears in the HS6 digits or in the lookup description."""
    q = query.strip().lower()
    if not q:
        return list(codes)
    m = hs6_description_map()
    out: list[str] = []
    for h in codes:
        hs = str(h).strip()
        if q in hs.lower():
            out.append(hs)
            continue
        if q in m.get(hs, "").lower():
            out.append(hs)
    return out


def _series_hs6_labels(s: pd.Series, *, max_total: int = 72) -> pd.Series:
    return s.map(lambda x: hs6_chart_label(x, max_total=max_total) if pd.notna(x) and str(x).strip() else "")


def _merge_hs6_description_column(df: pd.DataFrame, col: str = "hs6_code") -> pd.DataFrame:
    """Add `description` next to an hs6 column when missing."""
    if df.empty or col not in df.columns:
        return df
    if "description" in df.columns:
        return df
    m = hs6_description_map()
    out = df.copy()
    out["description"] = out[col].astype(str).map(lambda c: m.get(str(c).strip(), ""))
    idx = list(out.columns).index(col) + 1
    # move description immediately after hs6_code
    cols = [c for c in out.columns if c != "description"]
    cols = cols[:idx] + ["description"] + cols[idx:]
    return out[cols]


def _price_date_column(df: pd.DataFrame) -> pd.Series:
    m = df["data_month"]
    month = m.where(m.notna(), 1).astype(int).clip(1, 12)
    return pd.to_datetime(
        {"year": df["data_year"].astype(int), "month": month, "day": 1},
        errors="coerce",
    )


def _energy_date_column(df: pd.DataFrame) -> pd.Series:
    m = df["data_month"]
    month = m.where(m.notna(), 1).astype(int).clip(1, 12)
    return pd.to_datetime(
        {"year": df["data_year"].astype(int), "month": month, "day": 1},
        errors="coerce",
    )


@st.cache_data(ttl=300, show_spinner="Loading table catalog…")
def load_table_catalog_df() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "table_catalog", "*", order_by="sort_order"))


@st.cache_data(ttl=300, show_spinner="Loading energy flows…")
def load_energy_trade_flows() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "energy_trade_flows", "*", order_by="id"))


@st.cache_data(ttl=300, show_spinner="Loading fertilizer…")
def load_fertilizer_production() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "fertilizer_production", "*", order_by="id"))


@st.cache_data(ttl=300, show_spinner="Loading macro indicators…")
def load_country_macro_indicators() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "country_macro_indicators", "*", order_by="id"))


@st.cache_data(ttl=300, show_spinner="Loading food balance…")
def load_food_balance_sheets() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "food_balance_sheets", "*", order_by="id"))


@st.cache_data(ttl=300, show_spinner="Loading ProTEE…")
def load_cepii_protee_hs6() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "cepii_protee_hs6", "*", order_by="hs6_code"))


@st.cache_data(ttl=300, show_spinner="Loading GeoDep slice…")
def load_geodep_slice(
    country_upper: str,
    year_token: str,
    hs6_prefix: str,
    row_limit: int,
) -> pd.DataFrame:
    sb = supabase()
    q = sb.table("cepii_geodep_import_dependence").select("*")
    if country_upper:
        q = q.eq("country", country_upper)
    if year_token != "any":
        q = q.eq("data_year", int(year_token))
    if hs6_prefix:
        q = q.like("hs6_code", f"{hs6_prefix}%")
    q = q.order("import_value", desc=True).limit(row_limit)
    res = q.execute()
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Loading JODI slice…")
def load_jodi_slice(
    country_iso3: str,
    energy_product: str,
    flow_breakdown: str,
    unit_measure: str,
    year_from_token: str,
    year_to_token: str,
    row_limit: int,
) -> pd.DataFrame:
    sb = supabase()
    q = sb.table("jodi_energy_observations").select("*")
    cty = country_iso3.strip().upper()
    if cty and len(cty) == 3 and cty.isalpha():
        q = q.eq("country", cty)
    prod = energy_product.strip()
    if prod:
        q = q.eq("energy_product", prod)
    flow = flow_breakdown.strip()
    if flow:
        q = q.eq("flow_breakdown", flow)
    unit = unit_measure.strip()
    if unit:
        q = q.eq("unit_measure", unit)
    if year_from_token != "any":
        q = q.gte("data_year", int(year_from_token))
    if year_to_token != "any":
        q = q.lte("data_year", int(year_to_token))
    q = q.order("data_year", desc=False).order("data_month", desc=False).limit(row_limit)
    res = q.execute()
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Loading USGS MCS slice…")
def load_usgs_mcs_slice(
    country_iso3: str,
    commodity_substr: str,
    data_year_token: str,
    row_limit: int,
) -> pd.DataFrame:
    sb = supabase()
    q = sb.table("usgs_mineral_statistics").select("*")
    cty = country_iso3.strip().upper()
    if cty and len(cty) == 3 and cty.isalpha():
        q = q.eq("country_iso3", cty)
    cs = commodity_substr.strip()
    if cs:
        q = q.ilike("commodity", f"%{cs}%")
    if data_year_token != "any":
        q = q.eq("data_year", int(data_year_token))
    q = q.order("value_numeric", desc=True).limit(row_limit)
    res = q.execute()
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Loading USGS myb3 production…")
def load_usgs_myb3_production_slice(
    country_iso3: str,
    reference_year: int,
    commodity_substr: str,
    row_limit: int,
) -> pd.DataFrame:
    sb = supabase()
    q = (
        sb.table("usgs_myb3_production")
        .select("*")
        .eq("country_iso3", country_iso3.strip().upper())
        .eq("reference_year", int(reference_year))
    )
    cs = commodity_substr.strip()
    if cs:
        q = q.ilike("commodity_path", f"%{cs}%")
    q = q.order("stat_year", desc=False).order("commodity_path", desc=False).limit(row_limit)
    res = q.execute()
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Loading USGS facilities…")
def load_usgs_facilities_slice(
    country_iso3: str,
    reference_year: int,
    row_limit: int,
) -> pd.DataFrame:
    sb = supabase()
    q = (
        sb.table("usgs_country_mineral_facilities")
        .select("*")
        .eq("country_iso3", country_iso3.strip().upper())
        .eq("reference_year", int(reference_year))
        .limit(row_limit)
    )
    res = q.execute()
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Loading GEM sheet names…")
def load_gem_sheet_names(source_file: str) -> list[str]:
    fn = source_file.strip()
    if not fn:
        return []
    sb = supabase()
    res = (
        sb.table("gem_tracker_rows")
        .select("sheet_name")
        .eq("source_file", fn)
        .limit(5000)
        .execute()
    )
    rows = res.data or []
    names = {str(r["sheet_name"]) for r in rows if r.get("sheet_name") is not None}
    return sorted(names)


@st.cache_data(ttl=300, show_spinner="Loading GEM rows…")
def load_gem_rows_slice(source_file: str, sheet_name: str, row_limit: int) -> pd.DataFrame:
    sb = supabase()
    res = (
        sb.table("gem_tracker_rows")
        .select("id,source_file,sheet_name,excel_row_1based,payload,pulled_at")
        .eq("source_file", source_file.strip())
        .eq("sheet_name", sheet_name)
        .order("excel_row_1based", desc=False)
        .limit(row_limit)
        .execute()
    )
    return pd.DataFrame(res.data or [])


@st.cache_data(ttl=300, show_spinner="Loading country lookup…")
def load_country_lookup() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "country_lookup", "*", order_by="iso3"))


@st.cache_data(ttl=300)
def _lookup_iso3_to_name() -> dict[str, str]:
    """ISO3 → preferred display name from `country_lookup` when seeded."""
    df = load_country_lookup()
    if df.empty or "iso3" not in df.columns or "country_name" not in df.columns:
        return {}
    out: dict[str, str] = {}
    for _, r in df.iterrows():
        k = str(r["iso3"]).strip().upper()
        v = r.get("country_name")
        if k and pd.notna(v) and str(v).strip():
            out[k] = str(v).strip()
    return out


def country_display_name(iso3: object) -> str:
    """Resolve ISO 3166-1 alpha-3 to a common English name (DB override, then pycountry)."""
    if iso3 is None or (isinstance(iso3, float) and pd.isna(iso3)):
        return ""
    raw = str(iso3).strip()
    code = raw.upper()
    if len(code) != 3 or not code.isalpha():
        return raw
    m = _lookup_iso3_to_name()
    if code in m:
        return m[code]
    try:
        c = pycountry.countries.get(alpha_3=code)
        if c:
            return str(c.name)
    except (LookupError, KeyError, TypeError, AttributeError):
        pass
    return code


def country_select_label(iso3: object) -> str:
    """Selectbox / chart axis: 'United States (USA)' when resolvable, else code."""
    if iso3 is None or (isinstance(iso3, float) and pd.isna(iso3)):
        return ""
    code = str(iso3).strip().upper()
    name = country_display_name(iso3)
    if name == code or not name:
        return code
    return f"{name} ({code})"


def _series_country_labels(s: pd.Series) -> pd.Series:
    return s.map(lambda x: country_select_label(x) if pd.notna(x) and str(x).strip() else "")


def explore_table_catalog() -> None:
    st.subheader("Data dictionary")
    st.caption("What each table is for: title, grain, keys, maintainer.")
    df = load_table_catalog_df()
    if df.empty:
        st.info("No rows in `table_catalog`.")
        return
    disp = df.sort_values(["sort_order", "table_name"], na_position="last")
    cols = [
        c
        for c in (
            "table_name",
            "title",
            "summary",
            "row_grain",
            "key_columns",
            "populated_by",
        )
        if c in disp.columns
    ]
    st.dataframe(disp[cols], width="stretch", hide_index=True)


def explore_energy() -> None:
    st.subheader("Energy trade flows (EIA)")
    st.caption("`value_kbd` — thousand barrels per day where applicable (see pipeline docs).")
    df = load_energy_trade_flows()
    if df.empty:
        st.info("No rows in `energy_trade_flows`.")
        return
    reporters = sorted(df["reporter"].dropna().astype(str).unique())
    flows = sorted(df["flow_type"].dropna().astype(str).unique())
    products = sorted(df["product"].dropna().astype(str).unique())
    c1, c2, c3 = st.columns(3)
    with c1:
        r = st.selectbox("Reporter", reporters, format_func=country_select_label, key="em_eia_rep")
    with c2:
        ft = st.selectbox("Flow type", flows, key="em_eia_flow")
    with c3:
        pr = st.selectbox("Product", products, key="em_eia_prod")
    sub = df[
        (df["reporter"].astype(str) == r)
        & (df["flow_type"].astype(str) == ft)
        & (df["product"].astype(str) == pr)
    ].copy()
    if sub.empty:
        st.warning("No rows for this combination.")
        return
    sub = sub.assign(date=_energy_date_column(sub)).sort_values("date")
    line = sub.set_index("date")[["value_kbd"]].astype(float)
    st.line_chart(line, height=350)
    with st.expander("Raw rows"):
        show = sub.drop(columns=["date"], errors="ignore")
        st.dataframe(show, width="stretch", hide_index=True)


def explore_fertilizer() -> None:
    st.subheader("Fertilizer (FAOSTAT)")
    st.caption("Tonnes — pick a year, type, and metric to rank countries.")
    df = load_fertilizer_production()
    if df.empty:
        st.info("No rows in `fertilizer_production`.")
        return
    years = sorted(df["data_year"].dropna().astype(int).unique())
    ftypes = sorted(df["fertilizer_type"].dropna().astype(str).unique())
    metrics = sorted(df["metric"].dropna().astype(str).unique())
    c1, c2, c3 = st.columns(3)
    with c1:
        y = st.selectbox("Year", years, index=len(years) - 1, key="em_fert_y")
    with c2:
        ft = st.selectbox("Fertilizer type", ftypes, key="em_fert_t")
    with c3:
        met = st.selectbox("Metric", metrics, key="em_fert_m")
    sub = df[
        (df["data_year"] == y)
        & (df["fertilizer_type"].astype(str) == ft)
        & (df["metric"].astype(str) == met)
    ]
    if sub.empty:
        st.warning("No rows for this selection.")
        return
    top = (
        sub.groupby("country", as_index=False)["value_tonnes"]
        .sum()
        .nlargest(20, "value_tonnes")
    )
    st.caption("Top **20** countries by **value_tonnes** (largest at top)")
    top_chart = top.assign(_lbl=_series_country_labels(top["country"]))
    _st_altair_bar_h_by_value(top_chart, "value_tonnes", "_lbl", x_title="Tonnes")
    with st.expander("Raw rows (this slice)"):
        disp = sub.sort_values("country").copy()
        if "country" in disp.columns:
            i = list(disp.columns).index("country") + 1
            disp.insert(i, "country_name", _series_country_labels(disp["country"]))
        st.dataframe(disp, width="stretch", hide_index=True)


def explore_macro() -> None:
    st.subheader("Macro (World Bank WDI)")
    df = load_country_macro_indicators()
    if df.empty:
        st.info("No rows in `country_macro_indicators`.")
        return
    countries = sorted(df["country"].dropna().astype(str).unique())
    inds = sorted(df["indicator"].dropna().astype(str).unique())
    c1, c2 = st.columns(2)
    with c1:
        c = st.selectbox("Country", countries, format_func=country_select_label, key="em_mac_c")
    with c2:
        ind = st.selectbox("Indicator", inds, key="em_mac_i")
    sub = df[(df["country"].astype(str) == c) & (df["indicator"].astype(str) == ind)].copy()
    sub = sub.sort_values("data_year")
    if sub.empty:
        st.warning("No rows for this country and indicator.")
        return
    unit = sub["unit"].dropna().astype(str).iloc[-1] if sub["unit"].notna().any() else ""
    st.caption(f"**{country_display_name(c)}** (`{c}`) · unit: **{unit}**")
    line = sub.set_index("data_year")[["value"]].astype(float)
    st.line_chart(line, height=350)
    with st.expander("Raw rows"):
        st.dataframe(sub, width="stretch", hide_index=True)


def explore_fbs() -> None:
    st.subheader("Food balance sheets (FAOSTAT)")
    st.caption("Tonnes — V1 commodities in the pipeline.")
    df = load_food_balance_sheets()
    if df.empty:
        st.info("No rows in `food_balance_sheets`.")
        return
    mode = st.radio(
        "View",
        ["Top countries (one year)", "One country over time"],
        horizontal=True,
        key="em_fbs_mode",
    )
    commodities = sorted(df["commodity"].dropna().astype(str).unique())
    metrics = sorted(df["metric"].dropna().astype(str).unique())
    if mode == "Top countries (one year)":
        c1, c2, c3 = st.columns(3)
        with c1:
            com = st.selectbox("Commodity", commodities, key="em_fbs_c")
        with c2:
            met = st.selectbox("Metric", metrics, key="em_fbs_m")
        with c3:
            years = sorted(df["data_year"].dropna().astype(int).unique())
            y = st.selectbox("Year", years, index=len(years) - 1, key="em_fbs_y")
        sub = df[
            (df["commodity"].astype(str) == com)
            & (df["metric"].astype(str) == met)
            & (df["data_year"] == y)
        ]
        if sub.empty:
            st.warning("No rows for this selection.")
            return
        top = (
            sub.groupby("country", as_index=False)["value"]
            .sum()
            .nlargest(20, "value")
        )
        top_c = top.assign(_lbl=_series_country_labels(top["country"]))
        st.caption("Top **20** countries · bars sorted by value (largest at top).")
        _st_altair_bar_h_by_value(top_c, "value", "_lbl", x_title="Tonnes")
        with st.expander("Raw rows (this slice)"):
            d0 = sub.sort_values("country").copy()
            if "country" in d0.columns:
                j = list(d0.columns).index("country") + 1
                d0.insert(j, "country_name", _series_country_labels(d0["country"]))
            st.dataframe(d0, width="stretch", hide_index=True)
    else:
        countries = sorted(df["country"].dropna().astype(str).unique())
        c1, c2, c3 = st.columns(3)
        with c1:
            ctry = st.selectbox("Country", countries, format_func=country_select_label, key="em_fbs_c2")
        with c2:
            com = st.selectbox("Commodity", commodities, key="em_fbs_c3")
        with c3:
            met = st.selectbox("Metric", metrics, key="em_fbs_m2")
        sub = df[
            (df["country"].astype(str) == ctry)
            & (df["commodity"].astype(str) == com)
            & (df["metric"].astype(str) == met)
        ].sort_values("data_year")
        if sub.empty:
            st.warning("No rows for this selection.")
            return
        st.caption(f"**{country_display_name(ctry)}** (`{ctry}`)")
        line = sub.set_index("data_year")[["value"]].astype(float)
        st.line_chart(line, height=350)
        with st.expander("Raw rows"):
            st.dataframe(sub, width="stretch", hide_index=True)


def explore_protee() -> None:
    st.subheader("CEPII ProTEE (HS6 elasticities)")
    st.caption(
        "Import-demand elasticities — **not** trade flows. HS revision in data (often HS2007). "
        "Descriptions come from **`hs_code_lookup`** (same as BACI labels)."
    )
    df = load_cepii_protee_hs6()
    if df.empty:
        st.info("No rows in `cepii_protee_hs6`.")
        return
    c1, c2 = st.columns(2)
    with c1:
        prefix = st.text_input("HS6 prefix (optional, e.g. 2709)", value="", key="em_pt_prefix").strip()
    with c2:
        kw = st.text_input(
            "Keyword in description (optional, e.g. petroleum, urea)",
            value="",
            key="em_pt_kw",
        ).strip().lower()
    sub = df[df["hs6_code"].astype(str).str.startswith(prefix)] if prefix else df.copy()
    if kw:
        m = hs6_description_map()
        sub = sub[
            sub["hs6_code"]
            .astype(str)
            .map(lambda c: kw in str(c).lower() or kw in m.get(str(c).strip(), "").lower())
        ]
    if sub.empty:
        st.warning("No rows match this filter.")
        return
    work = sub.dropna(subset=["trade_elasticity"]).copy()
    if work.empty:
        st.warning("No numeric elasticities in this slice.")
        return
    work["_abs"] = work["trade_elasticity"].abs()
    top = work.nlargest(15, "_abs")
    st.markdown("**Largest |elasticity| (15 HS6 codes, signed)** — ranked by magnitude, largest at top")
    top_l = top.assign(
        _lbl=_series_hs6_labels(top["hs6_code"], max_total=76),
        _sort_abs=top["trade_elasticity"].abs(),
    )
    _st_altair_bar_h_by_value(
        top_l,
        "trade_elasticity",
        "_lbl",
        x_title="Elasticity",
        sort_by="_sort_abs",
        value_format=",.4f",
    )
    with st.expander("Filtered table (HS6 + description)"):
        disp = _merge_hs6_description_column(sub.sort_values("hs6_code"))
        st.dataframe(disp, width="stretch", hide_index=True)


def explore_geodep() -> None:
    st.subheader("CEPII GeoDep (import dependence)")
    st.caption(
        "Large table — filter by **ISO3** (e.g. SAU) and/or **year**. "
        "Names in charts use pycountry; seed `country_lookup` to override labels."
    )
    cty = st.text_input("Country (ISO3), optional", value="", key="em_geo_c").strip().upper()
    if len(cty) == 3 and cty.isalpha():
        st.caption(f"Showing codes for **{country_display_name(cty)}** (`{cty}`)")
    year_token = st.selectbox(
        "Year",
        ["any", "2019", "2020", "2021", "2022", "2023", "2024"],
        index=0,
        key="em_geo_y",
    )
    hs6p = st.text_input(
        "HS6 prefix (optional, digits only — narrows SQL query)",
        value="",
        key="em_geo_hs",
    ).strip()
    row_limit = st.slider(
        "Max rows fetched",
        min_value=100,
        max_value=5000,
        value=2000,
        step=100,
        key="em_geo_lim",
    )
    if not cty and year_token == "any":
        st.info("Enter a **country** and/or choose a **year** so the query stays bounded.")
        return
    df = load_geodep_slice(cty, year_token, hs6p, row_limit)
    if df.empty:
        st.warning("No rows for this filter (try another country/year or widen the limit).")
        return
    st.caption(f"Showing **{len(df)}** rows, ordered by **import_value** (desc).")
    top_hs = df.head(20).copy()
    if top_hs["import_value"].notna().any():
        st.markdown("**First 20 rows in this slice — import_value**")
        lbl = (
            top_hs["country"].map(country_select_label).astype(str)
            + " · "
            + _series_hs6_labels(top_hs["hs6_code"], max_total=68)
        )
        gviz = pd.DataFrame(
            {"_lbl": lbl.values, "import_value": top_hs["import_value"].astype(float).values}
        )
        _st_altair_bar_h_by_value(gviz, "import_value", "_lbl", x_title="import_value")
    with st.expander("Raw rows"):
        gd = df.copy()
        if "country" in gd.columns:
            gd.insert(
                list(gd.columns).index("country") + 1,
                "country_name",
                _series_country_labels(gd["country"]),
            )
        if "hs6_code" in gd.columns:
            gd = _merge_hs6_description_column(gd, "hs6_code")
        st.dataframe(gd, width="stretch", hide_index=True)


def explore_jodi() -> None:
    st.subheader("JODI (oil & gas observations)")
    st.caption(
        "Monthly reporter statistics — use **exact** `energy_product` and `flow_breakdown` strings from the loaded CSV "
        "(differs from EIA `energy_trade_flows`)."
    )
    cty = st.text_input("Country (ISO3), optional", value="", key="em_jodi_c").strip().upper()
    prod = st.text_input("Energy product (exact), optional", value="", key="em_jodi_p").strip()
    flow = st.text_input("Flow breakdown (exact), optional", value="", key="em_jodi_f").strip()
    unit = st.text_input("Unit measure (exact), optional", value="", key="em_jodi_u").strip()
    yopts = ["any"] + [str(y) for y in range(2005, 2031)]
    c1, c2 = st.columns(2)
    with c1:
        yf = st.selectbox("Year from", yopts, index=0, key="em_jodi_yf")
    with c2:
        yt = st.selectbox("Year to", yopts, index=0, key="em_jodi_yt")
    row_limit = st.slider(
        "Max rows fetched",
        min_value=500,
        max_value=5000,
        value=2000,
        step=100,
        key="em_jodi_lim",
    )
    has_cty = len(cty) == 3 and cty.isalpha()
    if not has_cty and not prod and not flow:
        st.info(
            "Set **country (ISO3)** and/or **energy product** and/or **flow breakdown** so the query stays bounded."
        )
        return
    df = load_jodi_slice(
        cty if has_cty else "",
        prod,
        flow,
        unit,
        yf,
        yt,
        row_limit,
    )
    if df.empty:
        st.warning("No rows for this filter.")
        return
    st.caption(f"Showing **{len(df)}** rows (limit {row_limit}).")
    work = df.copy()
    work["date"] = _energy_date_column(work)
    work = work.sort_values("date")
    num = work.dropna(subset=["obs_value"])
    if num.empty:
        st.warning("No numeric `obs_value` in this slice (see `obs_value_raw` in the table below).")
    else:
        key_cols = ["energy_product", "flow_breakdown", "unit_measure"]
        n_series = int(num[key_cols].drop_duplicates().shape[0])
        if n_series <= 8:
            num = num.copy()
            num["_series"] = (
                num["energy_product"].astype(str)
                + " · "
                + num["flow_breakdown"].astype(str)
                + " · "
                + num["unit_measure"].astype(str)
            )
            wide = num.pivot_table(
                index="date", columns="_series", values="obs_value", aggfunc="last"
            )
            wide = wide.sort_index()
            st.line_chart(wide.astype(float), height=350)
        else:
            st.caption(
                "Many distinct product/flow/unit combinations — narrow filters for a single time series, or use the table."
            )
    with st.expander("Raw rows"):
        st.dataframe(
            df.sort_values(["data_year", "data_month"]),
            width="stretch",
            hide_index=True,
        )


def explore_usgs() -> None:
    st.subheader("USGS minerals")
    mode = st.radio(
        "Dataset",
        [
            "MCS (commodity statistics)",
            "Yearbook Table 1 (production)",
            "Yearbook Table 2 (facilities)",
        ],
        horizontal=True,
        key="em_usgs_mode",
    )
    row_limit = st.slider(
        "Max rows",
        min_value=100,
        max_value=5000,
        value=2000,
        step=100,
        key="em_usgs_lim",
    )

    if mode.startswith("MCS"):
        st.caption("`usgs_mineral_statistics` — long-form MCS CSV rows.")
        cty = st.text_input("Country ISO3 (optional)", value="", key="em_usgs_mcs_c").strip().upper()
        com = st.text_input("Commodity contains (optional)", value="", key="em_usgs_mcs_com").strip()
        yopts = ["any"] + [str(y) for y in range(1990, 2031)]
        dy = st.selectbox("Data year", yopts, index=0, key="em_usgs_mcs_y")
        has_cty = len(cty) == 3 and cty.isalpha()
        if not has_cty and not com:
            st.info("Enter **ISO3** and/or a **commodity** substring so the query stays bounded.")
            return
        df = load_usgs_mcs_slice(
            cty if has_cty else "",
            com,
            dy,
            row_limit,
        )
        if df.empty:
            st.warning("No rows for this filter.")
            return
        st.caption(f"**{len(df)}** rows (limit {row_limit}), ordered by `value_numeric` desc.")
        top = df.dropna(subset=["value_numeric"]).head(20).copy()
        if not top.empty:
            top["_lbl"] = (
                top["commodity"].astype(str).str.slice(0, 56)
                + " · "
                + top["country_name"].astype(str).str.slice(0, 24)
            )
            _st_altair_bar_h_by_value(top, "value_numeric", "_lbl", x_title="value_numeric")
        with st.expander("Raw rows"):
            st.dataframe(df, width="stretch", hide_index=True)
        return

    if mode.startswith("Yearbook Table 1"):
        st.caption("`usgs_myb3_production` — melted yearbook Table 1.")
        cty = st.text_input("Country ISO3 (required)", value="", key="em_usgs_myb3_c").strip().upper()
        ref_y = st.number_input(
            "Reference year (from filename)",
            min_value=1990,
            max_value=2030,
            value=2019,
            key="em_usgs_myb3_ref",
        )
        com = st.text_input("Commodity path contains (optional)", value="", key="em_usgs_myb3_com").strip()
        if len(cty) != 3 or not cty.isalpha():
            st.info("Enter a valid **ISO3** country code.")
            return
        df = load_usgs_myb3_production_slice(cty, int(ref_y), com, row_limit)
        if df.empty:
            st.warning("No rows for this filter.")
            return
        st.caption(f"**{len(df)}** rows for **{country_select_label(cty)}** · ref year **{ref_y}**.")
        num = df.dropna(subset=["value_numeric"])
        if not num.empty:
            by_y = num.groupby("stat_year", as_index=False)["value_numeric"].sum().sort_values("stat_year")
            st.line_chart(by_y.set_index("stat_year")[["value_numeric"]].astype(float), height=320)
        top_c = (
            df.dropna(subset=["value_numeric"])
            .groupby("commodity_path", as_index=False)["value_numeric"]
            .sum()
            .nlargest(15, "value_numeric")
        )
        if not top_c.empty:
            st.markdown("**Top commodity paths by summed `value_numeric` (this slice)**")
            top_c = top_c.assign(_lbl=top_c["commodity_path"].astype(str).str.slice(0, 72))
            _st_altair_bar_h_by_value(top_c, "value_numeric", "_lbl", x_title="Sum value_numeric")
        with st.expander("Raw rows"):
            st.dataframe(df.sort_values(["stat_year", "commodity_path"]), width="stretch", hide_index=True)
        return

    st.caption("`usgs_country_mineral_facilities` — yearbook Table 2 merged blocks.")
    cty = st.text_input("Country ISO3 (required)", value="", key="em_usgs_fac_c").strip().upper()
    ref_y = st.number_input(
        "Reference year",
        min_value=1990,
        max_value=2030,
        value=2019,
        key="em_usgs_fac_ref",
    )
    if len(cty) != 3 or not cty.isalpha():
        st.info("Enter a valid **ISO3** country code.")
        return
    df = load_usgs_facilities_slice(cty, int(ref_y), row_limit)
    if df.empty:
        st.warning("No rows for this filter.")
        return
    st.caption(f"**{len(df)}** rows for **{country_select_label(cty)}** · ref year **{ref_y}**.")
    cap = df.dropna(subset=["capacity_numeric"])
    if not cap.empty:
        top = cap.nlargest(20, "capacity_numeric").copy()
        top["_lbl"] = (
            top["commodity_leaf_resolved"].astype(str).str.slice(0, 40)
            + " · "
            + top["facility_path"].astype(str).str.slice(0, 36)
        )
        _st_altair_bar_h_by_value(top, "capacity_numeric", "_lbl", x_title="capacity_numeric")
    with st.expander("Raw rows"):
        st.dataframe(df, width="stretch", hide_index=True)


def explore_gem() -> None:
    st.subheader("GEM tracker rows")
    st.caption(
        "Exact **`source_file`** as stored (workbook filename). **`payload`** is flattened where keys are shallow."
    )
    fn = st.text_input(
        "Source file (exact)",
        value="",
        placeholder="Global-Integrated-Power-March-2026-II.xlsx",
        key="em_gem_fn",
    ).strip()
    if not fn:
        st.info("Enter the workbook **filename** as in `gem_tracker_rows.source_file`.")
        return
    sheets = load_gem_sheet_names(fn)
    if not sheets:
        st.warning("No rows for this filename — check spelling or run `load_gem_xlsx.py`.")
        return
    sheet = st.selectbox("Sheet", sheets, key="em_gem_sheet")
    row_limit = st.slider(
        "Max rows",
        min_value=100,
        max_value=5000,
        value=500,
        step=100,
        key="em_gem_lim",
    )
    df = load_gem_rows_slice(fn, sheet, row_limit)
    if df.empty:
        st.warning("No rows for this sheet.")
        return
    st.caption(f"**{len(df)}** rows · `{fn}` · `{sheet}`")
    meta = df[["id", "source_file", "sheet_name", "excel_row_1based", "pulled_at"]].copy()
    payloads = df["payload"].tolist()
    try:
        norm = pd.json_normalize(payloads)
    except (TypeError, ValueError):
        norm = pd.DataFrame()
    if norm.empty and payloads:
        norm = pd.DataFrame(
            {
                "payload_json": [
                    json.dumps(p, default=str) if isinstance(p, dict) else str(p) for p in payloads
                ]
            }
        )
    disp = pd.concat([meta.reset_index(drop=True), norm.reset_index(drop=True)], axis=1)
    st.dataframe(disp, width="stretch", hide_index=True)
    with st.expander("First row `payload` (JSON)"):
        p0 = payloads[0] if payloads else None
        if isinstance(p0, dict):
            st.json(p0)
        elif p0 is not None:
            st.code(str(p0))


def explore_hs_lookup_tab() -> None:
    st.subheader("HS6 lookup (Comtrade)")
    st.caption("English HS6 labels — filter is **client-side** on the loaded table (~7k rows).")
    q = st.text_input("Filter (matches any column text)", value="", key="em_hs6_q").strip().lower()
    df = load_hs_lookup()
    if df.empty:
        st.info("No rows in `hs_code_lookup` — run `pull_comtrade_hs_lookup.py`.")
        return
    if q:
        blob = df.astype(str).agg(" ".join, axis=1).str.lower()
        sub = df.loc[blob.str.contains(q, regex=False, na=False)]
    else:
        sub = df
    st.caption(f"Showing **{len(sub)}** / {len(df)} rows")
    st.dataframe(sub.sort_values("hs6_code"), width="stretch", hide_index=True)


def explore_country_lookup() -> None:
    st.subheader("Country lookup (reference)")
    st.caption("Manual / future seed — ISO3 names and Gulf flags.")
    df = load_country_lookup()
    if df.empty:
        st.info("No rows in `country_lookup` yet.")
        return
    st.dataframe(df, width="stretch", hide_index=True)


def tab_explore_more() -> None:
    st.subheader("Explore more")
    st.caption("Extra datasets with the same selector + chart style as the other tabs.")
    a, b, c, d, e, f, g, h, i, j, k, l = st.tabs(
        [
            "Data dictionary",
            "Energy (EIA)",
            "Fertilizer",
            "Macro (WDI)",
            "Food balance",
            "ProTEE",
            "GeoDep",
            "JODI",
            "USGS",
            "GEM",
            "HS6",
            "Countries",
        ]
    )
    with a:
        explore_table_catalog()
    with b:
        explore_energy()
    with c:
        explore_fertilizer()
    with d:
        explore_macro()
    with e:
        explore_fbs()
    with f:
        explore_protee()
    with g:
        explore_geodep()
    with h:
        explore_jodi()
    with i:
        explore_usgs()
    with j:
        explore_gem()
    with k:
        explore_hs_lookup_tab()
    with l:
        explore_country_lookup()


def tab_prices() -> None:
    st.subheader("Prices over time")
    st.caption("World Bank Pink Sheet monthly series (units differ by commodity).")
    df = load_commodity_prices()
    if df.empty:
        st.info("No rows in `commodity_prices`.")
        return
    commodities = sorted(df["commodity"].dropna().unique().tolist())
    c1, c2 = st.columns(2)
    with c1:
        a = st.selectbox("Commodity", commodities, key="price_a")
    with c2:
        b = st.selectbox(
            "Overlay second commodity (optional)",
            ["— none —"] + [x for x in commodities if x != a],
            key="price_b",
        )
    sel = [a] if b == "— none —" else [a, b]
    sub = df[df["commodity"].isin(sel)].copy()
    sub["date"] = _price_date_column(sub)
    sub = sub.sort_values("date")
    wide = sub.pivot_table(index="date", columns="commodity", values="price", aggfunc="last")
    wide = wide.sort_index()
    units = sub.groupby("commodity")["unit"].agg(lambda s: s.dropna().iloc[-1] if len(s.dropna()) else "")
    st.caption(" · ".join(f"{c}: {units.get(c, '')}" for c in wide.columns))
    st.line_chart(wide, height=400)


def tab_who_trades() -> None:
    st.subheader("Who trades what")
    st.caption(
        "BACI trade values are **USD thousands** (same convention as CEPII / README). "
        "Loads **only the selected year × HS6** from Supabase (no full-table scan). "
        "HS6 labels use **`hs_code_lookup`** — run `pull_comtrade_hs_lookup.py` if descriptions are missing."
    )
    y_probe = bilateral_distinct_column_values(
        "data_year",
        (),
        max_rows=min(5000, BILATERAL_DISTINCT_SCAN_CAP),
    )
    if not y_probe:
        st.info("No rows in `bilateral_trade`.")
        return
    years_int = [int(y) for y in y_probe if str(y).isdigit() or isinstance(y, int)]
    years = sorted(set(years_int))
    if not years:
        st.info("No `data_year` values in `bilateral_trade`.")
        return
    year = st.selectbox("Year", years, index=len(years) - 1, key="trade_year")
    hs_list, hs_trunc = bilateral_hs6_codes_for_year(year, BILATERAL_DISTINCT_SCAN_CAP)
    if hs_trunc:
        st.warning(
            f"HS6 list built from the first **{BILATERAL_DISTINCT_SCAN_CAP:,}** rows for **{year}** — "
            "some codes may be missing. Narrow data or raise the cap in code if needed."
        )
    if not hs_list:
        st.info(f"No bilateral rows for year **{year}**.")
        return
    hs_search = st.text_input(
        "Search HS6 (digits and/or words from the product name, e.g. `2709`, `crude`, `urea`)",
        value="",
        key="trade_hs_q",
        help="Narrows the dropdown. Matches the six-digit code substring or any text in the Comtrade description.",
    )
    hs_filtered = filter_hs6_codes_by_search(hs_list, hs_search)
    if not hs_filtered:
        st.warning("No HS6 codes in BACI match this search. Clear the search or try other keywords.")
        return
    view_mode = st.radio(
        "View",
        ["Single HS6 product", "Aggregate all search matches"],
        horizontal=True,
        key="trade_view_mode",
        help="Single: one six-digit line. Aggregate: sum trade across every HS6 that matches your search (e.g. “cereal” clumps all matching products).",
    )
    c1, c2 = st.columns(2)
    with c1:
        if view_mode == "Single HS6 product":
            hs = st.selectbox(
                "HS6 product",
                hs_filtered,
                format_func=hs6_select_label,
                key="trade_hs",
            )
        else:
            hs = None
            st.metric("HS6 codes in aggregate", len(hs_filtered))
    with c2:
        st.caption(f"**Year {year}**")
    _AGG_HS6_CAP = 60
    if view_mode == "Single HS6 product":
        full_hs = hs6_full_description(hs)
        if full_hs:
            st.caption(f"**{hs}** — {full_hs}")
        else:
            st.caption(f"`{hs}` — *(no description in lookup)*")
        slice_df = bilateral_rows_hs6_year(str(hs), int(year), max_rows=2_000_000)
    else:
        q = hs_search.strip()
        if not q and len(hs_filtered) == len(hs_list):
            st.warning(
                "Aggregate mode with an **empty** search would sum **every** HS6 in the scanned list — add a search "
                "term (e.g. `cereal`, `1001`) or switch to **Single HS6 product**."
            )
            return
        filt_note = repr(q) if q else "codes matched by filter"
        if len(hs_filtered) > _AGG_HS6_CAP:
            st.error(
                f"Aggregate mode is limited to **{_AGG_HS6_CAP}** HS6 codes at once; "
                f"your filter matches **{len(hs_filtered)}**. Narrow the search."
            )
            return
        st.caption(
            f"**Aggregate** — summing **{len(hs_filtered)}** HS6 line(s) for **{year}** (search: {filt_note})."
        )
        if len(hs_filtered) > 80:
            with st.expander("HS6 codes included (first 80)"):
                st.write(", ".join(sorted(hs_filtered)[:80]) + (" …" if len(hs_filtered) > 80 else ""))
        parts: list[pd.DataFrame] = []
        for code in hs_filtered:
            parts.append(bilateral_rows_hs6_year(str(code), int(year), max_rows=2_000_000))
        slice_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if slice_df.empty:
        st.warning("No rows for this selection and year.")
        return
    tv = slice_df["trade_value_usd"].fillna(0)
    slice_df = slice_df.assign(_tv=tv)
    exp = (
        slice_df.groupby("exporter", as_index=False)["_tv"]
        .sum()
        .nlargest(10, "_tv")
        .rename(columns={"exporter": "country", "_tv": "trade_value_usd_thousands"})
        .sort_values("trade_value_usd_thousands", ascending=False)
    )
    imp = (
        slice_df.groupby("importer", as_index=False)["_tv"]
        .sum()
        .nlargest(10, "_tv")
        .rename(columns={"importer": "country", "_tv": "trade_value_usd_thousands"})
        .sort_values("trade_value_usd_thousands", ascending=False)
    )
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Top 10 exporters** (by trade value, USD thousands — largest first)")
        if exp.empty:
            st.info("No exporter data.")
        else:
            ex2 = exp.assign(_lbl=_series_country_labels(exp["country"]))
            _st_altair_bar_h_by_value(
                ex2, "trade_value_usd_thousands", "_lbl", x_title="USD thousands"
            )
    with col2:
        st.markdown("**Top 10 importers** (by trade value, USD thousands — largest first)")
        if imp.empty:
            st.info("No importer data.")
        else:
            im2 = imp.assign(_lbl=_series_country_labels(imp["country"]))
            _st_altair_bar_h_by_value(
                im2, "trade_value_usd_thousands", "_lbl", x_title="USD thousands"
            )


def tab_country_profile() -> None:
    st.subheader("Country profile")
    st.caption(
        "Per-product import and export totals (BACI, USD thousands) for one year. "
        "Loads **only that country × year** from Supabase (capped scan — see warning if truncated)."
    )
    lookup = load_hs_lookup()
    desc_map: dict[str, str] = {}
    if not lookup.empty and "hs6_code" in lookup.columns:
        for _, r in lookup.iterrows():
            code = str(r.get("hs6_code", "")).strip()
            d = r.get("description")
            if code and pd.notna(d):
                desc_map[code] = str(d)
    y_probe = bilateral_distinct_column_values(
        "data_year",
        (),
        max_rows=min(5000, BILATERAL_DISTINCT_SCAN_CAP),
    )
    if not y_probe:
        st.info("No rows in `bilateral_trade`.")
        return
    years_int = [int(y) for y in y_probe if str(y).isdigit() or isinstance(y, int)]
    years = sorted(set(years_int))
    if not years:
        st.info("No `data_year` values in `bilateral_trade`.")
        return
    year = st.selectbox("Year", years, index=len(years) - 1, key="prof_year")
    countries, c_trunc = bilateral_country_codes_for_year(year, BILATERAL_DISTINCT_SCAN_CAP)
    if c_trunc:
        st.warning(
            f"Country list built from the first **{BILATERAL_DISTINCT_SCAN_CAP:,}** rows for **{year}** — "
            "some countries may be missing."
        )
    if not countries:
        st.info(f"No bilateral rows for year **{year}**.")
        return
    c1, c2 = st.columns(2)
    with c1:
        country = st.selectbox("Country", countries, format_func=country_select_label, key="prof_country")
    with c2:
        st.caption(f"**Year {year}**")
    st.caption(f"**{country_display_name(country)}** (`{country}`) · **{year}**")
    df = bilateral_rows_country_year(country, year, max_rows=BILATERAL_DISTINCT_SCAN_CAP * 2)
    if len(df) >= BILATERAL_DISTINCT_SCAN_CAP * 2 - 10:
        st.warning(
            "Row cap reached for this country × year — import/export totals may be **incomplete**. "
            "Use the Exporter & partners tab or SQL for heavy slices."
        )
    imp = (
        df[df["importer"].astype(str) == country]
        .groupby("hs6_code", as_index=False)["trade_value_usd"]
        .sum()
        .rename(columns={"trade_value_usd": "imports_usd_k"})
    )
    exp = (
        df[df["exporter"].astype(str) == country]
        .groupby("hs6_code", as_index=False)["trade_value_usd"]
        .sum()
        .rename(columns={"trade_value_usd": "exports_usd_k"})
    )
    merged = pd.merge(imp, exp, on="hs6_code", how="outer").fillna(0)
    merged = merged.sort_values("hs6_code")
    merged["description"] = merged["hs6_code"].astype(str).map(
        lambda h: desc_map.get(str(h).strip(), "")
    )
    hs_filt = st.text_input(
        "Filter products (HS6 digits or words in the description, e.g. `1001`, `wheat`, `fertilizer`)",
        value="",
        key="prof_hs_q",
    ).strip().lower()
    if hs_filt:
        dcol = merged["description"].fillna("").astype(str).str.lower()
        ccol = merged["hs6_code"].astype(str).str.lower()
        merged = merged[dcol.str.contains(hs_filt, regex=False) | ccol.str.contains(hs_filt, regex=False)]
    merged["product"] = merged.apply(
        lambda r: (
            f"{r['hs6_code']} — {r['description']}"
            if str(r.get("description", "")).strip()
            else str(r["hs6_code"])
        ),
        axis=1,
    )
    show = merged[["product", "hs6_code", "description", "imports_usd_k", "exports_usd_k"]]
    st.caption(f"**{len(show)}** product row(s) · sortable columns; use your browser search (Ctrl/Cmd+F) inside the table if needed.")
    st.dataframe(show, width="stretch", hide_index=True)


def tab_crop_rank() -> None:
    st.subheader("Crop production by country")
    df = load_crop_production()
    if df.empty:
        st.info("No rows in `crop_production`.")
        return
    crops = sorted(df["crop"].dropna().astype(str).unique().tolist())
    metrics = sorted(df["metric"].dropna().astype(str).unique().tolist())
    c1, c2, c3 = st.columns(3)
    with c1:
        crop = st.selectbox("Crop", crops, key="crop_pick")
    with c2:
        metric = st.selectbox("Metric", metrics, key="crop_metric")
    years_avail = sorted(
        df[(df["crop"] == crop) & (df["metric"] == metric)]["data_year"]
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    with c3:
        if not years_avail:
            st.caption("No years for this crop/metric.")
            year = None
        else:
            year = st.selectbox(
                "Year",
                years_avail,
                index=len(years_avail) - 1,
                key="crop_year",
            )
    if year is None:
        st.warning("No data for this crop/metric combination.")
        return
    sub = df[
        (df["crop"] == crop) & (df["metric"] == metric) & (df["data_year"] == year)
    ].copy()
    if sub.empty:
        st.warning("No rows for this selection.")
        return
    unit = sub["unit"].dropna().astype(str).iloc[-1] if sub["unit"].notna().any() else ""
    top = sub.groupby("country", as_index=False)["value"].sum().nlargest(20, "value")
    st.caption(f"Year **{year}** · unit: **{unit}** · top **20** by value (largest at top)")
    top_cr = top.assign(_lbl=_series_country_labels(top["country"]))
    _st_altair_bar_h_by_value(top_cr, "value", "_lbl", x_title=unit or "Value")


def tab_pipeline() -> None:
    st.subheader("Pipeline status")
    df = load_pipeline_runs()
    if df.empty:
        st.info("No rows in `pipeline_runs`.")
        return
    df = df.copy()
    df["completed_at"] = pd.to_datetime(df["completed_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["script_name", "completed_at"])
    if df.empty:
        st.warning("No runs with a valid `completed_at`.")
        return
    df = df.sort_values("completed_at", ascending=False)
    latest = df.loc[df.groupby("script_name")["completed_at"].idxmax()]
    show_cols = [
        "script_name",
        "completed_at",
        "rows_written",
        "status",
        "error_message",
        "source_label",
    ]
    show_cols = [c for c in show_cols if c in latest.columns]
    disp = latest[show_cols].copy()
    if "error_message" in disp.columns:
        disp["error_message"] = disp["error_message"].astype(str).str.slice(0, 200)
    st.markdown("**Latest run per script**")
    st.dataframe(disp, width="stretch", hide_index=True)
    with st.expander("Recent runs (debug)"):
        tail = df.head(50)
        tcols = [c for c in show_cols if c in tail.columns]
        st.dataframe(tail[tcols], width="stretch", hide_index=True)


def _exporter_drilldown_country_options(year: int) -> tuple[list[str], bool]:
    """Gulf ISO3 list first (always), then other exporters seen for that year (scan capped)."""
    scanned, truncated = bilateral_exporters_for_year(year, BILATERAL_DISTINCT_SCAN_CAP)
    gulf = list(GULF_EXPORTER_ISO3_ORDER)
    gulf_set = set(gulf)
    tail = sorted(e for e in scanned if e not in gulf_set)
    return gulf + tail, truncated


def tab_exporter_partners() -> None:
    st.subheader("Exporter & partner imports (best fidelity)")
    st.caption(
        "**Step 1–2:** Choose **exporter** and **year**, then browse its exports by HS6 and partner. **Step 3:** "
        "For the chosen partner and product, **full supplier concentration** from BACI "
        "(every exporter → that importer × HS6 × year). Values are **USD thousands**. "
        "Gulf countries are listed first; other exporters appear after if present in your BACI load for that year. "
        "For a full HS6 portfolio for one country, run e.g. "
        "`load_baci.py --all --exporter-full-hs ARE` (use any ISO3). For partner supplier tables, add "
        "`--importer-full-hs IND` per partner."
    )
    # Do not auto-query Supabase when the user lands on this tab.
    refresh_lists = st.button("Refresh exporter/year lists", key="xpd_refresh_lists")

    if refresh_lists or "xpd_exporter_options" not in st.session_state:
        all_exporters, exporters_fallback = rpc_trade_distinct_exporters()
        if not all_exporters:
            st.info("No exporters found in `bilateral_trade`.")
            return
        exporters_set = set(all_exporters)
        pinned = [g for g in GULF_EXPORTER_ISO3_ORDER if g in exporters_set]
        tail = sorted(e for e in all_exporters if e not in set(pinned))
        st.session_state["xpd_exporter_options"] = pinned + tail
        st.session_state["xpd_exporters_fallback"] = bool(exporters_fallback)
        # Reset year cache when refreshing exporters.
        st.session_state["xpd_years_by_exporter"] = {}
        st.session_state["xpd_years_fallback_by_exporter"] = {}

    exporter_options = list(st.session_state.get("xpd_exporter_options") or [])
    if not exporter_options:
        st.info("No exporters found in `bilateral_trade`.")
        return
    if st.session_state.get("xpd_exporters_fallback"):
        st.warning(
            f"Exporter list built from the first **{BILATERAL_DISTINCT_SCAN_CAP:,}** rows — "
            "some exporters may be missing. Apply `schema/rpc_trade_dashboards.sql` to enable the complete list."
        )

    c1, c2 = st.columns(2)
    with c1:
        default_iso = "ARE" if "ARE" in exporter_options else exporter_options[0]
        default_idx = exporter_options.index(default_iso) if default_iso in exporter_options else 0
        exporter_iso = st.selectbox(
            "Exporter country",
            exporter_options,
            index=default_idx,
            format_func=country_select_label,
            key="xpd_exporter",
        )

    years_by_exporter: dict[str, list[int]] = st.session_state.get("xpd_years_by_exporter") or {}
    years_fallback_by_exporter: dict[str, bool] = (
        st.session_state.get("xpd_years_fallback_by_exporter") or {}
    )
    if refresh_lists or exporter_iso not in years_by_exporter:
        years_int, years_fallback = rpc_trade_years_for_exporter(exporter_iso)
        years_by_exporter[exporter_iso] = years_int
        years_fallback_by_exporter[exporter_iso] = bool(years_fallback)
        st.session_state["xpd_years_by_exporter"] = years_by_exporter
        st.session_state["xpd_years_fallback_by_exporter"] = years_fallback_by_exporter
    years_int = years_by_exporter.get(exporter_iso, [])
    years_fallback = bool(years_fallback_by_exporter.get(exporter_iso, False))
    if not years_int:
        st.info(f"No years available for exporter **{exporter_iso}**.")
        return
    if years_fallback:
        st.warning(
            f"Year list built from the first **{BILATERAL_DISTINCT_SCAN_CAP:,}** rows for exporter **{exporter_iso}** — "
            "some years may be missing. Apply `schema/rpc_trade_dashboards.sql` to enable the complete list."
        )
    with c2:
        year = st.selectbox("Year", years_int, index=len(years_int) - 1, key="xpd_year")

    hs_search = st.text_input(
        "Filter HS6 / description",
        value="",
        key="xpd_hs_q",
        help="Digits or words from the Comtrade description. No query is sent until you click **Load data**.",
    )
    top_n = st.slider(
        "Show top HS6 products (by exporter value)",
        15,
        200,
        60,
        key="xpd_topn",
    )
    # Reset loaded flags when upstream filters change.
    snap = (exporter_iso, int(year), hs_search, int(top_n))
    if st.session_state.get("xpd_loaded_snapshot") != snap:
        st.session_state["xpd_loaded"] = False
        st.session_state["xpd_partners_loaded"] = False
        st.session_state["xpd_suppliers_loaded"] = False
        st.session_state["xpd_loaded_snapshot"] = snap

    if st.button("Load data", key="xpd_load_btn", type="primary"):
        st.session_state["xpd_loaded"] = True
        st.session_state["xpd_partners_loaded"] = False
        st.session_state["xpd_suppliers_loaded"] = False

    if not st.session_state.get("xpd_loaded"):
        st.info("Set filters above, then click **Load data**. No Supabase queries are sent until then.")
        return

    by_hs = rpc_trade_exporter_hs6_totals(exporter_iso, year, hs_search, top_n)
    if by_hs.empty or "hs6_code" not in by_hs.columns:
        st.warning("No HS6 lines match this filter.")
        return

    st.caption(
        f"**{country_display_name(exporter_iso)}** (`{exporter_iso}`) · "
        f"**{len(by_hs)}** HS6 row(s) shown · **{year}**"
    )
    st.dataframe(
        _merge_hs6_description_column(by_hs.rename(columns={"hs6_code": "hs6_code"})),
        width="stretch",
        hide_index=True,
    )
    hs_sel = st.selectbox(
        "HS6 product (drill-down)",
        by_hs["hs6_code"].astype(str).tolist(),
        format_func=hs6_select_label,
        key="xpd_hs",
    )

    if st.button("Load partner breakdown", key="xpd_load_partners_btn"):
        st.session_state["xpd_partners_loaded"] = True
        st.session_state["xpd_suppliers_loaded"] = False

    if not st.session_state.get("xpd_partners_loaded"):
        st.info("Select an HS6, then click **Load partner breakdown**.")
        return

    part_df = rpc_trade_exporter_partner_totals(exporter_iso, year, str(hs_sel), 25)
    st.markdown(f"**Top destinations for this HS6 ({country_display_name(exporter_iso)} exports)**")
    if part_df.empty:
        st.warning("No partner rows.")
        return
    part_df = part_df.rename(columns={"partner_iso3": "partner"})
    st.dataframe(part_df, width="stretch", hide_index=True)
    partner = st.selectbox(
        "Partner country (importer)",
        part_df["partner"].astype(str).head(25).tolist(),
        format_func=country_select_label,
        key="xpd_partner",
    )
    exp_to_p = float(part_df.loc[part_df["partner"].astype(str) == partner, "trade_value_usd_k"].sum())
    st.metric(
        f"{country_display_name(exporter_iso)} → partner (this HS6, USD thousands)",
        f"{exp_to_p:,.0f}",
    )

    if st.button("Load supplier concentration", key="xpd_load_suppliers_btn"):
        st.session_state["xpd_suppliers_loaded"] = True

    if not st.session_state.get("xpd_suppliers_loaded"):
        st.info("Pick a partner, then click **Load supplier concentration**.")
        return

    st.markdown("### Partner import concentration (all suppliers, BACI)")
    sup = rpc_trade_importer_supplier_breakdown(partner, int(year), str(hs_sel), 25)
    if sup.empty:
        st.warning(
            f"No bilateral rows for **importer={partner}**, **hs6={hs_sel}**, **year={year}**. "
            f"Run: `uv run python loaders/load_baci.py --all --importer-full-hs {partner}` "
            "(plus your usual V1 / `--exporter-full-hs` flags as needed), then refresh."
        )
        gdf = load_geodep_slice(partner, str(year), str(hs_sel).strip()[:6], 15)
        if not gdf.empty:
            gdf = gdf[gdf["hs6_code"].astype(str).str.strip() == str(hs_sel).strip()]
        if not gdf.empty:
            st.info("**GeoDep** summary (CEPII diagnostics — not full supplier list):")
            st.dataframe(gdf.head(5), width="stretch", hide_index=True)
        return
    sup_g = sup.rename(columns={"supplier_iso3": "exporter", "trade_value_usd_k": "trade_value_usd"})
    met = rpc_trade_importer_supplier_metrics(partner, int(year), str(hs_sel))
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Suppliers (exporters)", int(met.get("n_suppliers") or 0))
    m2.metric("Partner import total (USD k)", f"{float(met.get('total_usd_k') or 0):,.0f}")
    hhi = met.get("hhi")
    cr3 = met.get("cr3_pct")
    m3.metric("HHI (suppliers)", f"{float(hhi):.4f}" if hhi is not None else "—")
    m4.metric("CR3 (%)", f"{float(cr3):.1f}" if cr3 is not None else "—")
    st.caption(
        "**HHI** = sum of squared value shares (1 = one supplier). **CR3** = top-three suppliers’ share of partner imports of this HS6."
    )
    sel_share = 0.0
    if float(met.get("total_usd_k") or 0) > 0:
        sel = sup_g[sup_g["exporter"].astype(str) == exporter_iso]
        if not sel.empty:
            sel_share = float(sel["trade_value_usd"].iloc[0] / float(met["total_usd_k"]) * 100)
    st.metric(
        f"{country_display_name(exporter_iso)} share of partner’s imports of this HS6",
        f"{sel_share:.2f} %",
    )
    top25 = sup_g.head(25).copy()
    top25["_lbl"] = _series_country_labels(top25["exporter"])
    _st_altair_bar_h_by_value(top25, "trade_value_usd", "_lbl", x_title="USD thousands")
    st.dataframe(
        sup_g.assign(share_pct=sup_g["share_pct"]),
        width="stretch",
        hide_index=True,
    )

    gdf = load_geodep_slice(partner, str(year), str(hs_sel).strip()[:6], 5)
    if not gdf.empty:
        gdf = gdf[gdf["hs6_code"].astype(str).str.strip() == str(hs_sel).strip()]
    if not gdf.empty:
        with st.expander("CEPII GeoDep (same importer × HS6 × year)"):
            st.dataframe(gdf, width="stretch", hide_index=True)


@st.fragment()
def tab_group_dependencies() -> None:
    st.subheader("Group dependencies")
    st.caption(
        "Export-side: for each HS6, the group’s share of world exports + within-group single-point-of-failure. "
        "Import-side: for one HS6, which importers are most exposed to this group (drill-down). "
        "Snapshots are saved in Supabase to avoid recomputing."
    )

    y_probe = bilateral_distinct_column_values(
        "data_year",
        (),
        max_rows=min(5000, BILATERAL_DISTINCT_SCAN_CAP),
    )
    years: list[int] = []
    for y in y_probe:
        try:
            years.append(int(y))
        except (TypeError, ValueError):
            continue
    years = sorted(set(years))
    if not years:
        st.info("No rows in `bilateral_trade`.")
        return

    default_group = ["SAU", "ARE", "IRQ", "KWT", "QAT", "IRN", "OMN"]

    c1, c2, c3, c4 = st.columns([2.2, 1.1, 1.5, 1.2])
    with c1:
        year = st.selectbox("Year", years, index=len(years) - 1, key="grpdep_year")
    exporters_seen, used_fallback_scan = rpc_trade_distinct_exporters_for_year(int(year))
    if used_fallback_scan:
        st.warning(
            f"Exporter options built from a capped scan (**{BILATERAL_DISTINCT_SCAN_CAP:,}** rows) for **{year}** — "
            "some exporters may be missing. Apply the updated trade RPCs to Supabase to enable the complete list."
        )
    exporter_options = sorted(set(list(GULF_EXPORTER_ISO3_ORDER) + exporters_seen + default_group))
    with c1:
        group = st.multiselect(
            "Country group (ISO3)",
            options=exporter_options,
            default=[g for g in default_group if g in exporter_options],
            format_func=country_select_label,
            key="grpdep_group",
        )
    with c2:
        top_n = st.slider("Top HS6", 25, 500, 200, 25, key="grpdep_topn")
    with c3:
        hs_query = st.text_input(
            "HS6 filter (digits/keyword)",
            value="",
            key="grpdep_hs_query",
            help="Matches HS6 digits or HS description (if `hs_code_lookup` is loaded).",
        )
    with c4:
        imp_limit = st.slider("Top importers", 10, 200, 50, 10, key="grpdep_imp_limit")
    coverage_only = st.checkbox(
        "Hide HS6 where world coverage is incomplete (few exporters loaded)",
        value=True,
        help="Your `bilateral_trade` may only contain a subset of exporters/HS6 (e.g. full-HS6 for one exporter). "
        "When coverage is low, “% of world exports” will be misleading.",
        key="grpdep_cov_only",
    )
    with st.expander("Heavy compute: run CLI worker (same save as this tab)"):
        st.markdown(
            "For large **Top HS6** values, you can run the same snapshot write in a separate process "
            "(avoids blocking the browser). Save a JSON file with keys "
            "`data_year`, `group_iso3`, `hs_query_text`, `limit_n_hs6`, `import_hs6_code`, "
            "`limit_n_importers`, `coverage_only` (optional, default true), then:"
        )
        st.code(
            "uv run python scripts/run_group_dependency_snapshot.py --params-json path/to/params.json\n"
            "uv run python scripts/run_group_dependency_snapshot.py --params-json params.json --force",
            language="bash",
        )
        st.caption("Apply `schema/alter_trade_group_dependency_snapshots_job.sql` once if inserts error on new columns.")

    if not group:
        st.info("Select at least one country in the group.")
        return

    b1, b2 = st.columns([1, 1])
    with b1:
        do_load = st.button("Load / compute", type="primary", key="grpdep_load")
    with b2:
        force = st.button("Force recompute", key="grpdep_force")

    with st.expander("Saved snapshots (load a prior run — no compute needed)", expanded=False):
        snaps_early = _snapshot_list(50)
        if snaps_early.empty:
            st.caption("No saved snapshots yet. Use **Load / compute** to create one.")
        else:
            st.dataframe(snaps_early.drop(columns=["params_json"], errors="ignore"), width="stretch", hide_index=True)
            chosen_early = st.selectbox(
                "Open snapshot",
                snaps_early["id"].astype(int).tolist(),
                format_func=lambda sid: f"Snapshot {sid}",
                key="grpdep_open_snapshot_early",
            )
            if st.button("Load selected snapshot", key="grpdep_load_snapshot_btn_early"):
                snap_e = snaps_early.loc[snaps_early["id"].astype(int) == int(chosen_early)].head(1)
                if not snap_e.empty:
                    sid_e = int(chosen_early)
                    ph_e = str(snap_e["params_hash"].iloc[0])
                    st.session_state["grpdep_cached_ph"] = ph_e
                    st.session_state["grpdep_cached_export_df"] = _snapshot_rows_export(sid_e).copy()
                    st.session_state["grpdep_view_saved_snapshot"] = True
                    st.session_state["grpdep_last_params_hash"] = ph_e
                    st.success(f"Loaded snapshot {sid_e} (hash `{ph_e[:10]}…`).")
                    st.rerun()

    params_json: dict[str, Any] = {
        "version": 1,
        "data_year": int(year),
        "group_iso3": sorted({str(x).strip().upper() for x in group if str(x).strip()}),
        "hs_query_text": str(hs_query).strip(),
        "limit_n_hs6": int(top_n),
        "import_hs6_code": "",  # filled once HS6 selection exists
        "limit_n_importers": int(imp_limit),
    }
    ph = gdc.params_hash(params_json)
    existing = gdc.snapshot_by_hash(supabase(), ph)

    compute_requested = bool(do_load or force)
    cached_ph = st.session_state.get("grpdep_cached_ph")
    cached_export = st.session_state.get("grpdep_cached_export_df")
    view_saved_snap = bool(st.session_state.get("grpdep_view_saved_snapshot"))
    if view_saved_snap and cached_ph is not None and ph != cached_ph:
        st.session_state["grpdep_view_saved_snapshot"] = False

    # Heavy RPCs / DB snapshot reads only when the user clicks Load (or we show a cached result for the same params,
    # or a table opened from **Saved snapshots**).
    if not compute_requested:
        cache_ok = cached_export is not None and (
            (cached_ph == ph) or (view_saved_snap and cached_ph is not None)
        )
        if cache_ok:
            export_df_raw = cached_export.copy()
            if view_saved_snap and cached_ph != ph:
                st.caption(
                    "Showing a snapshot opened from **Saved snapshots**; controls above may not match — "
                    "adjust and **Load / compute** to align."
                )
        else:
            st.info(
                "Choose **year**, **group**, filters, and **sliders**, then click **Load / compute**. "
                "Large Supabase queries run only on that click — not when widgets change."
            )
            return
    elif existing and not force:
        snapshot_id = int(existing["id"])
        export_df_raw = _snapshot_rows_export(snapshot_id)
        st.session_state["grpdep_cached_ph"] = ph
        st.session_state["grpdep_cached_export_df"] = export_df_raw.copy()
        st.session_state["grpdep_view_saved_snapshot"] = False
        st.caption(f"Loaded saved snapshot **{snapshot_id}** (hash `{ph[:10]}…`).")
    else:
        export_df_raw = rpc_trade_group_world_share_by_hs6(int(year), list(group), str(hs_query), int(top_n))
        st.session_state["grpdep_cached_ph"] = ph
        st.session_state["grpdep_cached_export_df"] = export_df_raw.copy()
        st.session_state["grpdep_view_saved_snapshot"] = False

    export_df = export_df_raw.copy()
    if export_df.empty or "hs6_code" not in export_df.columns:
        st.warning("No HS6 lines match this filter for this group/year.")
        return
    if coverage_only and "world_exporter_count" in export_df.columns:
        export_df = export_df[export_df["world_exporter_count"].fillna(0).astype(int) >= 10]
        if export_df.empty:
            st.warning(
                "After filtering for coverage, no rows remain. Uncheck the coverage filter to see all results, "
                "or load broader BACI coverage for these HS6 codes."
            )
            return

    hs6_options = export_df["hs6_code"].astype(str).tolist()
    if not hs6_options:
        st.warning("No HS6 codes returned.")
        return

    t_exp, t_imp = st.tabs(["Export share of world", "Importer exposure"])

    with t_exp:
        st.markdown("**Export share of world (by HS6)**")
        disp = export_df.copy()
        disp = _merge_hs6_description_column(disp, "hs6_code")
        if "world_exporter_count" in disp.columns:
            st.caption(
                "Tip: check **world_exporter_count**. If it’s very small (e.g. 1), it likely means only a subset of exporters "
                "were loaded for that HS6/year, so “% of world exports” reflects only the loaded slice."
            )
        st.dataframe(disp, width="stretch", hide_index=True)

        hs_pick = st.selectbox(
            "Within-group breakdown (pick HS6)",
            hs6_options,
            index=0,
            format_func=hs6_select_label,
            key="grpdep_hs_pick",
        )
        bd = rpc_trade_group_member_breakdown_for_hs6(int(year), str(hs_pick), list(group))
        if not bd.empty:
            bd2 = bd.copy()
            bd2["_lbl"] = _series_country_labels(bd2["exporter_iso3"])
            _st_altair_bar_h_by_value(bd2, "export_usd_k", "_lbl", x_title="USD thousands")
            st.dataframe(bd2.drop(columns=["_lbl"], errors="ignore"), width="stretch", hide_index=True)

    with t_imp:
        st.markdown("**Importer exposure (drill-down)**")
        import_hs6 = st.selectbox(
            "HS6 for importer exposure",
            hs6_options,
            index=0,
            format_func=hs6_select_label,
            key="grpdep_import_hs6",
        )
        # If we loaded a snapshot and the selected HS6 matches it, reuse snapshot rows; else compute fresh (bounded).
        imp_df: pd.DataFrame
        if existing and not force:
            snap_params = existing.get("params_json") or {}
            snap_hs6 = str(snap_params.get("import_hs6_code") or "").strip()
            if snap_hs6 and snap_hs6 == str(import_hs6).strip():
                imp_df = _snapshot_rows_importer(int(existing["id"]), snap_hs6)
            else:
                imp_df = rpc_trade_group_importer_exposure_for_hs6(
                    int(year), str(import_hs6), list(group), int(imp_limit)
                )
        else:
            imp_df = rpc_trade_group_importer_exposure_for_hs6(int(year), str(import_hs6), list(group), int(imp_limit))
        if imp_df.empty:
            st.warning("No importers found with imports from this group for this HS6.")
        else:
            imp_disp = imp_df.copy()
            imp_disp.insert(1, "importer_name", _series_country_labels(imp_disp["importer_iso3"]))
            st.dataframe(imp_disp, width="stretch", hide_index=True)

            importer_pick = st.selectbox(
                "Inspect suppliers (importer)",
                imp_df["importer_iso3"].astype(str).tolist(),
                format_func=country_select_label,
                key="grpdep_importer_pick",
            )
            sup = rpc_trade_importer_supplier_breakdown(str(importer_pick), int(year), str(import_hs6), 50)
            met = rpc_trade_importer_supplier_metrics(str(importer_pick), int(year), str(import_hs6))
            if not sup.empty:
                sup2 = sup.rename(columns={"supplier_iso3": "exporter", "trade_value_usd_k": "trade_value_usd"}).copy()
                sup2["is_in_group"] = sup2["exporter"].astype(str).str.upper().isin({g.upper() for g in group})
                total = float(met.get("total_usd_k") or 0)
                from_group = float(sup2.loc[sup2["is_in_group"], "trade_value_usd"].sum())
                pct = (from_group / total * 100) if total > 0 else 0.0
                m1, m2, m3 = st.columns(3)
                m1.metric("Importer total (USD k)", f"{total:,.0f}")
                m2.metric("From group (USD k)", f"{from_group:,.0f}")
                m3.metric("From group (%)", f"{pct:.2f}")
                top = sup2.head(25).copy()
                top["_lbl"] = _series_country_labels(top["exporter"])
                _st_altair_bar_h_by_value(top, "trade_value_usd", "_lbl", x_title="USD thousands")
                st.dataframe(
                    sup2[["exporter", "trade_value_usd", "share_pct", "is_in_group"]],
                    width="stretch",
                    hide_index=True,
                )

    if do_load or force:
        params_json["import_hs6_code"] = str(import_hs6).strip()
        imp_for_save = rpc_trade_group_importer_exposure_for_hs6(
            int(year), str(params_json["import_hs6_code"]), list(group), int(imp_limit)
        )
        snapshot_id, ph = _write_snapshot_and_rows(
            params_json=params_json,
            export_rows=export_df,
            importer_rows=imp_for_save,
            force_recompute=bool(force),
        )
        st.session_state["grpdep_last_params_hash"] = ph
        st.success(f"Saved snapshot id **{snapshot_id}** (hash `{ph[:10]}…`).")


def main() -> None:
    st.set_page_config(page_title="Hormuz Supply Chain", layout="wide")
    st.title("Hormuz Supply Chain — Data exploration")
    st.sidebar.caption(
        "Supabase: service role if set in .env (avoids empty RLS tables); else anon/publishable key."
    )
    section = st.sidebar.radio(
        "Section",
        list(_APP_SECTIONS),
        key="main_nav_section",
    )
    st.sidebar.caption(
        "Only this section’s code runs when you use its controls (not all eight areas at once)."
    )

    try:
        if section == "Prices over time":
            tab_prices()
        elif section == "Who trades what":
            tab_who_trades()
        elif section == "Country profile":
            tab_country_profile()
        elif section == "Exporter & partners":
            tab_exporter_partners()
        elif section == "Group dependencies":
            tab_group_dependencies()
        elif section == "Crop production":
            tab_crop_rank()
        elif section == "Pipeline status":
            tab_pipeline()
        elif section == "Explore more":
            tab_explore_more()
    except Exception as e:
        st.error(f"Query failed: {e}")


if __name__ == "__main__":
    main()
