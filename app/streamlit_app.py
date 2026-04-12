"""Hormuz supply chain — read-only Supabase exploration dashboard."""
from __future__ import annotations

import hashlib
import html
import json
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
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

import pydeck as pdk

from loaders.load_gem_xlsx import DEFAULT_WORKBOOKS
from utils import group_dependency_compute as gdc
from utils.gem_facility_categories import (
    all_known_category_labels,
    emoji_for_category_label,
    style_for_source_sheet,
)
from utils.gem_regions import GEM_REGION_OPTIONS, point_in_regions
from utils.gem_map_support import (
    first_payload_keys,
    map_records_to_dataframe,
    payloads_to_map_records,
    payloads_to_map_records_enriched,
)
from utils.jodi_display import (
    JODI_FLOW_MEANINGS_EXPANDER_BODY,
    build_jodi_balance_sankey_figure,
    build_jodi_column_config,
    jodi_columns_for_view,
    prepare_jodi_display_df,
)
from utils.supabase_client import get_client, get_read_client

PAGE_SIZE = 1000
# GEM `gem_tracker_rows`: keyset pagination on `id` with modest page size (OFFSET pagination times out on large sheets).
GEM_KEYSET_PAGE_SIZE = 500
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
    "GEM infrastructure map",
    "Explore more",
)
BILATERAL_EXPORT_DRILLDOWN_CAP = 500_000
# Country profile: max bilateral rows to load for one country × year (same order of magnitude as HS6 slice).
BILATERAL_COUNTRY_PROFILE_MAX_ROWS = 2_000_000
# Who trades what: max bars per chart; full rankings available in expander below.
WHO_TRADES_CHART_TOP_N_MAX = 100
# Group dependencies: max HS6 codes per request for member×product matrix RPC (keep PostgREST payload reasonable).
GRP_MEMBER_MATRIX_MAX_HS6 = 500

# Exporter & partners tab: list these ISO3 first when present in `bilateral_trade`, then others A–Z.
GULF_EXPORTER_ISO3_ORDER: tuple[str, ...] = ("SAU", "OMN", "KWT", "QAT", "IRQ", "BHR", "ARE", "IRN")


@st.cache_data(ttl=86400)
def _all_iso3_for_multiselect() -> list[str]:
    """ISO 3166-1 alpha-3 codes for country pickers (human labels via `country_select_label`)."""
    return sorted({c.alpha_3 for c in pycountry.countries})


# Approximate country centers (WGS84) for Table 2 facility maps — yearbooks have no lat/lon; we jitter per row.
_USGS_FACILITY_COUNTRY_CENTERS: dict[str, tuple[float, float]] = {
    "ARE": (24.3, 54.4),
    "BHR": (26.1, 50.6),
    "EGY": (26.8, 30.8),
    "IRQ": (33.3, 44.4),
    "IRN": (32.4, 53.7),
    "JOR": (31.0, 36.0),
    "KWT": (29.3, 47.5),
    "LBN": (33.9, 35.9),
    "OMN": (21.5, 57.0),
    "QAT": (25.3, 51.2),
    "SAU": (24.0, 45.0),
    "SYR": (34.8, 39.0),
    "YEM": (15.6, 48.0),
    "AFG": (33.9, 67.7),
    "PAK": (30.4, 69.3),
    "IND": (22.6, 78.9),
    "CHN": (35.9, 104.2),
    "USA": (39.8, -98.6),
    "CAN": (61.0, -107.0),
    "MEX": (23.6, -102.6),
    "BRA": (-10.0, -51.9),
    "RUS": (61.5, 105.3),
    "AUS": (-25.3, 133.8),
    "ZAF": (-30.6, 25.0),
    "GBR": (54.0, -2.5),
    "DEU": (51.2, 10.5),
    "FRA": (46.2, 2.2),
    "ITA": (42.8, 12.6),
    "ESP": (40.4, -3.7),
    "JPN": (36.2, 138.3),
    "KOR": (36.5, 127.9),
    "IDN": (-2.5, 118.0),
    "MYS": (4.2, 101.7),
    "KAZ": (48.0, 66.9),
    "CHL": (-35.7, -71.5),
    "PER": (-9.2, -75.0),
    "COL": (4.6, -74.3),
    "MAR": (31.8, -7.1),
    "DZA": (28.0, 2.6),
    "TUN": (33.9, 9.5),
    "LBY": (27.0, 17.0),
    "TUR": (39.0, 35.0),
}


def _usgs_jitter_lat_lon(lat: float, lon: float, key: str) -> tuple[float, float]:
    h = hashlib.sha256(key.encode("utf-8")).digest()
    dl = (int.from_bytes(h[0:4], "big") / 2**32 - 0.5) * 1.4
    dlo = (int.from_bytes(h[4:8], "big") / 2**32 - 0.5) * 1.4
    nlat = max(-85.0, min(85.0, lat + dl))
    nlon = ((lon + dlo + 180.0) % 360.0) - 180.0
    return nlat, nlon


def _usgs_facility_coord_ok(lat: object, lon: object) -> bool:
    try:
        if lat is None or lon is None:
            return False
        if pd.isna(lat) or pd.isna(lon):
            return False
        la = float(lat)
        lo = float(lon)
        return -85.0 <= la <= 85.0 and -180.0 <= lo <= 180.0
    except (TypeError, ValueError):
        return False


def _usgs_facilities_map_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame | None, bool]:
    """Build lat/lon rows for pydeck. Prefer DB `geocode_lat`/`geocode_lon`; else country center + jitter.

    Returns (map_df, any_nominatim) for OSM attribution when `geocode_source` is nominatim.
    """
    if df.empty or "country_iso3" not in df.columns:
        return None, False
    centers = _USGS_FACILITY_COUNTRY_CENTERS
    rows: list[dict[str, Any]] = []
    any_nominatim = False
    for _, r in df.iterrows():
        iso = str(r.get("country_iso3") or "").strip().upper()
        gla = r.get("geocode_lat")
        glo = r.get("geocode_lon")
        gsrc = str(r.get("geocode_source") or "").strip().lower()
        gq = str(r.get("geocode_query") or "").strip()

        if _usgs_facility_coord_ok(gla, glo):
            lat = float(gla)  # type: ignore[arg-type]
            lon = float(glo)  # type: ignore[arg-type]
            if gsrc == "nominatim":
                any_nominatim = True
        else:
            base = centers.get(iso)
            if base is None:
                continue
            lat0, lon0 = base
            jk = str(r.get("record_fingerprint") or r.get("id") or "")
            lat, lon = _usgs_jitter_lat_lon(lat0, lon0, jk)

        fac = str(r.get("commodity_leaf_resolved") or "")
        loc = str(r.get("location") or "")
        own = str(r.get("owner_operator") or "")
        cap = r.get("capacity_numeric")
        try:
            cap_s = (
                f"{float(cap):.4g}"
                if cap is not None and str(cap).strip() != "" and pd.notna(cap)
                else str(r.get("capacity_raw") or "")
            )
        except (TypeError, ValueError):
            cap_s = str(r.get("capacity_raw") or "")
        title = html.escape(fac[:90] + ("…" if len(fac) > 90 else ""), quote=True)
        loc_e = html.escape(loc[:100] + ("…" if len(loc) > 100 else ""), quote=True)
        own_e = html.escape(own[:80] + ("…" if len(own) > 80 else ""), quote=True)
        cap_e = html.escape(cap_s[:40], quote=True)
        iso_e = html.escape(iso, quote=True)
        gq_e = html.escape(gq[:160] + ("…" if len(gq) > 160 else ""), quote=True) if gq else ""
        geo_line = f"<br/><small>Geocode: {gq_e}</small>" if gq_e else ""
        rows.append(
            {
                "lat": lat,
                "lon": lon,
                "hover_html": (
                    f"<b>{title}</b><br/><small>{iso_e}</small><br/>{loc_e}<br/><i>{own_e}</i><br/>{cap_e}{geo_line}"
                ),
            }
        )
    if not rows:
        return None, False
    return pd.DataFrame(rows), any_nominatim


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


def _food_balance_macro_wide(macro: pd.DataFrame) -> pd.DataFrame:
    """Country × year rows with `population` and/or `gdp_current_usd` (World Bank WDI via `country_macro_indicators`)."""
    if macro is None or macro.empty:
        return pd.DataFrame()
    req = {"country", "indicator", "data_year", "value"}
    if not req.issubset(macro.columns):
        return pd.DataFrame()
    sub = macro[macro["indicator"].isin(("population", "gdp_current_usd"))].copy()
    if sub.empty:
        return pd.DataFrame()
    wide = sub.pivot_table(
        index=["country", "data_year"],
        columns="indicator",
        values="value",
        aggfunc="first",
    )
    return wide.reset_index()


def _grpdep_six_digit_hs(text: str) -> str | None:
    """First six consecutive digits, or None if fewer than six digits."""
    d = "".join(c for c in str(text) if c.isdigit())
    return d[:6] if len(d) >= 6 else None


def _grpdep_baci_reload_commands(year: int, hs6: str, missing_iso3: frozenset[str]) -> str:
    """Shell snippet: prefer one `--hs6-codes` pass; alternative repeated `--exporter-full-hs`."""
    hs = str(hs6).strip().zfill(6)[:6]
    y = int(year)
    miss = sorted(missing_iso3)
    primary = f'uv run python loaders/load_baci.py --year {y} --hs6-codes "{hs}"'
    alt_flags = " ".join(f"--exporter-full-hs {m}" for m in miss)
    alt = f"uv run python loaders/load_baci.py --year {y} {alt_flags}"
    return (
        f"# Smallest fix — global rows for this HS6 (all partners; fair world share):\n{primary}\n\n"
        f"# Heavier — full HS6 book for each missing exporter only:\n{alt}"
    )


def _client() -> Client:
    """Prefer server key when present so RLS does not hide rows (e.g. table_catalog with RLS, no anon policy)."""
    try:
        return get_client()
    except RuntimeError:
        return get_read_client()


def _rpc_error_hint(exc: BaseException) -> str:
    """PostgREST / RPC failure text for the UI (permissions, missing function, etc.)."""
    try:
        from postgrest.exceptions import APIError

        if isinstance(exc, APIError):
            parts = [exc.code, exc.message, exc.details, exc.hint]
            return " | ".join(str(p) for p in parts if p)[:900]
    except Exception:
        pass
    return f"{type(exc).__name__}: {exc}"[:900]


def _rpc_fix_hint_markdown(err: str | None) -> str:
    """
    Short guidance after a failed RPC: distinguish timeout (57014) from missing GRANT / role.
    """
    if not err:
        return (
            "Common fix: run **`schema/migrations/20260415_grant_execute_public_rpc_trade.sql`** "
            "(or the GRANT block at the end of **`schema/rpc_trade_dashboards.sql`**) so the "
            "**`anon`** role can `EXECUTE` `rpc_trade_*` when using the publishable key. "
            "Or set **`SUPABASE_SERVICE_ROLE_KEY`** in `.env` for local Streamlit."
        )
    el = err.lower()
    if "57014" in err or "statement timeout" in el or "canceling statement due to statement timeout" in el:
        return (
            "**Database statement timeout** (Postgres cancelled the query — often code `57014`). "
            "Year dropdowns should read the small **`bilateral_trade_data_years`** cache (see **`20260417_bilateral_trade_data_years_cache.sql`** "
            "or **`schema/rpc_trade_dashboards.sql`**). If the cache is empty, the RPC falls back to a heavy `DISTINCT` on "
            "`bilateral_trade` — run **`refresh_bilateral_trade_data_years_cache()`** with the service role after loads, "
            "or **`VACUUM ANALYZE bilateral_trade`** / **`idx_bilateral_trade_data_year`** if needed."
        )
    if "42501" in err or "permission denied" in el or "must be owner" in el:
        return (
            "Permission issue: run **`schema/migrations/20260415_grant_execute_public_rpc_trade.sql`** "
            "(or the GRANT block at the end of **`schema/rpc_trade_dashboards.sql`**) so **`anon`** can `EXECUTE` "
            "`rpc_trade_*`, or use **`SUPABASE_SERVICE_ROLE_KEY`** in `.env` for local Streamlit."
        )
    return (
        "If the error mentions **EXECUTE** or **permission**, apply the **GRANT** migration above or the **service role** key. "
        "If it is **timeout** / **57014**, apply **`20260416_rpc_trade_distinct_data_years_timeout_and_index.sql`** "
        "or full **`rpc_trade_dashboards.sql`**."
    )


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
def rpc_trade_distinct_exporters() -> tuple[list[str], bool, str | None]:
    """
    Prefer the DB RPC (complete list). If the RPC isn't deployed yet, fall back to a capped scan.
    Returns (exporters, used_fallback_scan, rpc_error_detail_or_none).
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
        return sorted(set(out)), False, None
    except Exception as e:
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
        return sorted(set(normed)), True, _rpc_error_hint(e)


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
def grpdep_common_years_and_latest(group_iso3: tuple[str, ...]) -> tuple[tuple[int, ...], int | None]:
    """
    Intersection of rpc_trade_years_for_exporter across the group; latest = max(intersection).
    group_iso3 must be sorted for stable cache keys.
    """
    if not group_iso3:
        return (), None
    inter: set[int] | None = None
    for iso in group_iso3:
        yrs, _ = rpc_trade_years_for_exporter(iso)
        s = set(yrs) if yrs else set()
        inter = s if inter is None else inter & s
    assert inter is not None
    common = tuple(sorted(inter))
    latest = common[-1] if common else None
    return common, latest


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


@st.cache_data(ttl=300, show_spinner=False)
def rpc_trade_distinct_hs6_for_year(year: int) -> tuple[list[str], bool, str | None]:
    """
    Prefer the DB RPC (complete distinct HS6 list for the year). If the RPC is not deployed yet,
    fall back to a capped bilateral row scan (may miss codes).
    Returns (hs6_codes, used_fallback_scan, rpc_error_detail_or_none).
    """
    sb = supabase()
    try:
        res = sb.rpc("rpc_trade_distinct_hs6_for_year", {"p_data_year": int(year)}).execute()
        rows = res.data or []
        out: list[str] = []
        for r in rows:
            v = r.get("hs6_code")
            if v is not None and str(v).strip():
                out.append(str(v).strip())
        return sorted(set(out)), False, None
    except Exception as e:
        codes, trunc = bilateral_hs6_codes_for_year(int(year), BILATERAL_DISTINCT_SCAN_CAP)
        return codes, True, _rpc_error_hint(e)


@st.cache_data(ttl=300, show_spinner=False)
def rpc_trade_distinct_data_years() -> tuple[list[int], bool, str | None]:
    """
    Prefer the DB RPC (complete distinct `data_year` list). Fallback scans rows and may miss years.
    Returns (years, used_fallback_scan, rpc_error_detail_or_none).
    """
    sb = supabase()
    try:
        res = sb.rpc("rpc_trade_distinct_data_years", {}).execute()
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
        return sorted(set(out)), False, None
    except Exception as e:
        y_probe = bilateral_distinct_column_values(
            "data_year",
            (),
            max_rows=BILATERAL_DISTINCT_SCAN_CAP,
        )
        out2: list[int] = []
        for y in y_probe:
            try:
                out2.append(int(y))
            except (TypeError, ValueError):
                continue
        return sorted(set(out2)), True, _rpc_error_hint(e)


@st.cache_data(ttl=300, show_spinner=False)
def rpc_trade_distinct_country_iso3_for_year(year: int) -> tuple[list[str], bool, str | None]:
    """
    Prefer the DB RPC (distinct exporters ∪ importers for the year). Fallback: capped bilateral scan.
    Returns (iso3_codes, used_fallback_scan, rpc_error_detail_or_none).
    """
    sb = supabase()
    try:
        res = sb.rpc(
            "rpc_trade_distinct_country_iso3_for_year",
            {"p_data_year": int(year)},
        ).execute()
        rows = res.data or []
        out: list[str] = []
        for r in rows:
            v = r.get("country_iso3")
            if v is not None and str(v).strip():
                out.append(str(v).strip().upper())
        return sorted(set(out)), False, None
    except Exception as e:
        countries, truncated = bilateral_country_codes_for_year(int(year), BILATERAL_DISTINCT_SCAN_CAP)
        return countries, bool(truncated), _rpc_error_hint(e)


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


@st.cache_data(ttl=300, show_spinner="Running trade RPC…")
def rpc_trade_group_member_exports_for_hs6_list(
    year: int,
    group_iso3: list[str],
    hs6_codes: tuple[str, ...],
) -> pd.DataFrame:
    """Each group member's exports for many HS6 at once (same product list as the main group table)."""
    sb = supabase()
    res = (
        sb.rpc(
            "rpc_trade_group_member_exports_for_hs6_list",
            {
                "p_data_year": int(year),
                "group_iso3": [str(x).strip().upper() for x in group_iso3 if str(x).strip()],
                "p_hs6_codes": [str(x).strip() for x in hs6_codes if str(x).strip()],
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
        .order("group_export_usd_k", desc=True)
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

@st.cache_data(ttl=300, show_spinner=False)
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
    # Order by exporter (not hs6_code) so the first max_scan rows sweep more countries before
    # repeating HS6 — yields more distinct HS6 codes when the RPC is unavailable (legacy fallback).
    rows = fetch_bilateral_pages_limited(
        sb,
        columns="hs6_code",
        eq_filters={"data_year": year},
        max_rows=max_scan,
        order_by="exporter",
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
) -> pd.DataFrame:
    sb = supabase()

    def base_q():
        q = sb.table("usgs_mineral_statistics").select("*")
        cty = country_iso3.strip().upper()
        if cty and len(cty) == 3 and cty.isalpha():
            q = q.eq("country_iso3", cty)
        cs = commodity_substr.strip()
        if cs:
            q = q.ilike("commodity", f"%{cs}%")
        if data_year_token != "any":
            q = q.eq("data_year", int(data_year_token))
        return q

    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        res = base_q().order("id", desc=False).range(offset, offset + PAGE_SIZE - 1).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    df = pd.DataFrame(rows)
    if df.empty or "value_numeric" not in df.columns:
        return df
    return df.sort_values("value_numeric", ascending=False, na_position="last")


_USGS_MCS_SHARE_GROUP_COLS = (
    "mcs_chapter",
    "section",
    "commodity",
    "statistics",
    "statistics_detail",
    "unit",
)


def _usgs_mcs_share_vs_world(
    df: pd.DataFrame,
    iso3s: list[str],
    *,
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    For MCS rows with the same `statistics` (e.g. Mine production), compare selected
    countries' `value_numeric` sums to a global denominator per commodity line:

    - Prefer the **World total** row when present and positive.
    - Otherwise use the **implied global** total: sum of all rows with a 3-letter ISO3
      plus the **Other countries** aggregate row (excludes **World total** and other
      non-country aggregates without ISO3).

    Returns (top_slice_for_group, per_country_detail_for_same_commodity_lines).
    """
    cols = list(_USGS_MCS_SHARE_GROUP_COLS)
    need = set(cols) | {"country_name", "country_iso3", "value_numeric"}
    missing = need - set(df.columns)
    if missing:
        return pd.DataFrame(), pd.DataFrame()

    num = df.dropna(subset=["value_numeric"]).copy()
    if num.empty:
        return pd.DataFrame(), pd.DataFrame()

    for c in cols:
        if c in num.columns:
            num[c] = num[c].fillna("").map(lambda x: str(x).strip() if x != "" else "")

    nm = num["country_name"].fillna("").astype(str).str.strip().str.casefold()
    world_mask = nm == "world total"
    sel_mask = num["country_iso3"].isin(iso3s)
    iso = num["country_iso3"]
    iso_valid = iso.notna() & iso.astype(str).str.strip().str.match(r"^[A-Za-z]{3}$", na=False)
    implied_mask = (nm != "world total") & (iso_valid | (nm == "other countries"))

    w = num.loc[world_mask].groupby(list(cols), dropna=False)["value_numeric"].sum()
    sel_sum = num.loc[sel_mask].groupby(list(cols), dropna=False)["value_numeric"].sum()
    implied = num.loc[implied_mask].groupby(list(cols), dropna=False)["value_numeric"].sum()

    merged = pd.concat(
        [
            w.rename("world_row_production"),
            sel_sum.rename("selected_production"),
            implied.rename("implied_global_production"),
        ],
        axis=1,
    )
    merged = merged.dropna(subset=["selected_production"])
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()

    use_world = merged["world_row_production"].notna() & (merged["world_row_production"] > 0)
    merged["global_denominator"] = merged["world_row_production"].where(
        use_world, merged["implied_global_production"]
    )
    merged["denominator_source"] = [
        "World total row" if bool(u) else "Implied global (Σ ISO3 + Other countries)" for u in use_world
    ]
    merged["pct_of_world"] = (
        (merged["selected_production"] / merged["global_denominator"] * 100.0).where(
            merged["global_denominator"].notna() & (merged["global_denominator"] > 0)
        )
    )
    merged = merged.sort_values("selected_production", ascending=False).head(int(top_n))

    top_keys = merged.reset_index()[list(cols)]
    sel_detail = (
        num.loc[sel_mask]
        .merge(top_keys, on=list(cols), how="inner")
        .groupby(["country_iso3"] + list(cols), dropna=False)["value_numeric"]
        .sum()
        .reset_index()
        .rename(columns={"value_numeric": "production"})
    )
    world_map = merged.reset_index()[
        list(cols) + ["global_denominator", "denominator_source"]
    ].drop_duplicates(subset=list(cols))
    sel_detail = sel_detail.merge(world_map, on=list(cols), how="left")
    sel_detail["pct_of_world"] = (
        (sel_detail["production"] / sel_detail["global_denominator"] * 100.0).where(
            sel_detail["global_denominator"].notna() & (sel_detail["global_denominator"] > 0)
        )
    )
    return merged.reset_index(), sel_detail


@st.cache_data(ttl=300, show_spinner="Loading USGS MCS (statistic × year)…")
def load_usgs_mcs_statistic_year(data_year: int, statistics_exact: str) -> pd.DataFrame:
    """All MCS rows for one calendar `data_year` and exact `statistics` label (e.g. Mine production)."""
    stat = statistics_exact.strip()
    if not stat:
        return pd.DataFrame()
    sb = supabase()
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        res = (
            sb.table("usgs_mineral_statistics")
            .select("*")
            .eq("data_year", int(data_year))
            .eq("statistics", stat)
            .order("id", desc=False)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner="Listing MCS statistics for year…")
def load_usgs_mcs_distinct_statistics_for_year(data_year: int) -> list[str]:
    """Distinct `statistics` labels present in MCS for `data_year` (scan; cached)."""
    sb = supabase()
    seen: set[str] = set()
    offset = 0
    while True:
        res = (
            sb.table("usgs_mineral_statistics")
            .select("statistics")
            .eq("data_year", int(data_year))
            .order("id", desc=False)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = res.data or []
        for r in batch:
            s = r.get("statistics")
            if s is None:
                continue
            t = str(s).strip()
            if t:
                seen.add(t)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return sorted(seen)


@st.cache_data(ttl=300, show_spinner="Loading USGS myb3 production…")
def load_usgs_myb3_production_slice(
    country_iso3s: tuple[str, ...],
    reference_year: int,
    commodity_substr: str,
) -> pd.DataFrame:
    sb = supabase()
    cty_list = sorted(
        {
            c.strip().upper()
            for c in country_iso3s
            if isinstance(c, str) and len(c.strip()) == 3 and str(c.strip()).isalpha()
        }
    )
    if not cty_list:
        return pd.DataFrame()
    ref_y = int(reference_year)

    def base_q():
        q = sb.table("usgs_myb3_production").select("*").eq("reference_year", ref_y)
        if len(cty_list) == 1:
            q = q.eq("country_iso3", cty_list[0])
        else:
            q = q.in_("country_iso3", cty_list)
        cs = commodity_substr.strip()
        if cs:
            q = q.ilike("commodity_path", f"%{cs}%")
        return q

    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        res = base_q().order("id", desc=False).range(offset, offset + PAGE_SIZE - 1).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["stat_year", "commodity_path"], ascending=[True, True])


@st.cache_data(ttl=300, show_spinner="Loading USGS facilities…")
def load_usgs_facilities_slice(
    country_iso3s: tuple[str, ...],
    reference_year: int,
) -> pd.DataFrame:
    sb = supabase()
    cty_list = sorted(
        {
            c.strip().upper()
            for c in country_iso3s
            if isinstance(c, str) and len(c.strip()) == 3 and str(c.strip()).isalpha()
        }
    )
    if not cty_list:
        return pd.DataFrame()
    ref_y = int(reference_year)
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        q = (
            sb.table("usgs_country_mineral_facilities")
            .select("*")
            .eq("reference_year", ref_y)
            .order("id", desc=False)
            .range(offset, offset + PAGE_SIZE - 1)
        )
        if len(cty_list) == 1:
            q = q.eq("country_iso3", cty_list[0])
        else:
            q = q.in_("country_iso3", cty_list)
        res = q.execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return pd.DataFrame(rows)


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


@st.cache_data(ttl=300, show_spinner="Listing GEM workbooks…")
def load_gem_distinct_source_files() -> list[str]:
    """Distinct `source_file` values in Supabase; falls back to default bundle list if RPC unavailable."""
    sb = supabase()
    try:
        res = sb.rpc("rpc_gem_distinct_source_files", {}).execute()
        rows = res.data or []
        out = sorted({str(r["source_file"]) for r in rows if r.get("source_file")})
        if out:
            return out
    except Exception:
        pass
    return sorted(DEFAULT_WORKBOOKS.keys())


@st.cache_data(ttl=300, show_spinner="Counting GEM rows…")
def load_gem_sheet_row_count(source_file: str, sheet_name: str) -> int | None:
    sb = supabase()
    try:
        res = (
            sb.table("gem_tracker_rows")
            .select("id", count="exact")
            .eq("source_file", source_file.strip())
            .eq("sheet_name", sheet_name)
            .limit(1)
            .execute()
        )
        c = res.count
        return int(c) if c is not None else None
    except Exception:
        return None


def _load_gem_payloads_keyset(
    source_file: str,
    sheet_name: str,
    *,
    max_rows: int | None,
) -> list[dict[str, Any]]:
    """
    `id` + `payload` in `id` order using keyset pagination (`id > last`).
    Avoids PostgREST `OFFSET`, which scans skipped rows and triggers **statement timeout** on large sheets.
    """
    sb = supabase()
    sf = source_file.strip()
    sn = sheet_name.strip()
    n_cap = None if max_rows is None else max(1, int(max_rows))
    rows: list[dict[str, Any]] = []
    last_id: int | None = None
    while True:
        if n_cap is not None and len(rows) >= n_cap:
            break
        take = GEM_KEYSET_PAGE_SIZE if n_cap is None else min(GEM_KEYSET_PAGE_SIZE, n_cap - len(rows))
        if take <= 0:
            break
        q = (
            sb.table("gem_tracker_rows")
            .select("id,payload")
            .eq("source_file", sf)
            .eq("sheet_name", sn)
        )
        if last_id is not None:
            q = q.gt("id", last_id)
        q = q.order("id").limit(take)
        res = q.execute()
        batch = res.data or []
        rows.extend(batch)
        if not batch:
            break
        if len(batch) < take:
            break
        last_id = int(batch[-1]["id"])
    return rows


@st.cache_data(ttl=300, show_spinner="Loading GEM rows for map…")
def load_gem_payloads_for_sheet(source_file: str, sheet_name: str, max_rows: int) -> list[dict[str, Any]]:
    """Paginated `id` + `payload` only, in `id` order, capped at `max_rows` (keyset pages)."""
    return _load_gem_payloads_keyset(source_file, sheet_name, max_rows=max_rows)


@st.cache_data(ttl=600, show_spinner="Loading full GEM sheet…")
def load_gem_payloads_for_sheet_complete(source_file: str, sheet_name: str) -> list[dict[str, Any]]:
    """All `id` + `payload` rows for a sheet (keyset pages until exhausted)."""
    return _load_gem_payloads_keyset(source_file, sheet_name, max_rows=None)


@st.cache_data(ttl=300, show_spinner="Listing GEM source/sheet pairs…")
def load_gem_all_source_sheet_pairs() -> list[tuple[str, str]]:
    """All `(source_file, sheet_name)` pairs present in the database."""
    files = load_gem_distinct_source_files()
    pairs: list[tuple[str, str]] = []
    for fn in files:
        for sn in load_gem_sheet_names(fn):
            pairs.append((fn, sn))
    return sorted(pairs)


def _gem_fetch_pair_payloads_complete(source_file: str, sheet_name: str) -> list[dict[str, Any]]:
    raw = load_gem_payloads_for_sheet_complete(source_file, sheet_name)
    return [{**dict(r), "source_file": source_file, "sheet_name": sheet_name} for r in raw]


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
    n1, n2 = st.columns(2)
    with n1:
        per_capita = st.checkbox(
            "Per capita (kg / year)",
            value=False,
            key="em_fert_per_cap",
            help="Fertilizer value ÷ population (World Bank WDI). Tonnes × 1000 / population.",
        )
    with n2:
        per_gdp = st.checkbox(
            "Per GDP (metric tonnes / $1B GDP)",
            value=False,
            key="em_fert_per_gdp",
            help="Fertilizer value ÷ GDP in current USD (World Bank WDI). Mass intensity relative to economy size.",
        )
    if per_capita and per_gdp:
        st.warning("Use **one** normalization at a time. Showing **per capita**; uncheck it to use per GDP.")
        per_gdp = False
    macro = (
        load_country_macro_indicators()
        if (per_capita or per_gdp)
        else pd.DataFrame()
    )
    macro_w = _food_balance_macro_wide(macro)
    top = sub.groupby("country", as_index=False)["value_tonnes"].sum()
    if per_capita or per_gdp:
        need_col = "population" if per_capita else "gdp_current_usd"
        if macro_w.empty or need_col not in macro_w.columns:
            st.warning(
                "No usable `country_macro_indicators` for this normalization. "
                "Run the World Bank WDI puller (`pull_worldbank_wdi.py`) to load population and GDP. "
                "Showing absolute tonnes."
            )
            top = top.nlargest(20, "value_tonnes")
            top_chart = top.assign(_lbl=_series_country_labels(top["country"]))
            st.caption(
                f"Top **20** countries · **{met}** — **{ft}** ({y}) · bars sorted by value (largest at top)."
            )
            _st_altair_bar_h_by_value(top_chart, "value_tonnes", "_lbl", x_title="Tonnes")
        else:
            top["data_year"] = int(y)
            merged = top.merge(macro_w, on=["country", "data_year"], how="left")
            if per_capita:
                pop = merged["population"].astype(float)
                ok = pop > 0
                merged.loc[ok, "_plot"] = merged.loc[ok, "value_tonnes"].astype(float) * 1000.0 / pop.loc[ok]
                x_title = "kg per capita (annual)"
                vf = ",.1f"
            else:
                gdp = merged["gdp_current_usd"].astype(float)
                ok = gdp > 0
                merged.loc[ok, "_plot"] = merged.loc[ok, "value_tonnes"].astype(float) * 1e9 / gdp.loc[ok]
                x_title = "Metric tonnes per $1B GDP"
                vf = ",.2f"
            plot_df = merged[ok & merged["_plot"].notna()].copy()
            if plot_df.empty:
                st.warning(
                    "No countries with macro data for this year to normalize (check WDI coverage vs fertilizer year). "
                    "Showing absolute tonnes."
                )
                top = sub.groupby("country", as_index=False)["value_tonnes"].sum().nlargest(20, "value_tonnes")
                top_chart = top.assign(_lbl=_series_country_labels(top["country"]))
                st.caption(
                    f"Top **20** countries · **{met}** — **{ft}** ({y}) · bars sorted by value (largest at top)."
                )
                _st_altair_bar_h_by_value(top_chart, "value_tonnes", "_lbl", x_title="Tonnes")
            else:
                plot_df = plot_df.nlargest(20, "_plot")
                top_chart = plot_df.assign(_lbl=_series_country_labels(plot_df["country"]))
                st.caption(
                    f"Top **20** countries · **{met}** — **{ft}** ({y}) · bars sorted by normalized value "
                    "(largest at top). World Bank WDI denominators."
                )
                _st_altair_bar_h_by_value(top_chart, "_plot", "_lbl", x_title=x_title, value_format=vf)
    else:
        top = top.nlargest(20, "value_tonnes")
        st.caption(
            f"Top **20** countries · **{met}** — **{ft}** ({y}) · bars sorted by value (largest at top)"
        )
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
        n1, n2 = st.columns(2)
        with n1:
            per_capita = st.checkbox(
                "Per capita (kg / year)",
                value=False,
                key="em_fbs_per_cap",
                help="Food balance value ÷ population (World Bank WDI). Interprets mass flows on a per-person basis.",
            )
        with n2:
            per_gdp = st.checkbox(
                "Per GDP (metric tonnes / $1B GDP)",
                value=False,
                key="em_fbs_per_gdp",
                help="Food balance value ÷ GDP in current USD (World Bank WDI). Mass intensity relative to economy size.",
            )
        if per_capita and per_gdp:
            st.warning("Use **one** normalization at a time. Showing **per capita**; uncheck it to use per GDP.")
            per_gdp = False
        macro = (
            load_country_macro_indicators()
            if (per_capita or per_gdp)
            else pd.DataFrame()
        )
        macro_w = _food_balance_macro_wide(macro)
        top = sub.groupby("country", as_index=False)["value"].sum()
        if per_capita or per_gdp:
            need_col = "population" if per_capita else "gdp_current_usd"
            if macro_w.empty or need_col not in macro_w.columns:
                st.warning(
                    "No usable `country_macro_indicators` for this normalization. "
                    "Run the World Bank WDI puller (`pull_worldbank_wdi.py`) to load population and GDP. "
                    "Showing absolute tonnes."
                )
                top = top.nlargest(20, "value")
                top_c = top.assign(_lbl=_series_country_labels(top["country"]))
                st.caption("Top **20** countries · bars sorted by value (largest at top).")
                _st_altair_bar_h_by_value(top_c, "value", "_lbl", x_title="Tonnes")
            else:
                top["data_year"] = int(y)
                merged = top.merge(macro_w, on=["country", "data_year"], how="left")
                if per_capita:
                    pop = merged["population"].astype(float)
                    ok = pop > 0
                    merged.loc[ok, "_plot"] = merged.loc[ok, "value"].astype(float) * 1000.0 / pop.loc[ok]
                    x_title = "kg per capita (annual)"
                    vf = ",.1f"
                else:
                    gdp = merged["gdp_current_usd"].astype(float)
                    ok = gdp > 0
                    merged.loc[ok, "_plot"] = merged.loc[ok, "value"].astype(float) * 1e9 / gdp.loc[ok]
                    x_title = "Metric tonnes per $1B GDP"
                    vf = ",.2f"
                plot_df = merged[ok & merged["_plot"].notna()].copy()
                if plot_df.empty:
                    st.warning(
                        "No countries with macro data for this year to normalize (check WDI coverage vs FBS year). "
                        "Showing absolute tonnes."
                    )
                    top = sub.groupby("country", as_index=False)["value"].sum().nlargest(20, "value")
                    top_c = top.assign(_lbl=_series_country_labels(top["country"]))
                    st.caption("Top **20** countries · bars sorted by value (largest at top).")
                    _st_altair_bar_h_by_value(top_c, "value", "_lbl", x_title="Tonnes")
                else:
                    plot_df = plot_df.nlargest(20, "_plot")
                    top_c = plot_df.assign(_lbl=_series_country_labels(plot_df["country"]))
                    st.caption(
                        f"Top **20** countries · **{met}** for **{com}** ({y}) · bars sorted by normalized value "
                        "(largest at top). World Bank WDI denominators."
                    )
                    _st_altair_bar_h_by_value(top_c, "_plot", "_lbl", x_title=x_title, value_format=vf)
        else:
            top = top.nlargest(20, "value")
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
        n1, n2 = st.columns(2)
        with n1:
            per_capita = st.checkbox(
                "Per capita (kg / year)",
                value=False,
                key="em_fbs_per_cap",
                help="Food balance value ÷ population (World Bank WDI). Interprets mass flows on a per-person basis.",
            )
        with n2:
            per_gdp = st.checkbox(
                "Per GDP (metric tonnes / $1B GDP)",
                value=False,
                key="em_fbs_per_gdp",
                help="Food balance value ÷ GDP in current USD (World Bank WDI). Mass intensity relative to economy size.",
            )
        if per_capita and per_gdp:
            st.warning("Use **one** normalization at a time. Showing **per capita**; uncheck it to use per GDP.")
            per_gdp = False
        macro = (
            load_country_macro_indicators()
            if (per_capita or per_gdp)
            else pd.DataFrame()
        )
        macro_w = _food_balance_macro_wide(macro)
        cap = f"**{country_display_name(ctry)}** (`{ctry}`)"
        if per_capita or per_gdp:
            need_col = "population" if per_capita else "gdp_current_usd"
            if macro_w.empty or need_col not in macro_w.columns:
                st.warning(
                    "No usable `country_macro_indicators` for this normalization. "
                    "Run the World Bank WDI puller (`pull_worldbank_wdi.py`) to load population and GDP. "
                    "Showing absolute tonnes."
                )
                st.caption(cap)
                line = sub.set_index("data_year")[["value"]].astype(float)
                st.line_chart(line, height=350)
            else:
                merged = sub.merge(macro_w, on=["country", "data_year"], how="left")
                if per_capita:
                    pop = merged["population"].astype(float)
                    ok = pop > 0
                    merged.loc[ok, "_plot"] = merged.loc[ok, "value"].astype(float) * 1000.0 / pop.loc[ok]
                    y_label = "kg per capita (annual)"
                else:
                    gdp = merged["gdp_current_usd"].astype(float)
                    ok = gdp > 0
                    merged.loc[ok, "_plot"] = merged.loc[ok, "value"].astype(float) * 1e9 / gdp.loc[ok]
                    y_label = "Metric tonnes per $1B GDP"
                line_part = merged.loc[ok & merged["_plot"].notna(), ["data_year", "_plot"]].copy()
                if line_part.empty:
                    st.warning("No overlapping World Bank years for this country; showing absolute tonnes.")
                    st.caption(cap)
                    line = sub.set_index("data_year")[["value"]].astype(float)
                    st.line_chart(line, height=350)
                else:
                    st.caption(f"{cap} · **{y_label}** (World Bank WDI denominators)")
                    line = (
                        line_part.set_index("data_year")[["_plot"]]
                        .astype(float)
                        .rename(columns={"_plot": y_label})
                    )
                    st.line_chart(line, height=350)
        else:
            st.caption(cap)
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
        "Monthly reporter statistics — use **exact** `energy_product` and `flow_breakdown` strings from the JODI CSV "
        "(SDMX field names; differs from EIA `energy_trade_flows`). Hover column headers in the table for definitions."
    )
    with st.expander("What do flow codes mean?"):
        st.markdown(JODI_FLOW_MEANINGS_EXPANDER_BODY)
    cty = st.text_input(
        "Country (ISO3), optional",
        value="",
        key="em_jodi_c",
        help="Filters `country` (ISO 3166-1 alpha-3). Rows store JODI `REF_AREA` as ISO2 in **Reporter (ISO2)**.",
    ).strip().upper()
    prod = st.text_input(
        "Energy product (exact), optional",
        value="CRUDEOIL",
        key="em_jodi_p",
        help="Must match CSV `ENERGY_PRODUCT` exactly (e.g. NATGAS, CRUDEOIL). Oil and gas use different product lists.",
    ).strip()
    flow = st.text_input(
        "Flow breakdown (exact), optional",
        value="",
        key="em_jodi_f",
        help="Must match CSV `FLOW_BREAKDOWN` exactly (e.g. INDPROD, TOTIMPSB). See JODI short/long name PDFs for codes.",
    ).strip()
    unit = st.text_input(
        "Unit measure (exact), optional",
        value="",
        key="em_jodi_u",
        help="Must match CSV `UNIT_MEASURE` exactly (e.g. M3, TJ, KBD).",
    ).strip()
    yopts = ["any"] + [str(y) for y in range(2005, 2031)]
    c1, c2 = st.columns(2)
    with c1:
        yf = st.selectbox(
            "Year from",
            yopts,
            index=0,
            key="em_jodi_yf",
            help="Lower bound on `data_year` (parsed from CSV `TIME_PERIOD`).",
        )
    with c2:
        yt = st.selectbox(
            "Year to",
            yopts,
            index=0,
            key="em_jodi_yt",
            help="Upper bound on `data_year`.",
        )
    row_limit = st.slider(
        "Max rows fetched",
        min_value=500,
        max_value=5000,
        value=2000,
        step=100,
        key="em_jodi_lim",
        help="Cap on rows returned from Supabase for this query.",
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
    display_df = prepare_jodi_display_df(
        df,
        country_label_fn=country_select_label,
        include_date=True,
        energy_date_fn=_energy_date_column,
    )
    display_df = display_df.sort_values(["data_year", "data_month"])
    main_cols = jodi_columns_for_view(display_df, technical=False)
    st.dataframe(
        display_df[main_cols],
        width="stretch",
        hide_index=True,
        column_order=main_cols,
        column_config=build_jodi_column_config(display_df, main_cols),
    )
    if has_cty and not display_df.empty:
        st.markdown("### Balance snapshot (Sankey)")
        st.caption(
            "National **questionnaire lines** for one month and unit — link thickness uses **absolute** values; "
            "hover shows the **signed** figure. This is an **illustrative** layout, not proof of a closed balance "
            "(reporting gaps and sign conventions apply). Stock **levels** (e.g. closing stocks) are omitted; "
            "stock **change** is split by sign (draw → supply side, build → disposition side)."
        )
        prod_u = prod.upper() if prod else ""
        dfp = (
            display_df[display_df["energy_product"].astype(str).str.upper() == prod_u].copy()
            if prod_u
            else display_df.copy()
        )
        years_avail = sorted({int(y) for y in dfp["data_year"].dropna().unique().tolist()})
        if years_avail:
            default_yi = len(years_avail) - 1
            if "date" in dfp.columns and not dfp.empty:
                try:
                    last_d = pd.to_datetime(dfp["date"], errors="coerce").max()
                    if pd.notna(last_d):
                        ly = int(last_d.year)
                        if ly in years_avail:
                            default_yi = years_avail.index(ly)
                except (TypeError, ValueError):
                    pass
            sy = st.selectbox(
                "Sankey — year",
                years_avail,
                index=default_yi,
                key="em_jodi_sy",
                help="Filter rows to this calendar year (must be present in the table slice above).",
            )
            d_y = dfp[dfp["data_year"] == sy]
            months_avail = sorted({int(m) for m in d_y["data_month"].dropna().unique().tolist()})
            month_labels = [f"{m:02d}" for m in months_avail]
            default_mi = len(months_avail) - 1
            if "date" in d_y.columns and not d_y.empty:
                try:
                    last_d = pd.to_datetime(d_y["date"], errors="coerce").max()
                    if pd.notna(last_d) and int(last_d.month) in months_avail:
                        default_mi = months_avail.index(int(last_d.month))
                except (TypeError, ValueError):
                    pass
            sm_label = st.selectbox(
                "Sankey — month",
                month_labels,
                index=min(default_mi, len(month_labels) - 1),
                key="em_jodi_sm",
                help="Calendar month for the snapshot.",
            )
            sm = int(sm_label)
            d_ym = d_y[d_y["data_month"] == sm]
            unit_opts = sorted(
                {str(u).strip() for u in d_ym["unit_measure"].dropna().unique().tolist() if str(u).strip()}
            )
            if unit_opts:
                pref_u = "KBD" if "KBD" in unit_opts else unit_opts[0]
                ui = unit_opts.index(pref_u) if pref_u in unit_opts else 0
                su = st.selectbox(
                    "Sankey — unit",
                    unit_opts,
                    index=ui,
                    key="em_jodi_su",
                    help="Compare flows only within the same unit (JODI may publish multiple units).",
                )
                sk = d_ym[d_ym["unit_measure"] == su].copy()
                sk = prepare_jodi_display_df(sk, country_label_fn=country_select_label, include_date=False)
                fig = build_jodi_balance_sankey_figure(
                    sk,
                    hub_label="Illustrative balance (selected month)",
                    title=f"{country_select_label(cty)} · {prod_u or '—'} · {sy}-{sm:02d} · {su}",
                )
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(
                        "No Sankey for this month/unit — need numeric `obs_value` rows classifiable as supply or disposition "
                        "(see table)."
                    )
            else:
                st.caption("No `unit_measure` values for this year/month in the current slice — widen year range or row limit.")
        else:
            st.caption("No years in this slice for the selected product — adjust filters.")

    work = display_df.sort_values("date")
    num = work.dropna(subset=["obs_value"])
    if num.empty:
        st.warning("No numeric `obs_value` in this slice (see **Value (raw)** in the table).")
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
    tech_cols = [c for c in ("id", "source", "pulled_at") if c in display_df.columns]
    if tech_cols:
        with st.expander("Loader metadata (row id, loader, timestamp)"):
            st.dataframe(
                display_df[tech_cols],
                width="stretch",
                hide_index=True,
                column_order=tech_cols,
                column_config=build_jodi_column_config(display_df, tech_cols),
            )


def explore_usgs() -> None:
    st.subheader("USGS minerals")
    mode = st.radio(
        "Dataset",
        [
            "MCS (commodity statistics)",
            "MCS production share (% of world)",
            "Yearbook Table 1 (production)",
            "Yearbook Table 2 (facilities)",
        ],
        horizontal=True,
        key="em_usgs_mode",
    )

    if mode.startswith("MCS (commodity statistics)"):
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
        )
        if df.empty:
            st.warning("No rows for this filter.")
            return
        st.caption(f"**{len(df)}** rows, ordered by `value_numeric` desc.")
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

    if mode.startswith("MCS production share"):
        st.caption(
            "Uses **`usgs_mineral_statistics`**: for each commodity line (chapter, section, commodity, statistic "
            "detail, unit), compares summed production for your **country selection** to the **World total** row in the "
            "same MCS extract (or the implied global total when World is missing). Pick the **Statistics** row type "
            "below — options are whatever labels exist in your loaded MCS data for the selected year."
        )
        _share_years = [str(y) for y in range(1990, 2031)]
        _def_y = "2024"
        cy = st.selectbox(
            "Data year",
            _share_years,
            index=_share_years.index(_def_y) if _def_y in _share_years else 0,
            key="em_usgs_share_y",
        )
        stat_opts = load_usgs_mcs_distinct_statistics_for_year(int(cy))
        _other = "— Other (type label) —"
        if stat_opts:
            default_stat = "Mine production"
            idx = stat_opts.index(default_stat) if default_stat in stat_opts else 0
            merged_opts = stat_opts + [_other]
            pick = st.selectbox(
                "Statistics",
                merged_opts,
                index=idx,
                key="em_usgs_share_stat_pick",
                help="MCS uses one row per statistic (mine production, imports, etc.). The query uses one label at a time so units stay comparable.",
            )
            if pick == _other:
                stat = st.text_input(
                    "Custom Statistics label",
                    value="",
                    key="em_usgs_share_stat_custom",
                    placeholder="Exact label as in MCS / database",
                ).strip()
            else:
                stat = pick
        else:
            st.caption(f"No MCS rows loaded for **{cy}** — enter a label to try anyway (e.g. **Mine production**).")
            stat = st.text_input(
                "Statistics",
                value="Mine production",
                key="em_usgs_share_stat_fallback",
            ).strip()
        _iso_opts = _all_iso3_for_multiselect()
        _gulf_default = [c for c in GULF_EXPORTER_ISO3_ORDER if c in _iso_opts]
        iso3s = st.multiselect(
            "Countries",
            options=_iso_opts,
            default=_gulf_default,
            format_func=country_select_label,
            help=(
                "Production is summed across the selection. Default = Gulf group (same order as **Exporter & "
                "partners**: Saudi Arabia, Oman, Kuwait, Qatar, Iraq, Bahrain, UAE, Iran)."
            ),
            key="em_usgs_share_countries",
        )
        top_n = st.slider("Top commodities (by combined production)", 5, 50, 15, key="em_usgs_share_top")
        if not iso3s:
            st.info("Select at least one country (default is the full **Gulf** set above).")
            return
        if not stat:
            st.warning("Enter a **Statistics** label.")
            return
        df_all = load_usgs_mcs_statistic_year(int(cy), stat)
        if df_all.empty:
            st.warning(f"No rows for **data_year** {cy} and **statistics** `{stat}`. Check spelling against the MCS CSV.")
            return
        top_df, detail_df = _usgs_mcs_share_vs_world(df_all, iso3s, top_n=int(top_n))
        if top_df.empty:
            st.warning(
                "No numeric production for the selected countries with this filter. "
                "Try another year/statistic, or confirm those countries appear in MCS for this line."
            )
            return
        n_implied = int((top_df["denominator_source"] == "Implied global (Σ ISO3 + Other countries)").sum())
        if n_implied:
            st.info(
                f"**{n_implied}** commodity line(s) have no usable **World total** row in MCS — the global total is "
                "**implied** by summing all country rows with an ISO3 code plus the **Other countries** row "
                "(same statistic line and units)."
            )
        bad_denom = top_df["global_denominator"].isna() | (top_df["global_denominator"] <= 0)
        n_bad = int(bad_denom.sum())
        if n_bad:
            st.warning(
                f"**{n_bad}** row(s) have no global denominator (no World row and no implied sum) — **%** is blank."
            )
        grp_lbl = ", ".join(country_select_label(c) for c in iso3s)
        st.caption(
            f"**{len(top_df)}** commodity lines — **{grp_lbl}** · share of **global total** "
            "(World row when present, else implied sum)."
        )
        disp = top_df.assign(
            _lbl=(
                top_df["commodity"].astype(str).str.slice(0, 52)
                + " · "
                + top_df["section"].astype(str).str.slice(0, 28)
            )
        )
        chart_df = disp.dropna(subset=["pct_of_world"])
        if not chart_df.empty:
            _st_altair_bar_h_by_value(
                chart_df,
                "pct_of_world",
                "_lbl",
                x_title="% of global total",
                value_format=",.1f",
            )
        show_cols = [
            "mcs_chapter",
            "section",
            "commodity",
            "statistics_detail",
            "unit",
            "selected_production",
            "global_denominator",
            "denominator_source",
            "world_row_production",
            "implied_global_production",
            "pct_of_world",
        ]
        show = top_df[[c for c in show_cols if c in top_df.columns]].copy()
        st.dataframe(
            show,
            width="stretch",
            hide_index=True,
            column_config={
                "selected_production": st.column_config.NumberColumn("Selected production", format="%.4g"),
                "global_denominator": st.column_config.NumberColumn("Global total (denominator)", format="%.4g"),
                "denominator_source": st.column_config.TextColumn("Denominator source"),
                "world_row_production": st.column_config.NumberColumn("MCS World row", format="%.4g"),
                "implied_global_production": st.column_config.NumberColumn("Implied global (Σ)", format="%.4g"),
                "pct_of_world": st.column_config.NumberColumn("% of global total", format="%.2f"),
            },
        )
        if len(iso3s) > 1 and not detail_df.empty:
            with st.expander("Breakdown by country (same commodity lines)"):
                brk = detail_df[
                    [
                        "country_iso3",
                        "commodity",
                        "section",
                        "unit",
                        "production",
                        "global_denominator",
                        "denominator_source",
                        "pct_of_world",
                    ]
                ].copy()
                brk["country"] = brk["country_iso3"].map(country_select_label)
                st.dataframe(
                    brk.sort_values(["commodity", "country"]),
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "production": st.column_config.NumberColumn("Production", format="%.4g"),
                        "global_denominator": st.column_config.NumberColumn("Global total", format="%.4g"),
                        "denominator_source": st.column_config.TextColumn("Denominator source"),
                        "pct_of_world": st.column_config.NumberColumn("% of global total", format="%.2f"),
                    },
                )
        return

    if mode.startswith("Yearbook Table 1"):
        st.caption("`usgs_myb3_production` — melted yearbook Table 1.")
        _iso_opts_myb3 = _all_iso3_for_multiselect()
        _gulf_myb3 = [c for c in GULF_EXPORTER_ISO3_ORDER if c in _iso_opts_myb3]
        iso3s_myb3 = st.multiselect(
            "Countries",
            options=_iso_opts_myb3,
            default=_gulf_myb3,
            format_func=country_select_label,
            help="Rows for all selected countries are combined (charts sum `value_numeric`). Default = Gulf group.",
            key="em_usgs_myb3_countries",
        )
        ref_y = st.number_input(
            "Reference year (from filename)",
            min_value=1990,
            max_value=2030,
            value=2019,
            key="em_usgs_myb3_ref",
        )
        com = st.text_input("Commodity path contains (optional)", value="", key="em_usgs_myb3_com").strip()
        if not iso3s_myb3:
            st.info("Select at least one country (default is the full **Gulf** set).")
            return
        df = load_usgs_myb3_production_slice(tuple(sorted(iso3s_myb3)), int(ref_y), com)
        if df.empty:
            st.warning("No rows for this filter.")
            return
        grp_myb3 = ", ".join(country_select_label(c) for c in iso3s_myb3)
        st.caption(f"**{len(df)}** rows — **{grp_myb3}** · ref year **{ref_y}**.")
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
        num = df.dropna(subset=["value_numeric"])
        if not num.empty:
            by_y = num.groupby("stat_year", as_index=False)["value_numeric"].sum().sort_values("stat_year")
            st.caption("Total `value_numeric` by **stat_year** (summed across selected countries).")
            st.line_chart(by_y.set_index("stat_year")[["value_numeric"]].astype(float), height=320)
        with st.expander("Raw rows"):
            sort_cols = ["country_iso3", "stat_year", "commodity_path"] if "country_iso3" in df.columns else ["stat_year", "commodity_path"]
            st.dataframe(df.sort_values(sort_cols), width="stretch", hide_index=True)
        return

    st.caption("`usgs_country_mineral_facilities` — yearbook Table 2 merged blocks.")
    _iso_opts_fac = _all_iso3_for_multiselect()
    _gulf_fac = [c for c in GULF_EXPORTER_ISO3_ORDER if c in _iso_opts_fac]
    iso3s_fac = st.multiselect(
        "Countries",
        options=_iso_opts_fac,
        default=_gulf_fac,
        format_func=country_select_label,
        help="Facility rows for all selected countries are combined. Default = Gulf group.",
        key="em_usgs_fac_countries",
    )
    ref_y = st.number_input(
        "Reference year",
        min_value=1990,
        max_value=2030,
        value=2019,
        key="em_usgs_fac_ref",
    )
    if not iso3s_fac:
        st.info("Select at least one country (default is the full **Gulf** set).")
        return
    df = load_usgs_facilities_slice(tuple(sorted(iso3s_fac)), int(ref_y))
    if df.empty:
        st.warning("No rows for this filter.")
        return
    grp_fac = ", ".join(country_select_label(c) for c in iso3s_fac)
    st.caption(f"**{len(df)}** rows — **{grp_fac}** · ref year **{ref_y}**.")
    st.markdown("#### Facility map")
    st.caption(
        "When **`geocode_lat` / `geocode_lon`** are populated (batch: **`scripts/geocode_usgs_facilities.py`**), the map "
        "uses those coordinates. Otherwise each point is placed at an **approximate country center** with a small "
        "**deterministic offset** so markers separate. Tooltips show commodity, location, capacity, and geocode "
        "query when present. Rows with neither stored coordinates nor a known country center are skipped."
    )
    map_df, any_nominatim = _usgs_facilities_map_dataframe(df)
    if map_df is None or map_df.empty:
        st.info(
            "No points on the map — run **`uv run python scripts/geocode_usgs_facilities.py`** after applying the "
            "geocode migration, add centers in **`_USGS_FACILITY_COUNTRY_CENTERS`**, or confirm this slice has rows. "
            "The table below still lists all facilities."
        )
    else:
        mid_lat = float(map_df["lat"].median())
        mid_lon = float(map_df["lon"].median())
        lat_span = float(map_df["lat"].max() - map_df["lat"].min())
        lon_span = float(map_df["lon"].max() - map_df["lon"].min())
        span = max(lat_span, lon_span, 1e-6)
        zoom = 4.0 if span > 22 else 5.0 if span > 10 else 6.0 if span > 4 else 7.0
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position=["lon", "lat"],
            get_fill_color=[200, 85, 40, 210],
            pickable=True,
            radius_min_pixels=4,
            radius_max_pixels=16,
        )
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=pdk.ViewState(latitude=mid_lat, longitude=mid_lon, zoom=zoom),
            map_style=pdk.map_styles.CARTO_LIGHT,
            tooltip={"html": "{hover_html}", "style": {"color": "white"}},
            height=520,
        )
        st.pydeck_chart(deck, width="stretch")
        if any_nominatim:
            st.caption(
                "Geocoding: data © [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors; search via "
                "[Nominatim](https://nominatim.org/). Follow usage limits for production workloads."
            )
        _omit = len(df) - len(map_df)
        st.caption(
            f"**{len(map_df)}** facilities on map (of **{len(df)}** loaded)"
            + (f"; **{_omit}** skipped (no coordinates and no map center for ISO3)." if _omit > 0 else ".")
        )
    cap = df.dropna(subset=["capacity_numeric"])
    if not cap.empty:
        st.markdown("**Top facilities by `capacity_numeric` (this slice)**")
        top = cap.nlargest(20, "capacity_numeric").copy()
        top["_lbl"] = (
            top["commodity_leaf_resolved"].astype(str).str.slice(0, 40)
            + " · "
            + top["facility_path"].astype(str).str.slice(0, 36)
        )
        _st_altair_bar_h_by_value(top, "capacity_numeric", "_lbl", x_title="capacity_numeric")
    with st.expander("Raw rows"):
        _sort_fac = [
            c for c in ("country_iso3", "commodity_leaf_resolved", "facility_path") if c in df.columns
        ]
        st.dataframe(
            df.sort_values(_sort_fac) if _sort_fac else df,
            width="stretch",
            hide_index=True,
        )


def explore_gem() -> None:
    st.subheader("GEM tracker rows")
    st.caption(
        "Global Energy Monitor Excel trackers in **`gem_tracker_rows`**: one row per data line; **`payload`** "
        "stores header→value pairs. The grid below flattens shallow keys; use the column filter when sheets are wide."
    )
    how = st.radio(
        "Workbook",
        ("Choose from database", "Type exact filename"),
        horizontal=True,
        key="em_gem_how",
    )
    fn = ""
    if how == "Choose from database":
        files = load_gem_distinct_source_files()
        if not files:
            st.warning(
                "No workbook names found. Load data with `uv run python loaders/load_gem_xlsx.py`, "
                "and ensure migration `20260416_rpc_gem_distinct_source_files.sql` is applied for the dropdown."
            )
            return
        fn = st.selectbox("Source file", files, key="em_gem_fn_sb")
    else:
        fn = st.text_input(
            "Source file (exact)",
            value="",
            placeholder="Global-Integrated-Power-March-2026-II.xlsx",
            key="em_gem_fn_tx",
        ).strip()
        if not fn:
            st.info("Enter the workbook **filename** exactly as in `gem_tracker_rows.source_file`.")
            return

    if fn in DEFAULT_WORKBOOKS:
        st.caption(
            "Sheets in the default bundle for this file: **"
            + "**, **".join(DEFAULT_WORKBOOKS[fn])
            + "** (database may differ if you used custom `--sheets`)."
        )

    sheets = load_gem_sheet_names(fn)
    if not sheets:
        st.warning("No rows for this filename — check spelling or run `load_gem_xlsx.py`.")
        return
    c1, c2 = st.columns([2, 1])
    with c1:
        sheet = st.selectbox("Sheet", sheets, key="em_gem_sheet")
    with c2:
        total_n = load_gem_sheet_row_count(fn, sheet)
        if total_n is not None:
            st.metric("Rows in database (this sheet)", f"{total_n:,}")

    st.markdown("#### Facility map")
    st.caption(
        "2D map (pan/zoom): markers use **latitude / longitude** (or **lat** / **lon** / **long**) from each row’s "
        "**`payload`**. Pipeline and line-only geometry are not drawn — points only."
    )
    map_max = st.slider(
        "Max rows to load for map",
        min_value=1000,
        max_value=50_000,
        value=15_000,
        step=1000,
        key="em_gem_map_max",
        help="Loads rows in database order by `id`. Lower this on slow connections or huge sheets.",
    )
    map_rows = load_gem_payloads_for_sheet(fn, sheet, map_max)
    records, sample_keys = payloads_to_map_records(map_rows)
    if not records:
        st.info(
            "No valid latitude/longitude pairs were found in **`payload`** for the rows loaded above. "
            "GEM column names vary by tracker — open the expander to see **payload** keys from the first row."
        )
        with st.expander("Payload keys (debug)"):
            keys = sample_keys or first_payload_keys(map_rows)
            st.code("\n".join(keys) if keys else "(no non-empty payload keys)")
    else:
        df_map = map_records_to_dataframe(records)
        loaded = len(map_rows)
        st.caption(
            f"Showing **{len(df_map):,}** points with coordinates from **{loaded:,}** rows loaded "
            f"(rows without parsable lat/long are skipped)."
        )
        mid_lat = float(df_map["lat"].median())
        mid_lon = float(df_map["lon"].median())
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_map,
            get_position=["lon", "lat"],
            get_fill_color=[210, 55, 45, 210],
            pickable=True,
            radius_min_pixels=3,
            radius_max_pixels=11,
        )
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=pdk.ViewState(latitude=mid_lat, longitude=mid_lon, zoom=2),
            map_style=pdk.map_styles.CARTO_LIGHT,
            tooltip={
                "html": "<b>GEM id {gem_id}</b><br>{hover_html}",
                "style": {"color": "white"},
            },
            height=560,
        )
        st.pydeck_chart(deck, width="stretch")

    st.divider()

    row_limit = st.slider(
        "Max rows to load (preview)",
        min_value=100,
        max_value=5000,
        value=500,
        step=100,
        key="em_gem_lim",
        help="Caps the Supabase query. Use a smaller slice for very large sheets (e.g. Integrated Power).",
    )
    df = load_gem_rows_slice(fn, sheet, row_limit)
    if df.empty:
        st.warning("No rows for this sheet.")
        return
    shown_note = ""
    if total_n is not None and total_n > len(df):
        shown_note = f" — showing first **{len(df):,}** of **{total_n:,}**"
    st.caption(f"Loaded **{len(df):,}** rows · `{fn}` · `{sheet}`{shown_note}")

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

    f1, f2 = st.columns(2)
    with f1:
        row_q = st.text_input(
            "Filter rows (matches any cell text)",
            value="",
            key="em_gem_row_q",
        ).strip().lower()
    with f2:
        col_q = st.text_input(
            "Column name contains (optional)",
            value="",
            key="em_gem_col_q",
        ).strip().lower()

    view = disp
    if col_q:
        keep = [c for c in view.columns if col_q in str(c).lower()]
        if not keep:
            st.warning("No columns match that name filter; showing all columns.")
        else:
            view = view[keep]
    if row_q:
        blob = view.astype(str).agg(" ".join, axis=1).str.lower()
        view = view.loc[blob.str.contains(row_q, regex=False, na=False)].copy()
        st.caption(f"**{len(view):,}** rows after row filter (from **{len(disp):,}** loaded).")

    csv_bytes = view.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download displayed table as CSV",
        data=csv_bytes,
        file_name=f"gem_{sheet.replace(' ', '_')[:40]}_preview.csv",
        mime="text/csv",
        key="em_gem_dl",
    )
    st.dataframe(view, width="stretch", hide_index=True, height=520)
    with st.expander("First row `payload` (JSON)"):
        p0 = payloads[0] if payloads else None
        if isinstance(p0, dict):
            st.json(p0)
        elif p0 is not None:
            st.code(str(p0))


def explore_gem_infrastructure_map() -> None:
    st.subheader("GEM infrastructure map")
    st.caption(
        "Loads **all rows** from each selected workbook sheet via **keyset pagination** on `id` (avoids slow `OFFSET` scans). "
        "Apply `schema/migrations/20260418_idx_gem_tracker_source_sheet_id.sql` on the database if loads still time out. "
        "Cement, chemicals, iron/steel, and **LNG terminals** are **worldwide** — a **Middle East–only** map hides most of them; "
        "leave **Filter by geographic area** off (default) or select every region. "
        "Large sheets can take a while on first load; results are cached briefly. "
        "**Pipelines** often lack a single lat/long — those rows are skipped. Capacity sums are **best-effort** and **mix units**. "
        "**Colour** encodes facility category (distinct, colour-blind–friendly hues); **shade** varies slightly by detected **subtype** "
        "(from payload columns). **Emoji** in the category picker and tooltips match each category — markers stay as dots for performance."
    )

    pairs = load_gem_all_source_sheet_pairs()
    if not pairs:
        st.warning(
            "No GEM data found. Run `uv run python loaders/load_gem_xlsx.py` after applying the schema."
        )
        return

    cat_labels = sorted({style_for_source_sheet(fn, sn)[0] for fn, sn in pairs})
    default_sel = [x for x in all_known_category_labels() if x in cat_labels]
    if not default_sel:
        default_sel = cat_labels

    sel = st.multiselect(
        "Facility categories",
        options=cat_labels,
        default=default_sel,
        key="gem_map_cats",
        format_func=lambda c: f"{emoji_for_category_label(c)}  {c}",
        help="Each category maps from `(source_file, sheet_name)`; unknown pairs are **Other**. Emoji matches map legend and tooltips.",
    )
    if not sel:
        st.info("Select at least one category.")
        return

    apply_geo = st.checkbox(
        "Filter by geographic area",
        value=False,
        key="gem_map_apply_geo",
        help="When off (default), every geocoded point is shown worldwide. When on, only points inside the selected rough regions (OR).",
    )
    region_sel = st.multiselect(
        "Areas of interest",
        options=list(GEM_REGION_OPTIONS),
        default=list(GEM_REGION_OPTIONS),
        key="gem_map_regions",
        disabled=not apply_geo,
        help="Rough lat/lon boxes. Use all six to approximate worldwide; pick fewer (e.g. Middle East) to narrow.",
    )
    if apply_geo and not region_sel:
        st.info("Select at least one area, or turn off **Filter by geographic area** to show the whole world.")
        return

    to_load = [
        (fn, sn)
        for fn, sn in pairs
        if style_for_source_sheet(fn, sn)[0] in set(sel)
    ]
    if not to_load:
        st.info("No sheets match the selected categories.")
        return

    workers = st.slider(
        "Parallel fetch workers (one task per sheet)",
        min_value=1,
        max_value=12,
        value=min(2, max(1, len(to_load))),
        step=1,
        key="gem_map_workers",
        help="Lower values reduce database load and avoid timeouts; raise only if your project allows heavier parallel scans.",
    )

    st.caption(
        f"Loading **all rows** from **{len(to_load)}** sheet(s) in parallel (up to **{workers}** workers)…"
    )

    def _one(pair: tuple[str, str]) -> list[dict[str, Any]]:
        fn, sn = pair
        return _gem_fetch_pair_payloads_complete(fn, sn)

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        chunks = list(pool.map(_one, to_load))

    merged: list[dict[str, Any]] = [row for part in chunks for row in part]
    st.caption(f"Fetched **{len(merged):,}** raw rows from Supabase (full sheet pagination).")

    raw_by_cat = Counter(style_for_source_sheet(r["source_file"], r["sheet_name"])[0] for r in merged)
    with st.expander("Rows loaded from database, by category", expanded=False):
        st.caption("If a category shows **0** raw rows, that workbook/sheet is not in Supabase — run `load_gem_xlsx.py` for that file.")
        st.dataframe(
            pd.DataFrame(
                [
                    {"emoji": emoji_for_category_label(k), "category": k, "raw_rows": raw_by_cat[k]}
                    for k in sorted(raw_by_cat.keys())
                ]
            ),
            width="stretch",
            hide_index=True,
        )

    records, _sample = payloads_to_map_records_enriched(merged)
    if not records:
        st.info(
            "No latitude/longitude pairs found in the loaded rows. "
            "Pipeline line geometry is not parsed here; plant-heavy sheets should show points when coordinates exist."
        )
        return

    n_geocoded = len(records)
    geo_by_cat = Counter(str(r["category_label"]) for r in records)
    with st.expander("Geocoded points by category (before area filter)", expanded=False):
        st.caption(
            "If raw rows exist but geocoded is **0**, coordinates may use unusual column names — check Explore more → GEM grid. "
            "Sparse categories can sit **under** dense layers on the map; we draw smaller categories on top."
        )
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "emoji": emoji_for_category_label(k),
                        "category": k,
                        "raw_rows": raw_by_cat.get(k, 0),
                        "geocoded_points": geo_by_cat.get(k, 0),
                    }
                    for k in sorted(set(raw_by_cat) | set(geo_by_cat))
                ]
            ),
            width="stretch",
            hide_index=True,
        )

    regs = frozenset(region_sel) if apply_geo else frozenset()
    records_view = [
        r
        for r in records
        if point_in_regions(float(r["lat"]), float(r["lon"]), regs)
    ]
    if not records_view:
        st.warning(
            f"No points fall inside the selected area(s) (**{n_geocoded:,}** geocoded rows loaded). "
            "Add regions or disable **Filter by geographic area**."
        )
        return

    df_map = pd.DataFrame(records_view)
    mid_lat = float(df_map["lat"].median())
    mid_lon = float(df_map["lon"].median())

    sum_rows: list[dict[str, Any]] = []
    for cat in sorted(df_map["category_label"].unique()):
        sub = df_map[df_map["category_label"] == cat]
        row: dict[str, Any] = {
            "emoji": emoji_for_category_label(cat),
            "category": cat,
            "points_on_map": int(len(sub)),
        }
        if "capacity_value" in sub.columns:
            cv = sub["capacity_value"]
            row["rows_with_capacity"] = int(cv.notna().sum())
            row["sum_capacity_indicative"] = float(cv.sum(skipna=True)) if cv.notna().any() else None
        else:
            row["rows_with_capacity"] = 0
            row["sum_capacity_indicative"] = None
        sum_rows.append(row)

    st.markdown("#### Summary")
    geo_note = ", ".join(region_sel) if apply_geo else "worldwide (no geographic filter)"
    st.caption(
        f"**Geocoded:** {n_geocoded:,} · **Shown after area filter:** {len(df_map):,} · **Filter:** {geo_note}. "
        "**sum_capacity_indicative** uses capacity-like columns; units differ — rough scale only."
    )
    sdf = pd.DataFrame(sum_rows)
    st.dataframe(sdf, width="stretch", hide_index=True)

    st.markdown("#### Map")
    st.caption(
        f"**{len(df_map):,}** points · pan/zoom. Tooltip: emoji, category, optional subtype, GEM id, and payload fields. "
        "Dot **colour** = category; **brightness** shifts slightly when a subtype is detected."
    )
    layers: list[pdk.Layer] = []
    # Draw high-volume categories first (bottom); sparse categories last so they are not hidden under e.g. power.
    cats_layer_order = sorted(
        df_map["category_label"].unique(),
        key=lambda c: len(df_map[df_map["category_label"] == c]),
        reverse=True,
    )
    for cat in cats_layer_order:
        sub = df_map[df_map["category_label"] == cat]
        if sub.empty:
            continue
        n_sub = len(sub)
        rmax = 14 if n_sub < 5_000 else 10
        rmin = 4 if n_sub < 5_000 else 2
        # PyDeck JSON serialization breaks on DataFrame + per-row color accessors; use plain dicts + fill_color.
        deck_rows: list[dict[str, Any]] = []
        for rec in sub.to_dict("records"):
            r0, g0, b0, a0 = int(rec["r"]), int(rec["g"]), int(rec["b"]), int(rec["a"])
            row = {**rec, "fill_color": [r0, g0, b0, a0]}
            deck_rows.append(row)
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                deck_rows,
                get_position=["lon", "lat"],
                get_fill_color="fill_color",
                pickable=True,
                radius_min_pixels=rmin,
                radius_max_pixels=rmax,
            )
        )
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=mid_lat, longitude=mid_lon, zoom=2),
        map_style=pdk.map_styles.CARTO_LIGHT,
        tooltip={
            "html": "{category_emoji} <b>{category_label}</b><br>{subtype_line}GEM id {gem_id}<br>{hover_html}",
            "style": {"color": "white"},
        },
        height=580,
    )
    st.pydeck_chart(deck, width="stretch")


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
            "Data dictionary (catalog)",
            "Energy trade (EIA)",
            "Fertilizer (FAOSTAT)",
            "Macro indicators (WDI)",
            "Food balance (FAOSTAT)",
            "HS6 elasticities (CEPII ProTEE)",
            "Import dependence (CEPII GeoDep)",
            "Oil & gas (JODI)",
            "Minerals (USGS)",
            "Infrastructure (GEM)",
            "HS6 lookup (UN Comtrade)",
            "Countries (reference)",
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
        "Charts show the **largest N** exporters/importers by value for that slice — use **Full rankings** below "
        "to see every country in the loaded data. "
        "HS6 labels use **`hs_code_lookup`** — run `pull_comtrade_hs_lookup.py` if descriptions are missing."
    )
    years, years_fb, years_err = rpc_trade_distinct_data_years()
    if years_fb:
        st.warning(
            "Year list uses a **fallback** (the `rpc_trade_distinct_data_years` RPC did not succeed). "
            "Charts may omit years; fix the RPC error below."
        )
        if years_err:
            with st.expander("PostgREST error — how to fix", expanded=True):
                st.code(years_err)
                st.markdown(_rpc_fix_hint_markdown(years_err))
    if not years:
        st.info("No rows in `bilateral_trade`.")
        return
    year = st.selectbox("Year", years, index=len(years) - 1, key="trade_year")
    hs_list, hs_fallback, hs_err = rpc_trade_distinct_hs6_for_year(int(year))
    if hs_fallback:
        st.warning(
            f"HS6 product list uses a **fallback row scan** (capped at **{BILATERAL_DISTINCT_SCAN_CAP:,}** rows for "
            f"**{year}**) — the distinct-HS6 RPC failed or returned nothing usable."
        )
        if hs_err:
            with st.expander("PostgREST error — how to fix", expanded=False):
                st.code(hs_err)
                st.markdown(_rpc_fix_hint_markdown(hs_err))
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
    exp_all = (
        slice_df.groupby("exporter", as_index=False)["_tv"]
        .sum()
        .rename(columns={"exporter": "country", "_tv": "trade_value_usd_thousands"})
        .sort_values("trade_value_usd_thousands", ascending=False)
    )
    imp_all = (
        slice_df.groupby("importer", as_index=False)["_tv"]
        .sum()
        .rename(columns={"importer": "country", "_tv": "trade_value_usd_thousands"})
        .sort_values("trade_value_usd_thousands", ascending=False)
    )
    n_exp = len(exp_all)
    n_imp = len(imp_all)
    chart_max = max(n_exp, n_imp, 1)
    cap = min(WHO_TRADES_CHART_TOP_N_MAX, chart_max)
    top_n = st.slider(
        "Countries per chart (largest trade value first)",
        min_value=1,
        max_value=cap,
        value=min(25, cap),
        step=1,
        key="trade_top_n",
        help="Bars show the top N by total USD thousands for this HS6 (or aggregate) and year. "
        "Open **Full rankings** for the complete list.",
    )
    exp = exp_all.head(int(top_n)).copy()
    imp = imp_all.head(int(top_n)).copy()
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Top {min(top_n, n_exp)} exporters** (by trade value, USD thousands — largest first)")
        if exp.empty:
            st.info("No exporter data.")
        else:
            ex2 = exp.assign(_lbl=_series_country_labels(exp["country"]))
            _st_altair_bar_h_by_value(
                ex2, "trade_value_usd_thousands", "_lbl", x_title="USD thousands"
            )
    with col2:
        st.markdown(f"**Top {min(top_n, n_imp)} importers** (by trade value, USD thousands — largest first)")
        if imp.empty:
            st.info("No importer data.")
        else:
            im2 = imp.assign(_lbl=_series_country_labels(imp["country"]))
            _st_altair_bar_h_by_value(
                im2, "trade_value_usd_thousands", "_lbl", x_title="USD thousands"
            )
    with st.expander("Full rankings — every exporter and importer in this slice", expanded=False):
        st.caption(
            f"**{n_exp}** distinct exporters and **{n_imp}** distinct importers in `bilateral_trade` for this selection."
        )
        fc1, fc2 = st.columns(2)
        with fc1:
            st.markdown("**All exporters** (sortable)")
            if exp_all.empty:
                st.info("None.")
            else:
                show_e = exp_all.assign(
                    label=_series_country_labels(exp_all["country"]),
                )[["country", "label", "trade_value_usd_thousands"]]
                show_e = show_e.rename(
                    columns={
                        "country": "ISO3",
                        "label": "Country",
                        "trade_value_usd_thousands": "USD thousands",
                    }
                )
                st.dataframe(show_e, width="stretch", hide_index=True)
        with fc2:
            st.markdown("**All importers** (sortable)")
            if imp_all.empty:
                st.info("None.")
            else:
                show_i = imp_all.assign(
                    label=_series_country_labels(imp_all["country"]),
                )[["country", "label", "trade_value_usd_thousands"]]
                show_i = show_i.rename(
                    columns={
                        "country": "ISO3",
                        "label": "Country",
                        "trade_value_usd_thousands": "USD thousands",
                    }
                )
                st.dataframe(show_i, width="stretch", hide_index=True)


def tab_country_profile() -> None:
    st.subheader("Country profile")
    st.caption(
        "Per-product import and export totals (BACI, USD thousands) for one year. "
        "Loads **only that country × year** from Supabase (see warning if the row cap is hit)."
    )
    lookup = load_hs_lookup()
    desc_map: dict[str, str] = {}
    if not lookup.empty and "hs6_code" in lookup.columns:
        for _, r in lookup.iterrows():
            code = str(r.get("hs6_code", "")).strip()
            d = r.get("description")
            if code and pd.notna(d):
                desc_map[code] = str(d)
    years, years_fb, years_err = rpc_trade_distinct_data_years()
    if years_fb:
        st.warning("Year list uses a **fallback** (`rpc_trade_distinct_data_years` RPC failed).")
        if years_err:
            with st.expander("PostgREST error — how to fix"):
                st.code(years_err)
                st.markdown(_rpc_fix_hint_markdown(years_err))
    if not years:
        st.info("No rows in `bilateral_trade`.")
        return
    year = st.selectbox("Year", years, index=len(years) - 1, key="prof_year")
    countries, c_trunc, c_err = rpc_trade_distinct_country_iso3_for_year(int(year))
    if c_trunc:
        st.warning(
            f"Country list uses a **fallback row scan** (capped at **{BILATERAL_DISTINCT_SCAN_CAP:,}** rows for "
            f"**{year}**)."
        )
        if c_err:
            with st.expander("PostgREST error — how to fix"):
                st.code(c_err)
                st.markdown(_rpc_fix_hint_markdown(c_err))
    if not countries:
        st.info(f"No bilateral rows for year **{year}**.")
        return
    c1, c2 = st.columns(2)
    with c1:
        country = st.selectbox("Country", countries, format_func=country_select_label, key="prof_country")
    with c2:
        st.caption(f"**Year {year}**")
    st.caption(f"**{country_display_name(country)}** (`{country}`) · **{year}**")
    df = bilateral_rows_country_year(country, year, max_rows=BILATERAL_COUNTRY_PROFILE_MAX_ROWS)
    if len(df) >= BILATERAL_COUNTRY_PROFILE_MAX_ROWS - 10:
        st.warning(
            f"Row cap (**{BILATERAL_COUNTRY_PROFILE_MAX_ROWS:,}** bilateral rows) reached for this country × year — "
            "import/export totals may be **incomplete**. Use the Exporter & partners tab or SQL for very heavy slices."
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

    def _on_crop_pick_change() -> None:
        # New crop → metric/year options change; drop keys so widgets re-init with valid defaults.
        st.session_state.pop("crop_metric", None)
        st.session_state.pop("crop_year", None)

    def _on_crop_metric_change() -> None:
        st.session_state.pop("crop_year", None)

    df = load_crop_production()
    if df.empty:
        st.info("No rows in `crop_production`.")
        return
    crops = sorted(df["crop"].dropna().astype(str).unique().tolist())
    c1, c2, c3 = st.columns(3)
    with c1:
        crop = st.selectbox("Crop", crops, key="crop_pick", on_change=_on_crop_pick_change)
    crop_df = df[df["crop"] == crop]
    metrics_for_crop = sorted(crop_df["metric"].dropna().astype(str).unique().tolist())
    with c2:
        if not metrics_for_crop:
            st.caption("No metrics for this crop.")
            metric = None
        else:
            metric = st.selectbox(
                "Metric",
                metrics_for_crop,
                key="crop_metric",
                on_change=_on_crop_metric_change,
            )
    if metric is None:
        st.warning("No metric rows for this crop.")
        return
    years_avail = sorted(
        crop_df[crop_df["metric"] == metric]["data_year"]
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
    """Gulf ISO3 first when present, then other exporters for that year (prefers RPC; scan may be capped)."""
    scanned, truncated = rpc_trade_distinct_exporters_for_year(int(year))
    gulf_set = set(GULF_EXPORTER_ISO3_ORDER)
    pinned = [g for g in GULF_EXPORTER_ISO3_ORDER if g in scanned]
    tail = sorted(e for e in scanned if e not in gulf_set)
    return pinned + tail, truncated


def tab_exporter_partners() -> None:
    st.subheader("Exporter & partner imports (best fidelity)")
    st.caption(
        "**Step 1–2:** Choose **exporter** and **year**, then browse its exports by HS6 and partner. **Step 3:** "
        "For the chosen partner and product, **full supplier concentration** from BACI "
        "(every exporter → that importer × HS6 × year). Values are **USD thousands**. "
        "Gulf countries are listed first; other exporters appear after if present in your BACI load for that year. "
        "For a full HS6 portfolio for one country, run e.g. "
        "`load_baci.py --all --exporter-full-hs ARE` (repeat `--exporter-full-hs` per country). "
        "For partner supplier tables, add `--importer-full-hs` per partner ISO3."
    )
    # Do not auto-query Supabase when the user lands on this tab.
    refresh_lists = st.button("Refresh exporter/year lists", key="xpd_refresh_lists")

    if refresh_lists or "xpd_exporter_options" not in st.session_state:
        all_exporters, exporters_fallback, exporters_rpc_err = rpc_trade_distinct_exporters()
        if not all_exporters:
            st.info("No exporters found in `bilateral_trade`.")
            return
        exporters_set = set(all_exporters)
        pinned = [g for g in GULF_EXPORTER_ISO3_ORDER if g in exporters_set]
        tail = sorted(e for e in all_exporters if e not in set(pinned))
        st.session_state["xpd_exporter_options"] = pinned + tail
        st.session_state["xpd_exporters_fallback"] = bool(exporters_fallback)
        st.session_state["xpd_exporter_rpc_err"] = exporters_rpc_err
        # Reset year cache when refreshing exporters.
        st.session_state["xpd_years_by_exporter"] = {}
        st.session_state["xpd_years_fallback_by_exporter"] = {}

    exporter_options = list(st.session_state.get("xpd_exporter_options") or [])
    if not exporter_options:
        st.info("No exporters found in `bilateral_trade`.")
        return
    if st.session_state.get("xpd_exporters_fallback"):
        _xpd_err = st.session_state.get("xpd_exporter_rpc_err")
        st.warning(
            (
                f"Exporter list built from the first **{BILATERAL_DISTINCT_SCAN_CAP:,}** rows — "
                "some exporters may be missing. The **`rpc_trade_distinct_exporters`** RPC did not succeed "
                "(see error below). Apply `schema/rpc_trade_dashboards.sql` and "
                "`schema/migrations/20260415_grant_execute_public_rpc_trade.sql`, or use the service role in `.env`."
            )
            + (f"\n\n`{_xpd_err}`" if _xpd_err else "")
        )
        if _xpd_err:
            st.caption(_rpc_fix_hint_markdown(_xpd_err))

    c1, c2 = st.columns(2)
    with c1:
        exporter_iso = st.selectbox(
            "Exporter country",
            exporter_options,
            index=0,
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
def _grpdep_drilldown_ui(
    *,
    year: int,
    group: list[str],
    export_df: pd.DataFrame,
    hs6_options: list[str],
    existing_snapshot: dict[str, Any] | None,
    force_snapshot_refresh: bool,
    imp_limit: int,
) -> None:
    """
    HS6 / importer drill-down only. Nested fragment so changing pickers does not rerun the parent
    fragment’s cache gate (which would drop the main table when cached_ph != ph).
    """
    detail_hs6_raw = st.text_input(
        "Optional HS6 for detail charts (6 digits)",
        value="",
        key="grpdep_detail_hs6",
        help="If a product is missing from the table above (not in the Top-N cut), type its six-digit code here. "
        "Applies to within-group breakdown and importer exposure.",
    )
    detail_hs6 = _grpdep_six_digit_hs(detail_hs6_raw)

    t_exp, t_imp = st.tabs(["Export share of world", "Importer exposure"])

    with t_exp:
        st.markdown("**Export share of world (by HS6)**")
        disp = export_df.copy()
        disp = _merge_hs6_description_column(disp, "hs6_code")
        if "world_exporter_count" in disp.columns:
            st.caption(
                "Tip: check **world_exporter_count**. If it’s very small (e.g. 1), only a subset of world exporters "
                "were loaded for that HS6/year, so “% of world exports” reflects the loaded slice."
            )
        st.dataframe(disp, width="stretch", hide_index=True)
        st.caption(
            "**Top N** above = **N different HS6 products** (one row per product). It is **not** “N rows per exporter country.”"
        )
        with st.expander("Group members × each product in this table (same HS6 list)", expanded=False):
            st.markdown(
                "The ranked table is **aggregate** (group vs world). Here you see **each selected member’s export value** "
                "for **each HS6** in that table — multiple countries per product when they all export it."
            )
            load_mx = st.checkbox(
                "Load member × HS6 detail (one extra query)",
                value=False,
                key="grpdep_load_member_matrix",
            )
            if load_mx:
                codes_raw = [str(x).strip() for x in export_df["hs6_code"].astype(str).tolist() if str(x).strip()]
                codes_use = codes_raw[:GRP_MEMBER_MATRIX_MAX_HS6]
                if len(codes_raw) > GRP_MEMBER_MATRIX_MAX_HS6:
                    st.warning(
                        f"HS6 list truncated from **{len(codes_raw)}** to **{GRP_MEMBER_MATRIX_MAX_HS6}** for this request."
                    )
                try:
                    mx_long = rpc_trade_group_member_exports_for_hs6_list(
                        int(year), list(group), tuple(sorted(set(codes_use)))
                    )
                except Exception as e:
                    st.error(
                        f"Member×HS6 query failed: {e}. Apply `schema/rpc_trade_dashboards.sql` (includes "
                        f"`rpc_trade_group_member_exports_for_hs6_list`) to Supabase."
                    )
                    mx_long = pd.DataFrame()
                if mx_long.empty:
                    st.info("No member×HS6 rows (or RPC not deployed).")
                else:
                    ml = mx_long.copy()
                    ml.insert(2, "exporter_name", _series_country_labels(ml["exporter_iso3"]))
                    st.markdown("**Long format**")
                    st.dataframe(ml, width="stretch", hide_index=True)
                    st.markdown("**Pivot — USD k by member (columns = your group)**")
                    pv = mx_long.pivot_table(
                        index="hs6_code",
                        columns="exporter_iso3",
                        values="export_usd_k",
                        aggfunc="sum",
                    )
                    if not pv.empty:
                        pv = pv.fillna(0.0)
                        group_order = sorted({str(x).upper().strip() for x in group})
                        for g_iso in group_order:
                            if g_iso not in pv.columns:
                                pv[g_iso] = 0.0
                        pv = pv[group_order]
                        pv2 = _merge_hs6_description_column(pv.reset_index(), "hs6_code")
                        st.dataframe(pv2, width="stretch", hide_index=True)

        hs_pick = st.selectbox(
            "Within-group breakdown (pick HS6)",
            hs6_options,
            index=0,
            format_func=hs6_select_label,
            key="grpdep_hs_pick",
        )
        hs_for_breakdown = detail_hs6 if detail_hs6 is not None else str(hs_pick).strip()
        if detail_hs6 is not None and detail_hs6 != str(hs_pick).strip():
            st.caption(f"Using **optional HS6 {hs_for_breakdown}** for the chart (not the dropdown selection).")
        bd = rpc_trade_group_member_breakdown_for_hs6(int(year), hs_for_breakdown, list(group))
        if bd.empty:
            st.warning(
                f"No exporter-side group rows for HS6 **{hs_for_breakdown}** in **{year}** — wrong code, "
                "or no `bilateral_trade` data for this product/year."
            )
        if not bd.empty:
            members_in_data = set(bd["exporter_iso3"].astype(str).str.upper().str.strip())
            group_set = {str(g).upper().strip() for g in group}
            missing = group_set - members_in_data
            if len(group) > 1 and missing:
                st.info(
                    f"In **`bilateral_trade`** (not necessarily in reality), only **{len(members_in_data)}** of "
                    f"**{len(group)}** selected members have exporter rows for this HS6 × year. "
                    f"Missing in DB: **{', '.join(sorted(missing))}**. Charts below use loaded data only."
                )
                with st.expander("Copy-paste: load BACI so this HS6 is represented for all partners", expanded=True):
                    st.code(
                        _grpdep_baci_reload_commands(int(year), hs_for_breakdown, frozenset(missing)),
                        language="bash",
                    )
                    st.caption("Prefer the first command (one `--hs6-codes`) unless you need every HS6 for each country.")
            bd2 = bd.copy()
            bd2["_lbl"] = _series_country_labels(bd2["exporter_iso3"])
            _st_altair_bar_h_by_value(bd2, "export_usd_k", "_lbl", x_title="USD thousands")
            st.dataframe(bd2.drop(columns=["_lbl"], errors="ignore"), width="stretch", hide_index=True)

    with t_imp:
        st.markdown("**Importer exposure (drill-down)**")
        import_hs6_sel = st.selectbox(
            "HS6 for importer exposure",
            hs6_options,
            index=0,
            format_func=hs6_select_label,
            key="grpdep_import_hs6",
        )
        import_hs6 = detail_hs6 if detail_hs6 is not None else str(import_hs6_sel).strip()
        if detail_hs6 is not None and detail_hs6 != str(import_hs6_sel).strip():
            st.caption(f"Using **optional HS6 {import_hs6}** (see field above the tabs).")
        imp_df: pd.DataFrame
        if existing_snapshot and not force_snapshot_refresh:
            snap_params = existing_snapshot.get("params_json") or {}
            snap_hs6 = str(snap_params.get("import_hs6_code") or "").strip()
            if snap_hs6 and snap_hs6 == str(import_hs6).strip():
                imp_df = _snapshot_rows_importer(int(existing_snapshot["id"]), snap_hs6)
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


@st.fragment()
def tab_group_dependencies() -> None:
    st.subheader("Group dependencies")
    st.caption(
        "All numbers come from **one table**: `bilateral_trade`. Postgres aggregates it with filters you set "
        "(year, group ISO3s, HS6 scope). Nothing is computed “off to the side” — if a country × HS6 × year "
        "combination is missing from the table, the filter has nothing to sum (that is a **data load** issue, not a wrong filter)."
    )
    with st.expander("What gets filtered (same table, explicit predicates)", expanded=False):
        st.markdown(
            """
**Source:** `bilateral_trade` — one row per `(exporter, importer, hs6_code, data_year)` with `trade_value_usd` (USD thousands).

**Your controls map to SQL roughly like this:**

- **Year** → `data_year = <Year>`
- **Country group** (export-side) → `exporter IN (<your ISO3 list>)` when summing the group’s exports
- **World denominator** (share of world) → same `hs6_code` and year, **all** exporters in the table for that slice
- **Show top N (slider) / HS6 text** → after metrics are computed for the full universe (below), the table shows the **first N** rows by **combined group exports** (`group_export_usd_k` — sum of your selected countries for that HS6)
- **Importer exposure** → flows where `exporter` is in your group and `importer` is the buyer

So: **the app is already “filter thoroughly per country as you defined”** — the group is your exporter set.  
If e.g. SAU does not appear for HS6 710812, the filtered aggregate is empty for SAU because **there is no row** for that triple in `bilateral_trade` after your BACI load — not because the UI dropped SAU. Reload BACI so those legs exist (`--hs6-codes` for that product, or `--exporter-full-hs` per country). Use **Only HS6 where every selected country has export rows** to hide incomplete product lines until the table is full enough.

**Snapshots** save a prior aggregate so you do not re-query; they still reflect whatever rows existed when you saved.
            """
        )

    # Years + all-time exporter list: two cached RPCs; run in parallel on cold cache to cut first-paint latency.
    with st.spinner("Loading year and country lists…"):
        with ThreadPoolExecutor(max_workers=2) as _pool:
            _f_y = _pool.submit(rpc_trade_distinct_data_years)
            _f_e = _pool.submit(rpc_trade_distinct_exporters)
            years_tup = _f_y.result()
            exporters_seen, used_exporters_fallback, exporters_list_err = _f_e.result()
    years, years_list_fallback, years_list_err = years_tup
    if not years:
        st.info("No rows in `bilateral_trade`.")
        return
    if years_list_fallback:
        st.warning(
            f"Year list built from a capped scan (**{BILATERAL_DISTINCT_SCAN_CAP:,}** rows) — "
            "some years may be missing. Apply `schema/rpc_trade_dashboards.sql` so `rpc_trade_distinct_data_years` is available."
            + (f" ({years_list_err})" if years_list_err else "")
        )

    c1, c2, c3, c4 = st.columns([2.2, 1.1, 1.5, 1.2])
    with c1:
        year = st.selectbox("Year", years, index=len(years) - 1, key="grpdep_year")
    # Gulf ISO3s first (same order as Exporter & partners), then remaining exporters A–Z.
    exporters_set = set(exporters_seen)
    pinned = [g for g in GULF_EXPORTER_ISO3_ORDER if g in exporters_set]
    tail = sorted(e for e in exporters_seen if e not in exporters_set)
    exporter_options = pinned + tail
    gulf_defaults = list(pinned)
    if "grpdep_group" not in st.session_state and gulf_defaults:
        st.session_state["grpdep_group"] = gulf_defaults
    if used_exporters_fallback:
        st.warning(
            (
                f"Exporter options built from a capped scan (**{BILATERAL_DISTINCT_SCAN_CAP:,}** rows) — "
                "some exporters may be missing. The **`rpc_trade_distinct_exporters`** RPC did not succeed."
            )
            + (f" `{exporters_list_err}`" if exporters_list_err else "")
        )
        if exporters_list_err:
            st.caption(_rpc_fix_hint_markdown(exporters_list_err))
    with c1:
        group = st.multiselect(
            "Country group (ISO3)",
            options=exporter_options,
            format_func=country_select_label,
            key="grpdep_group",
        )
    with c2:
        top_n = st.slider(
            "Show top N rows (after ranking)",
            25,
            10000,
            200,
            25,
            key="grpdep_topn",
            help="Ranking: **group_export_usd_k** DESC (total exports of your group for that HS6), then **group_share_pct** DESC. "
            "Returned rows: min(N, 10000). "
            "A full BACI year has on the order of **~5k distinct HS6** in this project’s loads — under 10k — so you can "
            "slide N high to see almost the full ranked list. The cap exists for PostgREST payload size and "
            "`statement_timeout` (120s on the RPC), not because there are only 10k products in the world.",
        )
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
    group_balance_only = st.checkbox(
        "Only HS6 where every selected country has export rows (loaded data)",
        value=False,
        disabled=len(group) <= 1,
        help="Uses `group_exporter_count` from the query vs your group size. Drops lines like 710812 (gold) when "
        "only ARE was loaded with full HS but others are V1-only — not when a country truly exports nothing.",
        key="grpdep_group_balance",
    )
    with st.expander("Heavy compute: run CLI worker (same save as this tab)"):
        st.markdown(
            "For large **Show top N** values, you can run the same snapshot write in a separate process "
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

    group_key = tuple(sorted({str(x).strip().upper() for x in group if str(x).strip()}))
    common_year_tuple, latest_common_year = grpdep_common_years_and_latest(group_key)
    cc1, cc2 = st.columns([4.2, 1.1])
    with cc1:
        if latest_common_year is None:
            st.warning(
                "No single **data_year** has exporter-side `bilateral_trade` rows for **every** selected country. "
                "Reload BACI for the same years across the group, or remove members until the intersection is non-empty."
            )
        else:
            st.caption(
                f"Newest year with data for **all** selected members (as exporters): **{latest_common_year}** "
                f"({len(common_year_tuple)} year(s) in the intersection)."
            )
    with cc2:
        set_common = st.button(
            "Use this year",
            key="grpdep_use_common_year",
            disabled=(latest_common_year is None or latest_common_year not in years),
            help="Sets **Year** to the newest value that appears for every selected country.",
        )
    if set_common and latest_common_year is not None and latest_common_year in years:
        st.session_state["grpdep_year"] = latest_common_year
        st.rerun()

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
        st.info(
            "Showing **persisted snapshot** rows (not a live re-query). After reloading **BACI** or changing "
            "`bilateral_trade`, click **Force recompute** so the UI matches the database."
        )
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

    if (
        len(group) > 1
        and group_balance_only
        and "group_exporter_count" in export_df.columns
    ):
        need = len(group)
        export_df = export_df[export_df["group_exporter_count"].fillna(0).astype(int) >= need]
        if export_df.empty:
            st.warning(
                "No HS6 rows left after **Only HS6 where every selected country has export rows**. "
                "Uncheck that option to see partial coverage, or load BACI so each member has rows for the products you care about."
            )
            return

    hs6_options = export_df["hs6_code"].astype(str).tolist()
    if not hs6_options:
        st.warning("No HS6 codes returned.")
        return

    with st.expander("Definition: what “top N” means here", expanded=False):
        st.markdown(
            """
**Universe (before any “top”):** every HS6 where **at least one** selected group member has positive exports in
`bilateral_trade` for the chosen year, and the row matches **HS6 filter** (digits / description) if you set one.

**Metrics:** for each HS6 in that universe we compute group totals, world totals, `group_share_pct`, within-group
concentration, etc.

**Ranking (what “top” sorts by):** **`group_export_usd_k` descending** — total exports of **all selected group members**
combined for that HS6 (USD thousands in `bilateral_trade`), then **`group_share_pct` descending**, then HS6 code for stable ties.

**Display cut:** you only **see** the first **N** rows after that sort, where **N** = the slider (capped at **10000**
on the server — a guardrail for **response size** and **query time**, not a claim about how many HS6 exist globally).

**How many HS6 are in the data?** In this pipeline, `bilateral_trade` typically holds **low–mid thousands** of distinct
six-digit codes per year after loading (often **~5k–5.5k** for a full-year BACI slice — checked against the live DB).
That is **below** the 10k cap, so setting **Show top N** near the top can return **nearly the entire ranked universe**
for your group. The WCO HS nomenclature has on the order of **~5.2k–5.6k** HS6 codes total; BACI matches that order of magnitude.

Products ranked **below** your chosen N still exist in the database; use **HS6 filter** or **Optional HS6** to target one code.

**Columns:** one row per **HS6**, not per country. **top_group_exporter_iso3** = largest group exporter for that product.
**group_exporter_count** = how many group members have export rows for that HS6.
            """
        )
    _grpdep_drilldown_ui(
        year=int(year),
        group=list(group),
        export_df=export_df,
        hs6_options=hs6_options,
        existing_snapshot=existing,
        force_snapshot_refresh=bool(force),
        imp_limit=int(imp_limit),
    )

    if do_load or force:
        d_sv = _grpdep_six_digit_hs(str(st.session_state.get("grpdep_detail_hs6") or ""))
        imp_st = st.session_state.get("grpdep_import_hs6")
        hs_st = st.session_state.get("grpdep_hs_pick")
        if d_sv:
            import_code = d_sv
        elif imp_st is not None and str(imp_st).strip():
            import_code = str(imp_st).strip()
        elif hs_st is not None and str(hs_st).strip():
            import_code = str(hs_st).strip()
        elif hs6_options:
            import_code = str(hs6_options[0]).strip()
        else:
            import_code = ""
        params_json["import_hs6_code"] = import_code
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
        "Only this section’s code runs when you use its controls (not every section at once)."
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
        elif section == "GEM infrastructure map":
            explore_gem_infrastructure_map()
        elif section == "Explore more":
            tab_explore_more()
    except Exception as e:
        st.error(f"Query failed: {e}")


if __name__ == "__main__":
    main()
