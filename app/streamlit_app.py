"""Hormuz supply chain — read-only Supabase exploration dashboard."""
from __future__ import annotations

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

from utils.supabase_client import get_client, get_read_client

PAGE_SIZE = 1000


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
    st.altair_chart(chart, use_container_width=True)


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


@st.cache_data(ttl=300, show_spinner="Loading commodity prices…")
def load_commodity_prices() -> pd.DataFrame:
    sb = supabase()
    return pd.DataFrame(fetch_all_pages(sb, "commodity_prices", "*", order_by="id"))


@st.cache_data(ttl=300, show_spinner="Loading bilateral trade…")
def load_bilateral_slim() -> pd.DataFrame:
    sb = supabase()
    cols = "id,exporter,importer,hs6_code,trade_value_usd,data_year"
    return pd.DataFrame(fetch_all_pages(sb, "bilateral_trade", cols, order_by="id"))


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
    st.dataframe(disp[cols], use_container_width=True, hide_index=True)


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
        st.dataframe(show, use_container_width=True, hide_index=True)


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
        st.dataframe(disp, use_container_width=True, hide_index=True)


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
        st.dataframe(sub, use_container_width=True, hide_index=True)


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
            st.dataframe(d0, use_container_width=True, hide_index=True)
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
            st.dataframe(sub, use_container_width=True, hide_index=True)


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
        st.dataframe(disp, use_container_width=True, hide_index=True)


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
        st.dataframe(gd, use_container_width=True, hide_index=True)


def explore_country_lookup() -> None:
    st.subheader("Country lookup (reference)")
    st.caption("Manual / future seed — ISO3 names and Gulf flags.")
    df = load_country_lookup()
    if df.empty:
        st.info("No rows in `country_lookup` yet.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)


def tab_explore_more() -> None:
    st.subheader("Explore more")
    st.caption("Extra datasets with the same selector + chart style as the other tabs.")
    a, b, c, d, e, f, g, h = st.tabs(
        [
            "Data dictionary",
            "Energy (EIA)",
            "Fertilizer",
            "Macro (WDI)",
            "Food balance",
            "ProTEE",
            "GeoDep",
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
        "HS6 labels use **`hs_code_lookup`** — run `pull_comtrade_hs_lookup.py` if descriptions are missing."
    )
    df = load_bilateral_slim()
    if df.empty:
        st.info("No rows in `bilateral_trade`.")
        return
    hs_list = sorted(df["hs6_code"].dropna().astype(str).unique().tolist())
    years = sorted(df["data_year"].dropna().astype(int).unique().tolist())
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
        year = st.selectbox("Year", years, index=len(years) - 1, key="trade_year")
    if view_mode == "Single HS6 product":
        full_hs = hs6_full_description(hs)
        if full_hs:
            st.caption(f"**{hs}** — {full_hs}")
        else:
            st.caption(f"`{hs}` — *(no description in lookup)*")
        slice_df = df[(df["hs6_code"].astype(str) == str(hs)) & (df["data_year"] == year)]
    else:
        q = hs_search.strip()
        if not q and len(hs_filtered) == len(hs_list):
            st.warning(
                "Aggregate mode with an **empty** search would sum **every** HS6 in BACI — add a search "
                "term (e.g. `cereal`, `1001`) or switch to **Single HS6 product**."
            )
            return
        filt_note = repr(q) if q else "codes matched by filter"
        st.caption(
            f"**Aggregate** — summing **{len(hs_filtered)}** HS6 line(s) for **{year}** (search: {filt_note})."
        )
        if len(hs_filtered) > 80:
            with st.expander("HS6 codes included (first 80)"):
                st.write(", ".join(sorted(hs_filtered)[:80]) + (" …" if len(hs_filtered) > 80 else ""))
        slice_df = df[
            (df["hs6_code"].astype(str).isin(hs_filtered)) & (df["data_year"] == year)
        ]
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
    st.caption("Per-product import and export totals (BACI, USD thousands) for one year.")
    df = load_bilateral_slim()
    lookup = load_hs_lookup()
    desc_map: dict[str, str] = {}
    if not lookup.empty and "hs6_code" in lookup.columns:
        for _, r in lookup.iterrows():
            code = str(r.get("hs6_code", "")).strip()
            d = r.get("description")
            if code and pd.notna(d):
                desc_map[code] = str(d)
    if df.empty:
        st.info("No rows in `bilateral_trade`.")
        return
    countries = sorted(
        set(df["exporter"].dropna().astype(str))
        | set(df["importer"].dropna().astype(str))
    )
    years = sorted(df["data_year"].dropna().astype(int).unique().tolist())
    c1, c2 = st.columns(2)
    with c1:
        country = st.selectbox("Country", countries, format_func=country_select_label, key="prof_country")
    with c2:
        year = st.selectbox("Year", years, index=len(years) - 1, key="prof_year")
    st.caption(f"**{country_display_name(country)}** (`{country}`) · **{year}**")
    imp = (
        df[(df["importer"].astype(str) == country) & (df["data_year"] == year)]
        .groupby("hs6_code", as_index=False)["trade_value_usd"]
        .sum()
        .rename(columns={"trade_value_usd": "imports_usd_k"})
    )
    exp = (
        df[(df["exporter"].astype(str) == country) & (df["data_year"] == year)]
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
    st.dataframe(show, use_container_width=True, hide_index=True)


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
    st.dataframe(disp, use_container_width=True, hide_index=True)
    with st.expander("Recent runs (debug)"):
        tail = df.head(50)
        tcols = [c for c in show_cols if c in tail.columns]
        st.dataframe(tail[tcols], use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Hormuz Supply Chain", layout="wide")
    st.title("Hormuz Supply Chain — Data exploration")
    st.sidebar.caption(
        "Supabase: service role if set in .env (avoids empty RLS tables); else anon/publishable key."
    )

    t1, t2, t3, t4, t5, t6 = st.tabs(
        [
            "Prices over time",
            "Who trades what",
            "Country profile",
            "Crop production",
            "Pipeline status",
            "Explore more",
        ]
    )
    try:
        with t1:
            tab_prices()
        with t2:
            tab_who_trades()
        with t3:
            tab_country_profile()
        with t4:
            tab_crop_rank()
        with t5:
            tab_pipeline()
        with t6:
            tab_explore_more()
    except Exception as e:
        st.error(f"Query failed: {e}")


if __name__ == "__main__":
    main()
