"""Hormuz supply chain — read-only Supabase explorer."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import streamlit as st

from utils.supabase_client import get_client, get_read_client

TABLES = [
    "pipeline_runs",
    "commodity_prices",
    "energy_trade_flows",
    "bilateral_trade",
    "fertilizer_production",
    "crop_production",
    "hs_code_lookup",
    "country_lookup",
]


def _client():
    try:
        return get_read_client()
    except RuntimeError:
        return get_client()


@st.cache_resource
def supabase():
    return _client()


def main() -> None:
    st.set_page_config(page_title="Hormuz Supply Chain", layout="wide")
    st.title("Hormuz Supply Chain Data Explorer")
    sb = supabase()

    with st.sidebar:
        table = st.selectbox("Table", TABLES)
        limit = st.slider("Max rows", 100, 5000, 1000, 100)

    try:
        res = sb.table(table).select("*").limit(limit).execute()
        rows = res.data or []
    except Exception as e:
        st.error(f"Query failed: {e}")
        return

    st.caption(f"{len(rows)} rows (limit {limit}).")
    if not rows:
        st.info("No rows.")
        return

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if num_cols:
        st.subheader("Numeric summary")
        st.dataframe(df[num_cols].describe().T, use_container_width=True)


if __name__ == "__main__":
    main()
