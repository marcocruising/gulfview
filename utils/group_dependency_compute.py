"""Shared logic for group-dependency snapshots (Streamlit + CLI worker)."""
from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd
from supabase import Client


def canonical_params_json(params: dict[str, Any]) -> str:
    """Stable JSON used to compute a deterministic params_hash."""
    return json.dumps(params, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def params_hash(params: dict[str, Any]) -> str:
    payload = canonical_params_json(params).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def snapshot_by_hash(sb: Client, params_hash_val: str) -> dict[str, Any] | None:
    res = (
        sb.table("trade_group_dependency_snapshots")
        .select("*")
        .eq("params_hash", str(params_hash_val))
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def rpc_trade_group_world_share_by_hs6(
    sb: Client,
    year: int,
    group_iso3: list[str],
    hs_query_text: str,
    limit_n: int,
) -> pd.DataFrame:
    res = (
        sb.rpc(
            "rpc_trade_group_world_share_by_hs6",
            {
                # Must match Postgres parameter names (p_* avoids PostgREST / column ambiguity).
                "p_data_year": int(year),
                "group_iso3": [str(x).strip().upper() for x in group_iso3 if str(x).strip()],
                "hs_query_text": hs_query_text,
                "limit_n": int(limit_n),
            },
        )
        .execute()
    )
    return pd.DataFrame(res.data or [])


def rpc_trade_group_importer_exposure_for_hs6(
    sb: Client,
    year: int,
    hs6_code: str,
    group_iso3: list[str],
    limit_n: int,
) -> pd.DataFrame:
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


def write_snapshot_and_rows(
    sb: Client,
    *,
    params_json: dict[str, Any],
    export_rows: pd.DataFrame,
    importer_rows: pd.DataFrame,
    force_recompute: bool,
) -> tuple[int, str]:
    ph = params_hash(params_json)
    existing = snapshot_by_hash(sb, ph)
    row_counts = {
        "export_world_share": int(export_rows.shape[0]) if isinstance(export_rows, pd.DataFrame) else 0,
        "importer_exposure": int(importer_rows.shape[0]) if isinstance(importer_rows, pd.DataFrame) else 0,
    }
    now_iso = pd.Timestamp.utcnow().isoformat()

    if existing and force_recompute:
        snapshot_id = int(existing["id"])
        sb.table("trade_group_dependency_rows").delete().eq("snapshot_id", snapshot_id).execute()
        sb.table("trade_group_dependency_snapshots").update(
            {
                "computed_at": now_iso,
                "started_at": now_iso,
                "data_year": int(params_json["data_year"]),
                "group_iso3": params_json["group_iso3"],
                "params_json": params_json,
                "status": "success",
                "row_counts": row_counts,
                "error_message": None,
            }
        ).eq("id", snapshot_id).execute()
    elif existing and not force_recompute:
        snapshot_id = int(existing["id"])
        return snapshot_id, ph
    else:
        ins = (
            sb.table("trade_group_dependency_snapshots")
            .insert(
                {
                    "data_year": int(params_json["data_year"]),
                    "group_iso3": params_json["group_iso3"],
                    "params_json": params_json,
                    "params_hash": ph,
                    "status": "success",
                    "row_counts": row_counts,
                    "started_at": now_iso,
                    "computed_at": now_iso,
                    "error_message": None,
                }
            )
            .execute()
        )
        snapshot_id = int((ins.data or [{}])[0].get("id"))

    rows_payload: list[dict[str, Any]] = []
    if not export_rows.empty:
        for _, r in export_rows.iterrows():
            rows_payload.append(
                {
                    "snapshot_id": snapshot_id,
                    "view_type": "export_world_share",
                    "hs6_code": str(r.get("hs6_code", "")).strip(),
                    "group_export_usd_k": r.get("group_export_usd_k"),
                    "world_export_usd_k": r.get("world_export_usd_k"),
                    "world_exporter_count": r.get("world_exporter_count"),
                    "group_share_pct": r.get("group_share_pct"),
                    "top_group_exporter_iso3": r.get("top_group_exporter_iso3"),
                    "top_group_exporter_share_pct": r.get("top_group_exporter_share_pct"),
                    "group_member_hhi": r.get("group_member_hhi"),
                    "group_exporter_count": r.get("group_exporter_count"),
                }
            )
    if not importer_rows.empty:
        hs6 = str(params_json.get("import_hs6_code") or "").strip()
        for _, r in importer_rows.iterrows():
            rows_payload.append(
                {
                    "snapshot_id": snapshot_id,
                    "view_type": "importer_exposure",
                    "hs6_code": hs6,
                    "importer_iso3": str(r.get("importer_iso3", "")).strip(),
                    "importer_total_import_usd_k": r.get("importer_total_import_usd_k"),
                    "imports_from_group_usd_k": r.get("imports_from_group_usd_k"),
                    "exposure_pct": r.get("exposure_pct"),
                    "supplier_total_hhi": r.get("supplier_total_hhi"),
                    "supplier_cr1_pct": r.get("supplier_cr1_pct"),
                    "supplier_cr3_pct": r.get("supplier_cr3_pct"),
                    "group_supplier_hhi": r.get("group_supplier_hhi"),
                    "group_supplier_cr1_pct": r.get("group_supplier_cr1_pct"),
                }
            )
    if rows_payload:
        chunk = 500
        for i in range(0, len(rows_payload), chunk):
            sb.table("trade_group_dependency_rows").insert(rows_payload[i : i + chunk]).execute()

    return snapshot_id, ph


def compute_export_and_importer_frames(
    sb: Client,
    *,
    data_year: int,
    group_iso3: list[str],
    hs_query_text: str,
    limit_n_hs6: int,
    import_hs6_code: str,
    limit_n_importers: int,
    coverage_only: bool,
    min_world_exporters: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run RPCs and optional coverage filter; importer frame for one HS6."""
    export_df = rpc_trade_group_world_share_by_hs6(
        sb, int(data_year), list(group_iso3), str(hs_query_text).strip(), int(limit_n_hs6)
    )
    if coverage_only and not export_df.empty and "world_exporter_count" in export_df.columns:
        export_df = export_df[export_df["world_exporter_count"].fillna(0).astype(int) >= min_world_exporters]
    imp_df = rpc_trade_group_importer_exposure_for_hs6(
        sb,
        int(data_year),
        str(import_hs6_code).strip(),
        list(group_iso3),
        int(limit_n_importers),
    )
    return export_df, imp_df
