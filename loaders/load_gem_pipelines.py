# ============================================================
# SCRIPT:  load_gem_pipelines.py
# SOURCE:  Global Energy Monitor — pipeline GeoJSON exports (unzipped locally)
# URL:     https://www.globalenergymonitor.org/
# API KEY: not required
# WRITES:  gem_pipeline_segments
# REFRESH: Re-download geojson and re-run; dataset is delete-then-insert.
# NOTES:   Inserts raw GeoJSON geometry into geom_geojson; Postgres trigger materializes PostGIS geom.
# ============================================================

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

SCRIPT_NAME = "load_gem_pipelines"
SOURCE_LABEL = "Global Energy Monitor (GEM) pipelines GeoJSON"

GEM_DIR = _ROOT / "data" / "globalenergymonitor"

DATASETS: dict[str, dict[str, Any]] = {
    "goit_oil_ngl": {
        "path": GEM_DIR
        / "GEM-GOIT-Oil-NGL-Pipelines-2025-03"
        / "GEM-GOIT-Oil-NGL-Pipelines-2025-03.geojson",
        "fuel": "Oil/NGL",
    },
    "ggit_gas": {
        "path": GEM_DIR
        / "GEM-GGIT-Gas-Pipelines-2025-11"
        / "GEM-GGIT-Gas-Pipelines-2025-11.geojson",
        "fuel": "Gas",
    },
}

# Geometry materialization happens in a DB trigger (ST_GeomFromGeoJSON per row).
# Keep batches small to avoid Postgres statement timeouts on PostgREST bulk inserts.
INSERT_BATCH = 5


def _as_text(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _as_float(x: Any) -> float | None:
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return None
    s = str(x).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _pick(props: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in props:
            return props.get(k)
    return None


def _feature_to_row(
    ft: dict[str, Any],
    dataset: str,
    fuel: str,
    pulled_at: str,
) -> dict[str, Any] | None:
    props = ft.get("properties") or {}
    geom = ft.get("geometry")

    row: dict[str, Any] = {
        "dataset": dataset,
        "pulled_at": pulled_at,
        "fuel": fuel,
        "project_id": _as_text(_pick(props, "ProjectID")),
        "pipeline_name": _as_text(_pick(props, "PipelineName")),
        "segment_name": _as_text(_pick(props, "SegmentName")),
        "status": _as_text(_pick(props, "Status")),
        "start_location": _as_text(_pick(props, "StartLocation")),
        "end_location": _as_text(_pick(props, "EndLocation")),
        "countries": _as_text(_pick(props, "Countries", "CountriesOrAreas")),
        "capacity": _as_text(_pick(props, "Capacity")),
        "capacity_units": _as_text(_pick(props, "CapacityUnits")),
        "diameter": _as_text(_pick(props, "Diameter")),
        "length_estimate_km": _as_float(_pick(props, "LengthEstimateKm")),
        "length_known_km": _as_float(_pick(props, "LengthKnownKm")),
        "properties": props,
        "geom_geojson": geom,
    }

    # Keep rows even if geometry is missing/empty; RPC filters geom IS NOT NULL.
    return row


def _load_geojson_rows(path: Path, dataset: str, fuel: str, pulled_at: str) -> list[dict[str, Any]]:
    with path.open("rb") as f:
        data = json.load(f)
    feats = data.get("features") or []
    out: list[dict[str, Any]] = []
    for ft in feats:
        if not isinstance(ft, dict):
            continue
        row = _feature_to_row(ft, dataset=dataset, fuel=fuel, pulled_at=pulled_at)
        if row:
            out.append(row)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Load GEM pipeline GeoJSONs into gem_pipeline_segments (PostGIS).")
    ap.add_argument(
        "--dataset",
        choices=sorted(DATASETS.keys()),
        action="append",
        help="Limit to one dataset. Repeatable. Default: both.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Parse files and print counts; do not touch Supabase.")
    args = ap.parse_args()

    chosen = args.dataset or list(DATASETS.keys())

    pulled_at = datetime.now(timezone.utc).isoformat()

    if args.dry_run:
        client = None
        run_id = None
    else:
        # Import Supabase deps only when we actually write (keeps --dry-run working without supabase installed).
        from utils.pipeline_logger import finish_run, start_run
        from utils.supabase_client import get_client

        try:
            client = get_client()
        except RuntimeError as e:
            print(e, file=sys.stderr)
            return 1
        params: dict[str, Any] = {"datasets": chosen}
        try:
            run_id = start_run(client, SCRIPT_NAME, SOURCE_LABEL, params)
        except Exception as e:
            print(f"Could not log pipeline run: {e}", file=sys.stderr)
            return 1

    total_written = 0
    try:
        for ds in chosen:
            cfg = DATASETS[ds]
            path: Path = cfg["path"]
            fuel: str = cfg["fuel"]
            if not path.is_file():
                msg = f"File not found: {path}"
                if client and run_id is not None:
                    finish_run(client, run_id, total_written, "error", msg)
                print(msg, file=sys.stderr)
                return 1

            rows = _load_geojson_rows(path, dataset=ds, fuel=fuel, pulled_at=pulled_at)
            print(f"{ds}: {path.name} -> {len(rows):,} features")
            if args.dry_run:
                continue

            assert client is not None
            client.table("gem_pipeline_segments").delete().eq("dataset", ds).execute()
            for i in range(0, len(rows), INSERT_BATCH):
                batch = rows[i : i + INSERT_BATCH]
                # Avoid returning huge JSON/geometry payloads on insert (faster, less memory).
                client.table("gem_pipeline_segments").insert(batch, returning="minimal").execute()
                total_written += len(batch)

        if client and run_id is not None:
            from utils.pipeline_logger import finish_run

            finish_run(client, run_id, total_written, "success", None)
        if args.dry_run:
            print("Dry run only; no writes.")
        else:
            print(f"Inserted {total_written:,} rows into gem_pipeline_segments.")
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        if client and run_id is not None:
            from utils.pipeline_logger import finish_run

            finish_run(client, run_id, total_written, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

