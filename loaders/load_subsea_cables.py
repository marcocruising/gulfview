# ============================================================
# SCRIPT:  load_subsea_cables.py
# SOURCE:  TeleGeography — Submarine Cable Map (CC BY-NC-SA 3.0)
# URLS:
#   - https://www.submarinecablemap.com/api/v3/cable/cable-geo.json
#   - https://www.submarinecablemap.com/api/v3/landing-point/landing-point-geo.json
#   - https://www.submarinecablemap.com/api/v3/cable/all.json
# WRITES:
#   - subsea_landing_points
#   - subsea_cable_systems
#   - subsea_cable_routes
#   - subsea_cable_landing_points
# REFRESH: Re-run to upsert latest snapshot.
# NOTES:
#   - Streamlit reads only from Supabase; this loader uses service_role.
#   - Geometry columns are optional; loader stores JSON coordinate arrays used by PyDeck.
# ============================================================

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

_ROOT = __import__("pathlib").Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "load_subsea_cables"
SOURCE_LABEL = "TeleGeography Submarine Cable Map"
LICENSE = "CC BY-NC-SA 3.0"

URL_CABLE_GEO = "https://www.submarinecablemap.com/api/v3/cable/cable-geo.json"
URL_LANDING_GEO = (
    "https://www.submarinecablemap.com/api/v3/landing-point/landing-point-geo.json"
)
URL_CABLE_ALL = "https://www.submarinecablemap.com/api/v3/cable/all.json"

# Fallback mirror for routes if the primary endpoint is slow/unreachable in some environments.
URL_CABLE_GEO_FALLBACK = (
    "https://raw.githubusercontent.com/giswqs/geemap/master/examples/data/cable_geo.geojson"
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _chunks(items: list[dict[str, Any]], n: int) -> Iterable[list[dict[str, Any]]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _get_json(session: requests.Session, url: str, *, timeout_sec: float) -> Any:
    r = session.get(url, timeout=timeout_sec, headers={"User-Agent": "hormuz-supply-chain/0.1"})
    r.raise_for_status()
    return r.json()


def _safe_int(x: Any) -> int | None:
    if x in (None, ""):
        return None
    try:
        return int(str(x).strip())
    except Exception:
        return None


def _safe_float(x: Any) -> float | None:
    if x in (None, ""):
        return None
    try:
        return float(str(x).strip().replace(",", ""))
    except Exception:
        return None


def _parse_rfs_year(detail: dict[str, Any]) -> int | None:
    for k in ("rfs_year", "rfs", "ready_for_service", "readyForService", "rfsYear"):
        if k in detail:
            v = detail.get(k)
            # Some payloads may use full dates/strings; keep only the year.
            if isinstance(v, int):
                return v
            s = str(v or "").strip()
            if len(s) >= 4 and s[:4].isdigit():
                return _safe_int(s[:4])
            return _safe_int(s)
    return None


def _parse_length_km(detail: dict[str, Any]) -> float | None:
    for k in ("length_km", "lengthKm", "length", "length_km_estimated", "length_km_est"):
        if k in detail:
            v = detail.get(k)
            f = _safe_float(v)
            if f is not None:
                return f
    # Sometimes a text field like "12,000 km"
    for k in ("length", "cable_length", "cableLength"):
        v = detail.get(k)
        if isinstance(v, str) and "km" in v.lower():
            s = v.lower().replace("km", "").replace(",", "").strip()
            f = _safe_float(s)
            if f is not None:
                return f
    return None


def _parse_owners(detail: dict[str, Any]) -> str | None:
    for k in ("owners", "owner", "cable_owners", "cableOwners"):
        v = detail.get(k)
        if isinstance(v, list):
            out = ", ".join([str(x).strip() for x in v if str(x).strip()])
            return out or None
        if isinstance(v, str):
            s = v.strip()
            return s or None
    return None


def _parse_website(detail: dict[str, Any]) -> str | None:
    for k in ("website", "url", "home_page", "homepage"):
        v = detail.get(k)
        if isinstance(v, str):
            s = v.strip()
            return s or None
    return None


def _extract_landing_point_ids(detail: dict[str, Any]) -> list[str]:
    """
    Best-effort extraction of landing point ids from cable detail JSON.
    We only persist ids that match subsea_landing_points.id.
    """
    for k in ("landing_points", "landingPoints", "landing_points_list", "landingPointsList"):
        v = detail.get(k)
        if isinstance(v, list):
            ids: list[str] = []
            for it in v:
                if isinstance(it, dict):
                    lp_id = it.get("id") or it.get("landing_point_id") or it.get("slug")
                    if isinstance(lp_id, str) and lp_id.strip():
                        ids.append(lp_id.strip())
                elif isinstance(it, str) and it.strip():
                    ids.append(it.strip())
            return ids
    return []


def _fetch_cable_detail(
    session: requests.Session, slug: str, *, timeout_sec: float
) -> dict[str, Any] | None:
    url = f"https://www.submarinecablemap.com/api/v3/cable/{slug}.json"
    try:
        obj = _get_json(session, url, timeout_sec=timeout_sec)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Load TeleGeography submarine cable data into Supabase.")
    ap.add_argument("--timeout-sec", type=float, default=60.0)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("--limit-cables", type=int, default=0, help="0 = no limit (all cables).")
    ap.add_argument(
        "--skip-details",
        action="store_true",
        help="Skip per-cable detail fetch; loads routes + names only.",
    )
    args = ap.parse_args()

    sb = get_client()
    run_id = start_run(
        sb,
        SCRIPT_NAME,
        SOURCE_LABEL,
        {
            "timeout_sec": args.timeout_sec,
            "batch": args.batch,
            "max_workers": args.max_workers,
            "limit_cables": args.limit_cables,
            "skip_details": bool(args.skip_details),
            "urls": {
                "cable_geo": URL_CABLE_GEO,
                "cable_geo_fallback": URL_CABLE_GEO_FALLBACK,
                "landing_geo": URL_LANDING_GEO,
                "cable_all": URL_CABLE_ALL,
            },
            "license": LICENSE,
        },
    )

    rows_written_total = 0
    pulled_at = _utc_now_iso()

    session = requests.Session()
    try:
        # 1) Landing points
        landing_geo = _get_json(session, URL_LANDING_GEO, timeout_sec=float(args.timeout_sec))
        feats = (landing_geo or {}).get("features") or []
        lp_rows: list[dict[str, Any]] = []
        for ft in feats:
            props = (ft or {}).get("properties") or {}
            geom = (ft or {}).get("geometry") or {}
            coords = geom.get("coordinates") or []
            if not (isinstance(coords, list) and len(coords) >= 2):
                continue
            lon, lat = coords[0], coords[1]
            lp_id = props.get("id")
            name = props.get("name")
            if not isinstance(lp_id, str) or not lp_id.strip():
                continue
            if not isinstance(name, str) or not name.strip():
                continue
            lp_rows.append(
                {
                    "id": lp_id.strip(),
                    "name": name.strip(),
                    "is_tbd": bool(props.get("is_tbd", False)),
                    "lon": float(lon),
                    "lat": float(lat),
                    "geom": None,
                    "source": SOURCE_LABEL,
                    "source_url": URL_LANDING_GEO,
                    "license": LICENSE,
                    "pulled_at": pulled_at,
                }
            )
        for part in _chunks(lp_rows, int(args.batch)):
            sb.table("subsea_landing_points").upsert(part, on_conflict="id").execute()
        rows_written_total += len(lp_rows)

        # 2) Cable list (slug -> name)
        cable_all = _get_json(session, URL_CABLE_ALL, timeout_sec=float(args.timeout_sec))
        slug_to_name: dict[str, str] = {}
        if isinstance(cable_all, list):
            for row in cable_all:
                if isinstance(row, dict):
                    slug = row.get("id")
                    name = row.get("name")
                    if isinstance(slug, str) and isinstance(name, str) and slug and name:
                        slug_to_name[slug] = name

        # 3) Cable routes GeoJSON
        try:
            cable_geo = _get_json(session, URL_CABLE_GEO, timeout_sec=float(args.timeout_sec))
            cable_geo_url_used = URL_CABLE_GEO
        except Exception:
            cable_geo = _get_json(session, URL_CABLE_GEO_FALLBACK, timeout_sec=float(args.timeout_sec))
            cable_geo_url_used = URL_CABLE_GEO_FALLBACK

        feats = (cable_geo or {}).get("features") or []
        cable_features: list[dict[str, Any]] = []
        for ft in feats:
            props = (ft or {}).get("properties") or {}
            geom = (ft or {}).get("geometry") or {}
            # TeleGeography endpoint commonly uses `slug`; some mirrors use `id`.
            slug = props.get("slug") or props.get("id")
            if not isinstance(slug, str) or not slug.strip():
                continue
            gtype = geom.get("type")
            if gtype != "MultiLineString":
                continue
            coords = geom.get("coordinates")
            if not isinstance(coords, list) or not coords:
                continue
            cable_features.append(
                {
                    "slug": slug.strip(),
                    "color": (props.get("color") or None),
                    "coords": coords,
                }
            )
        if args.limit_cables and int(args.limit_cables) > 0:
            cable_features = cable_features[: int(args.limit_cables)]

        # 4) Cable details (best-effort, parallel)
        details_by_slug: dict[str, dict[str, Any]] = {}
        landing_ids_by_slug: dict[str, list[str]] = {}
        if not args.skip_details:
            slugs = [c["slug"] for c in cable_features]
            with ThreadPoolExecutor(max_workers=max(1, int(args.max_workers))) as pool:
                futs = {
                    pool.submit(_fetch_cable_detail, session, slug, timeout_sec=float(args.timeout_sec)): slug
                    for slug in slugs
                }
                for fut in as_completed(futs):
                    slug = futs[fut]
                    detail = fut.result()
                    if isinstance(detail, dict) and detail:
                        details_by_slug[slug] = detail
                        landing_ids_by_slug[slug] = _extract_landing_point_ids(detail)

        # 5) Upsert cable systems + routes
        systems_rows: list[dict[str, Any]] = []
        routes_rows: list[dict[str, Any]] = []
        rel_rows: list[dict[str, Any]] = []

        # Deduplicate by slug to avoid PostgREST "row a second time" ON CONFLICT errors
        # when the source GeoJSON contains repeated features for a cable system.
        systems_by_slug: dict[str, dict[str, Any]] = {}
        routes_by_slug: dict[str, dict[str, Any]] = {}

        for cf in cable_features:
            slug = str(cf["slug"])
            detail = details_by_slug.get(slug) or {}
            name = slug_to_name.get(slug) or detail.get("name") or slug
            if not isinstance(name, str):
                name = str(name)

            rfs_year = _parse_rfs_year(detail)
            length_km = _parse_length_km(detail)
            owners = _parse_owners(detail)
            website = _parse_website(detail)

            systems_by_slug[slug] = {
                "slug": slug,
                "name": name.strip() if isinstance(name, str) else slug,
                "rfs_year": rfs_year,
                "length_km": length_km,
                "owners": owners,
                "website": website,
                "source": SOURCE_LABEL,
                "source_url": URL_CABLE_ALL,
                "license": LICENSE,
                "pulled_at": pulled_at,
            }

            # path_coords: list of line strings; each line string is [[lon,lat],...]
            routes_by_slug[slug] = {
                "cable_slug": slug,
                "color": (str(cf.get("color")) if cf.get("color") is not None else None),
                "path_coords": cf["coords"],
                "geom": None,
                "source": SOURCE_LABEL,
                "source_url": cable_geo_url_used,
                "license": LICENSE,
                "pulled_at": pulled_at,
            }

            for i, lp_id in enumerate(landing_ids_by_slug.get(slug) or []):
                if isinstance(lp_id, str) and lp_id.strip():
                    rel_rows.append(
                        {
                            "cable_slug": slug,
                            "landing_point_id": lp_id.strip(),
                            "ordinal": i + 1,
                            "pulled_at": pulled_at,
                        }
                    )

        systems_rows = list(systems_by_slug.values())
        routes_rows = list(routes_by_slug.values())

        for part in _chunks(systems_rows, int(args.batch)):
            sb.table("subsea_cable_systems").upsert(part, on_conflict="slug").execute()
        rows_written_total += len(systems_rows)

        for part in _chunks(routes_rows, int(args.batch)):
            sb.table("subsea_cable_routes").upsert(part, on_conflict="cable_slug").execute()
        rows_written_total += len(routes_rows)

        # Relationships are optional; insert only those that match known landing ids.
        if rel_rows:
            known_lp = set([r["id"] for r in lp_rows])
            rel_rows = [r for r in rel_rows if r.get("landing_point_id") in known_lp]
            # Deduplicate composite key to avoid ON CONFLICT "row a second time".
            rel_by_key: dict[tuple[str, str], dict[str, Any]] = {}
            for r in rel_rows:
                cs = r.get("cable_slug")
                lp = r.get("landing_point_id")
                if isinstance(cs, str) and isinstance(lp, str) and cs and lp:
                    rel_by_key[(cs, lp)] = r
            rel_rows = list(rel_by_key.values())
            for part in _chunks(rel_rows, int(args.batch)):
                sb.table("subsea_cable_landing_points").upsert(
                    part, on_conflict="cable_slug,landing_point_id"
                ).execute()
            rows_written_total += len(rel_rows)

        finish_run(sb, run_id, rows_written_total, "success", None)
        return 0
    except Exception as e:
        finish_run(sb, run_id, rows_written_total, "error", f"{type(e).__name__}: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())

