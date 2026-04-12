#!/usr/bin/env python3
"""
Batch-geocode USGS myb3 Table 2 facilities via Nominatim (OpenStreetMap).

Respects public instance policy: ~1 req/s, valid User-Agent. Run after loaders/load_usgs.py facilities.

Usage:
  uv run python scripts/geocode_usgs_facilities.py
  uv run python scripts/geocode_usgs_facilities.py --limit 20
  uv run python scripts/geocode_usgs_facilities.py --force --reference-year 2019

Env:
  NOMINATIM_BASE_URL   default https://nominatim.openstreetmap.org
  NOMINATIM_USER_AGENT  default HormuzSupplyChain/1.0 (set a contact URL or email per Nominatim policy)
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pycountry
import requests

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.supabase_client import get_client

DEFAULT_BASE = "https://nominatim.openstreetmap.org"
DEFAULT_UA = os.environ.get(
    "NOMINATIM_USER_AGENT",
    "HormuzSupplyChain/1.0 (USGS geocode batch; +https://github.com/)",
)
MAX_QUERY_LEN = 400
PAGE_SIZE = 200
SOURCE = "nominatim"


def _iso3_to_country_name(iso3: str) -> str:
    iso3 = (iso3 or "").strip().upper()
    if len(iso3) != 3:
        return iso3
    try:
        c = pycountry.countries.get(alpha_3=iso3)
        if c:
            return c.name
    except (LookupError, KeyError, TypeError, AttributeError):
        pass
    return iso3


def _norm(s: object) -> str:
    if s is None:
        return ""
    t = str(s).strip()
    return " ".join(t.split())


def build_geocode_candidates(row: dict[str, Any]) -> list[str]:
    """Ordered query attempts: location+country, owner+location+country, commodity+location+country."""
    iso = _norm(row.get("country_iso3"))
    if len(iso) != 3:
        return []
    cn = _iso3_to_country_name(iso)
    loc = _norm(row.get("location"))
    owner = _norm(row.get("owner_operator"))
    leaf = _norm(row.get("commodity_leaf_resolved"))

    out: list[str] = []
    seen: set[str] = set()

    def add(q: str) -> None:
        q = q.strip()
        if not q or len(q) > MAX_QUERY_LEN:
            return
        if q in seen:
            return
        seen.add(q)
        out.append(q)

    if loc:
        add(f"{loc}, {cn}")
    if owner and loc:
        add(f"{owner}, {loc}, {cn}")
    if leaf and loc:
        add(f"{leaf}, {loc}, {cn}")
    return out


def nominatim_search(
    session: requests.Session,
    base_url: str,
    query: str,
) -> tuple[float, float] | None:
    url = base_url.rstrip("/") + "/search"
    r = session.get(
        url,
        params={
            "q": query,
            "format": "json",
            "limit": 1,
        },
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    first = data[0]
    lat_s = first.get("lat")
    lon_s = first.get("lon")
    if lat_s is None or lon_s is None:
        return None
    try:
        return float(lat_s), float(lon_s)
    except (TypeError, ValueError):
        return None


def geocode_row(
    session: requests.Session,
    base_url: str,
    row: dict[str, Any],
    sleep_sec: float,
) -> tuple[float, float, str] | None:
    cands = build_geocode_candidates(row)
    for q in cands:
        coords = nominatim_search(session, base_url, q)
        time.sleep(sleep_sec)
        if coords:
            lat, lon = coords
            if -85 <= lat <= 85 and -180 <= lon <= 180:
                return lat, lon, q
    return None


def fetch_page(
    sb: Any,
    *,
    offset: int,
    force: bool,
    reference_year: int | None,
) -> list[dict[str, Any]]:
    q = (
        sb.table("usgs_country_mineral_facilities")
        .select(
            "id,record_fingerprint,country_iso3,location,owner_operator,commodity_leaf_resolved,"
            "geocode_lat,geocode_lon"
        )
        .order("id", desc=False)
    )
    if not force:
        q = q.is_("geocode_lat", None)
    if reference_year is not None:
        q = q.eq("reference_year", int(reference_year))
    res = q.range(offset, offset + PAGE_SIZE - 1).execute()
    return list(res.data or [])


def main() -> int:
    ap = argparse.ArgumentParser(description="Geocode usgs_country_mineral_facilities via Nominatim.")
    ap.add_argument("--limit", type=int, default=None, help="Max rows to process (default: no cap).")
    ap.add_argument("--force", action="store_true", help="Re-geocode rows that already have geocode_lat.")
    ap.add_argument("--reference-year", type=int, default=None, help="Only this reference_year.")
    ap.add_argument(
        "--sleep",
        type=float,
        default=1.1,
        help="Seconds between HTTP requests (default 1.1 for public Nominatim).",
    )
    ap.add_argument(
        "--base-url",
        default=os.environ.get("NOMINATIM_BASE_URL", DEFAULT_BASE),
        help="Nominatim API base (default: public instance or NOMINATIM_BASE_URL).",
    )
    args = ap.parse_args()
    base_url = str(args.base_url).rstrip("/")

    sb = get_client()
    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_UA})

    processed = 0
    ok = 0
    fail = 0
    offset = 0

    while True:
        rows = fetch_page(sb, offset=offset, force=args.force, reference_year=args.reference_year)
        if not rows:
            break
        for row in rows:
            if args.limit is not None and processed >= args.limit:
                print(f"Stopped at --limit {args.limit}. ok={ok} fail={fail}", flush=True)
                return 0
            processed += 1
            rid = row.get("id")
            if rid is None:
                fail += 1
                time.sleep(args.sleep)
                continue
            result = geocode_row(session, base_url, row, sleep_sec=args.sleep)
            if result:
                lat, lon, qwin = result
                now = datetime.now(timezone.utc).isoformat()
                sb.table("usgs_country_mineral_facilities").update(
                    {
                        "geocode_lat": lat,
                        "geocode_lon": lon,
                        "geocode_query": qwin,
                        "geocode_source": SOURCE,
                        "geocoded_at": now,
                    }
                ).eq("id", int(rid)).execute()
                ok += 1
                print(f"id={rid} OK lat={lat:.5f} lon={lon:.5f} q={qwin[:80]!r}", flush=True)
            else:
                fail += 1
                print(f"id={rid} FAIL (no Nominatim result)", flush=True)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    print(f"Done. processed={processed} ok={ok} fail={fail}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
