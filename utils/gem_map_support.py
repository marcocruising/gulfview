"""GEM `payload` → lat/lon + hover text for PyDeck map layers."""

from __future__ import annotations

import hashlib
import html
import math
import re
from collections import Counter
from typing import Any

import pandas as pd

from utils.gem_facility_categories import style_for_source_sheet
from utils.gem_regions import point_in_regions

def _is_geo_column(nk: str) -> bool:
    if nk in ("latitude", "longitude", "lat", "lon", "lng", "long", "x", "y"):
        return True
    if "latitude" in nk or "longitude" in nk:
        return True
    return False


# Hover: prefer columns whose normalized names match these substrings (order = priority within tier).
_HOVER_PRIORITY: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Name", ("name", "plant name", "facility name", "project name", "unit name", "station")),
    ("Facility / plant", ("facility", "plant", "project", "unit")),
    ("Country / area", ("country", "region", "area")),
    ("Status", ("status", "stage", "operating")),
    ("Capacity", ("capacity", "mw", "mt", "production")),
    ("Fuel / tech", ("fuel", "technology", "type")),
)


def _norm_key(k: object) -> str:
    return re.sub(r"\s+", " ", str(k).strip().lower())


def _parse_two_floats_from_text(val: Any) -> tuple[float | None, float | None]:
    """GEM sometimes stores `lat, lon` in one cell (comma / semicolon / slash)."""
    if val is None:
        return None, None
    s = str(val).strip().replace("°", " ")
    if not s or ("," not in s and ";" not in s and "/" not in s and "|" not in s):
        return None, None
    parts = re.split(r"[,;/|]", s)
    nums: list[float] = []
    for part in parts:
        f = _as_float(part)
        if f is not None:
            nums.append(f)
        if len(nums) >= 2:
            break
    if len(nums) < 2:
        return None, None
    a, b = nums[0], nums[1]
    if -90.0 <= a <= 90.0 and -180.0 <= b <= 180.0:
        return a, b
    if -90.0 <= b <= 90.0 and -180.0 <= a <= 180.0:
        return b, a
    return None, None


def _as_float(x: Any) -> float | None:
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        v = float(x)
        return v
    try:
        s = str(x).strip().replace(",", "")
        if not s:
            return None
        v = pd.to_numeric(s, errors="coerce")
        if pd.isna(v):
            return None
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def extract_lat_lon(payload: dict[str, Any] | None) -> tuple[float | None, float | None]:
    """Read shallow keys only; common GEM headers: Latitude/Longitude, lat/lon."""
    if not isinstance(payload, dict) or not payload:
        return None, None

    nk_map: dict[str, tuple[str, Any]] = {}
    for k, v in payload.items():
        nk = _norm_key(k)
        if nk and nk not in nk_map:
            nk_map[nk] = (str(k), v)

    lat: float | None = None
    lon: float | None = None

    for key in ("latitude", "lat"):
        if key in nk_map:
            tv = _as_float(nk_map[key][1])
            if tv is not None and -90.0 <= tv <= 90.0:
                lat = tv
                break

    if lat is None:
        for nk, (_, val) in nk_map.items():
            if "longitude" in nk or nk in ("lng", "lon") or nk == "long":
                continue
            if nk == "latitude" or nk.endswith(" latitude") or nk.startswith("latitude "):
                tv = _as_float(val)
                if tv is not None and -90.0 <= tv <= 90.0:
                    lat = tv
                    break
            elif nk == "lat" or nk.endswith(" lat") or nk.startswith("lat "):
                tv = _as_float(val)
                if tv is not None and -90.0 <= tv <= 90.0:
                    lat = tv
                    break

    for key in ("longitude", "long", "lng", "lon"):
        if key in nk_map:
            tv = _as_float(nk_map[key][1])
            if tv is not None and -180.0 <= tv <= 180.0:
                lon = tv
                break

    if lon is None:
        for nk, (_, val) in nk_map.items():
            if nk == "longitude" or nk.endswith(" longitude") or nk.startswith("longitude "):
                tv = _as_float(val)
                if tv is not None and -180.0 <= tv <= 180.0:
                    lon = tv
                    break
            elif nk in ("long", "lng", "lon"):
                tv = _as_float(val)
                if tv is not None and -180.0 <= tv <= 180.0:
                    lon = tv
                    break

    if lat is None or lon is None:
        for nk, (_, val) in nk_map.items():
            tnk = nk.replace(" ", "")
            if "coordinate" not in nk and "gps" not in nk and "lat/long" not in tnk:
                continue
            la, lo = _parse_two_floats_from_text(val)
            if la is not None and lo is not None:
                lat, lon = la, lo
                break

    return lat, lon


# Summary stats: best-effort numeric capacity from shallow keys (units vary by tracker).
_CAPACITY_NEEDLES: tuple[str, ...] = (
    "nameplate",
    "capacity",
    "installed capacity",
    "production capacity",
    "annual production",
    "throughput",
    "mtpa",
    "mw",
    " mwe",
    "production",
)


def extract_subtype(
    payload: dict[str, Any] | None,
    source_file: str,
    sheet_name: str,
) -> str | None:
    """
    Best-effort subtype string from payload columns (tracker-specific column name needles).
    Used for tooltip text and subtle per-point color modulation within a category.
    """
    if not isinstance(payload, dict) or not payload:
        return None

    sf = source_file.lower()
    sn = sheet_name.lower()

    nk_map: dict[str, tuple[str, Any]] = {}
    for k, v in payload.items():
        nk = _norm_key(k)
        if nk:
            nk_map[nk] = (str(k), v)

    if "integrated-power" in sf:
        needles = (
            "technology",
            "fuel",
            "energy source",
            "primary fuel",
            "fuel source",
            "type",
        )
    elif "lng" in sf and "terminal" in sf:
        needles = ("facility type", "terminal type", "asset type", "type", "category")
    elif "pipeline" in sn or "goit" in sf or "ggit" in sf:
        needles = ("product type", "pipeline type", "commodity", "fluid", "type", "service")
    elif "cement" in sf or "concrete" in sf:
        needles = ("plant type", "facility type", "type", "cement type", "production type")
    elif "chemical" in sf:
        needles = ("sector", "subsector", "plant type", "type", "segment")
    elif "iron-ore" in sf or "iron ore" in sf:
        needles = ("mine type", "type", "commodity", "deposit type")
    elif "iron-and-steel" in sf or "iron and steel" in sf:
        needles = (
            "plant type",
            "facility type",
            "technology",
            "type",
            "production process",
            "route",
        )
    else:
        needles = (
            "technology",
            "fuel",
            "type",
            "facility type",
            "plant type",
            "category",
            "subtype",
        )

    for needle in needles:
        for nk, (_orig, val) in nk_map.items():
            if _is_geo_column(nk):
                continue
            if needle not in nk:
                continue
            s = _fmt_cell(val)
            if not s or len(s) > 200:
                continue
            return s[:180]
    return None


def _subtype_shade_rgba(
    rgba: tuple[int, int, int, int],
    subtype: str | None,
) -> tuple[int, int, int, int]:
    """Slight RGB scaling from deterministic hash of subtype (same category hue, distinguishable stacks)."""
    r, g, b, a = rgba
    if not subtype or not str(subtype).strip():
        return rgba
    raw = str(subtype).strip().encode("utf-8")
    h = hashlib.sha256(raw).digest()
    v = int.from_bytes(h[:2], "big") / 65535.0
    factor = 0.88 + v * 0.24
    out_r = max(0, min(255, int(round(r * factor))))
    out_g = max(0, min(255, int(round(g * factor))))
    out_b = max(0, min(255, int(round(b * factor))))
    return out_r, out_g, out_b, a


def extract_capacity_for_summary(payload: dict[str, Any] | None) -> tuple[float | None, str | None]:
    """
    Return `(numeric_value, original_column_name)` for aggregate hints.
    Picks the first matching column (priority order) with a parsable number; **units are not normalized**.
    """
    if not isinstance(payload, dict) or not payload:
        return None, None

    nk_map: dict[str, tuple[str, Any]] = {}
    for k, v in payload.items():
        nk = _norm_key(k)
        if nk:
            nk_map[nk] = (str(k), v)

    for needle in _CAPACITY_NEEDLES:
        for nk, (orig, val) in nk_map.items():
            if _is_geo_column(nk):
                continue
            if needle not in nk:
                continue
            f = _as_float(val)
            if f is None:
                continue
            return f, orig

    return None, None


def build_hover_html(payload: dict[str, Any]) -> str:
    """Up to ~6 short lines for map tooltip (HTML-safe)."""
    if not isinstance(payload, dict) or not payload:
        return ""

    nk_map: dict[str, tuple[str, Any]] = {}
    for k, v in payload.items():
        nk = _norm_key(k)
        if nk:
            nk_map[nk] = (str(k), v)

    lines: list[str] = []
    used: set[str] = set()

    for label, needles in _HOVER_PRIORITY:
        if len(lines) >= 6:
            break
        for nk, (orig, val) in nk_map.items():
            if nk in used or _is_geo_column(nk):
                continue
            if not any(n in nk for n in needles):
                continue
            s = _fmt_cell(val)
            if not s:
                continue
            used.add(nk)
            lines.append(f"<b>{html.escape(label)}</b>: {html.escape(s)}")
            break

    if len(lines) < 6:
        for nk, (orig, val) in nk_map.items():
            if len(lines) >= 6:
                break
            if nk in used or _is_geo_column(nk):
                continue
            s = _fmt_cell(val)
            if not s:
                continue
            used.add(nk)
            lines.append(f"<b>{html.escape(orig)}</b>: {html.escape(s)}")

    return "<br>".join(lines[:6])


def _fmt_cell(val: Any) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return ""
    s = str(val).strip()
    if len(s) > 200:
        s = s[:197] + "…"
    return s


def first_payload_keys(rows: list[dict[str, Any]], *, payload_key: str = "payload") -> list[str]:
    """Sorted key names from the first row with a non-empty dict payload (for debugging)."""
    for row in rows:
        p = row.get(payload_key)
        if isinstance(p, dict) and p:
            return sorted(str(k) for k in p.keys())
    return []


def payloads_to_map_records(
    rows: list[dict[str, Any]],
    *,
    id_key: str = "id",
    payload_key: str = "payload",
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Build PyDeck-ready dicts with lon, lat, hover_html, gem_id.
    Returns (records, sample_payload_keys_from_first_row).
    """
    sample_keys: list[str] = []
    out: list[dict[str, Any]] = []

    for i, row in enumerate(rows):
        rid = row.get(id_key)
        p = row.get(payload_key)
        if i == 0 and isinstance(p, dict) and p:
            sample_keys = sorted(str(k) for k in p.keys())

        lat, lon = extract_lat_lon(p if isinstance(p, dict) else None)
        if lat is None or lon is None:
            continue

        h = build_hover_html(p if isinstance(p, dict) else {})
        if not h and isinstance(p, dict):
            h = "<br>".join(
                f"<b>{html.escape(str(k))}</b>: {html.escape(_fmt_cell(v))}"
                for k, v in list(p.items())[:4]
            )
        out.append(
            {
                "lon": lon,
                "lat": lat,
                "hover_html": h or f"id={rid}",
                "gem_id": rid,
            }
        )

    return out, sample_keys


def payloads_to_map_records_enriched(
    rows: list[dict[str, Any]],
    *,
    id_key: str = "id",
    payload_key: str = "payload",
    source_file_key: str = "source_file",
    sheet_name_key: str = "sheet_name",
    geo_regions: frozenset[str] = frozenset(),
) -> tuple[list[dict[str, Any]], list[str], Counter[str]]:
    """
    Like `payloads_to_map_records` but adds `category_label`, `category_emoji`, `subtype_line` (HTML),
    `r`, `g`, `b`, `a` (subtype-shaded within category), optional `capacity_value` / `capacity_column`.
    Skips rows without coordinates.

    When `geo_regions` is non-empty, rows whose (lat, lon) fall outside those rough regions are skipped
    **before** hover/subtype/capacity work — large speedup for regional maps (e.g. Middle East only).
    `geo_by_cat_all` still counts every geocoded row by category (worldwide), for UI summaries.
    """
    sample_keys: list[str] = []
    out: list[dict[str, Any]] = []
    geo_by_cat_all: Counter[str] = Counter()

    for i, row in enumerate(rows):
        rid = row.get(id_key)
        p = row.get(payload_key)
        sf = str(row.get(source_file_key, "") or "").strip()
        sn = str(row.get(sheet_name_key, "") or "").strip()

        if i == 0 and isinstance(p, dict) and p:
            sample_keys = sorted(str(k) for k in p.keys())

        lat, lon = extract_lat_lon(p if isinstance(p, dict) else None)
        if lat is None or lon is None:
            continue

        label, rgba, emoji = style_for_source_sheet(sf, sn)
        geo_by_cat_all[str(label)] += 1

        if geo_regions and not point_in_regions(float(lat), float(lon), geo_regions):
            continue

        h = build_hover_html(p if isinstance(p, dict) else {})
        if not h and isinstance(p, dict):
            h = "<br>".join(
                f"<b>{html.escape(str(k))}</b>: {html.escape(_fmt_cell(v))}"
                for k, v in list(p.items())[:4]
            )

        sub = extract_subtype(p if isinstance(p, dict) else None, sf, sn)
        rgba2 = _subtype_shade_rgba(rgba, sub)
        if sub:
            sub_esc = html.escape(sub)
            subtype_line = f"<b>Subtype</b>: {sub_esc}<br>"
        else:
            subtype_line = ""

        cap_v, cap_col = extract_capacity_for_summary(p if isinstance(p, dict) else None)

        rec: dict[str, Any] = {
            "lon": lon,
            "lat": lat,
            "hover_html": h or f"id={rid}",
            "gem_id": rid,
            "category_label": label,
            "category_emoji": emoji,
            "subtype_line": subtype_line,
            "r": rgba2[0],
            "g": rgba2[1],
            "b": rgba2[2],
            "a": rgba2[3],
            "source_file": sf,
            "sheet_name": sn,
        }
        if cap_v is not None:
            rec["capacity_value"] = cap_v
        if cap_col:
            rec["capacity_column"] = cap_col
        out.append(rec)

    return out, sample_keys, geo_by_cat_all


def map_records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(records) if records else pd.DataFrame(columns=["lon", "lat", "hover_html", "gem_id"])
