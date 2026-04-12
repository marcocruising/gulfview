"""Rough WGS84 bounding boxes for GEM map “area of interest” filters (OR logic)."""

from __future__ import annotations

# Display order in the UI
GEM_REGION_OPTIONS: tuple[str, ...] = (
    "North America",
    "South America",
    "Africa",
    "Europe",
    "Asia & Oceania",
    "Middle East",
)

# Each region: list of (lat_min, lat_max, lon_min, lon_max). Overlap between regions is OK (OR logic).
_GEM_REGION_BBOXES: dict[str, list[tuple[float, float, float, float]]] = {
    "Middle East": [
        # Western edge ~26°E to include Egypt / Red Sea coast; not exact political borders.
        (12.0, 43.0, 26.0, 63.0),
    ],
    "North America": [
        (7.0, 83.0, -169.0, -15.0),
    ],
    "South America": [
        (-56.0, 14.0, -82.0, -34.0),
    ],
    "Africa": [
        (-35.5, 38.0, -20.0, 52.0),
    ],
    "Europe": [
        (35.0, 72.0, -25.0, 40.0),
    ],
    "Asia & Oceania": [
        (-50.0, 55.0, 60.0, 180.0),
        (-48.0, -8.0, 110.0, 155.0),
        (-48.0, -33.5, 165.0, 180.0),
        (-12.0, 25.0, 95.0, 155.0),
    ],
}


def _in_bbox(lat: float, lon: float, lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> bool:
    if lat < lat_min or lat > lat_max:
        return False
    return lon_min <= lon <= lon_max


def point_in_regions(lat: float, lon: float, selected: frozenset[str]) -> bool:
    """True if `(lat, lon)` lies in **any** bbox of **any** selected region name."""
    if not selected:
        return True
    for name in selected:
        boxes = _GEM_REGION_BBOXES.get(name)
        if not boxes:
            continue
        for lat_min, lat_max, lon_min, lon_max in boxes:
            if _in_bbox(lat, lon, lat_min, lat_max, lon_min, lon_max):
                return True
    return False
