"""Map GEM `(source_file, sheet_name)` to display label, PyDeck RGBA, and legend emoji — aligned with `load_gem_xlsx.DEFAULT_WORKBOOKS`."""

from __future__ import annotations

from loaders.load_gem_xlsx import DEFAULT_WORKBOOKS

# (source_file, sheet_name) -> (short_label, rgba, emoji).
# RGBA uses distinct, largely color-blind-friendly hues (Tol-inspired); alpha high for visibility on light basemap.
_GEM_PAIR_STYLES: dict[tuple[str, str], tuple[str, tuple[int, int, int, int], str]] = {
    ("Global-Cement-and-Concrete-Tracker_July-2025.xlsx", "Plant Data"): (
        "Cement & concrete",
        (153, 112, 88, 230),
        "🧱",
    ),
    ("Global-Iron-Ore-Mines-Tracker-August-2025-V1.xlsx", "Main Data"): (
        "Iron ore mines",
        (0, 119, 187, 230),
        "⛏️",
    ),
    (
        "Plant-level-data-Global-Chemicals-Inventory-November-2025-V1.xlsx",
        "Plant data",
    ): (
        "Chemicals",
        (86, 180, 233, 230),
        "🧪",
    ),
    ("Plant-level-data-Global-Iron-and-Steel-Tracker-March-2026-V1.xlsx", "Plant data"): (
        "Iron & steel (plants)",
        (0, 68, 136, 230),
        "🏭",
    ),
    (
        "Plant-level-data-Global-Iron-and-Steel-Tracker-March-2026-V1.xlsx",
        "Plant capacities and status",
    ): (
        "Iron & steel (capacities)",
        (51, 102, 170, 230),
        "📋",
    ),
    ("Plant-level-data-Global-Iron-and-Steel-Tracker-March-2026-V1.xlsx", "Plant production"): (
        "Iron & steel (production)",
        (102, 136, 187, 230),
        "📈",
    ),
    ("GEM-GOIT-Oil-NGL-Pipelines-2025-03.xlsx", "Pipelines"): (
        "Oil & NGL pipelines",
        (213, 94, 0, 230),
        "🛢️",
    ),
    ("GEM-GGIT-LNG-Terminals-2025-09.xlsx", "LNG Terminals"): (
        "LNG terminals",
        (0, 153, 136, 230),
        "🚢",
    ),
    ("GEM-GGIT-Gas-Pipelines-2025-11.xlsx", "Pipelines"): (
        "Gas pipelines",
        (230, 159, 0, 230),
        "🔥",
    ),
    ("Global-Integrated-Power-March-2026-II.xlsx", "Power facilities"): (
        "Power facilities",
        (204, 121, 167, 230),
        "⚡",
    ),
    ("Global-Integrated-Power-March-2026-II.xlsx", "Regions, area, and countries"): (
        "Power (regions)",
        (170, 68, 153, 230),
        "🗺️",
    ),
}

_OTHER_RGBA: tuple[int, int, int, int] = (119, 119, 119, 210)
_OTHER_EMOJI = "📌"

# label -> emoji (for UI when only the label string is available)
_LABEL_TO_EMOJI: dict[str, str] = {}
for _pair, (lab, _rgba, emo) in _GEM_PAIR_STYLES.items():
    _LABEL_TO_EMOJI[lab] = emo


def style_for_source_sheet(
    source_file: str, sheet_name: str
) -> tuple[str, tuple[int, int, int, int], str]:
    """Return `(category_label, rgba, emoji)` for map styling; unknown pairs use **Other**."""
    key = (source_file.strip(), sheet_name.strip())
    if key in _GEM_PAIR_STYLES:
        lab, rgba, emo = _GEM_PAIR_STYLES[key]
        return lab, rgba, emo
    return "Other", _OTHER_RGBA, _OTHER_EMOJI


def emoji_for_category_label(category_label: str) -> str:
    """Stable emoji for multiselect / summary when only the category string is known."""
    return _LABEL_TO_EMOJI.get(str(category_label).strip(), _OTHER_EMOJI)


def default_workbook_sheet_pairs() -> list[tuple[str, str]]:
    """All `(source_file, sheet_name)` pairs from the default GEM bundle (loader contract)."""
    out: list[tuple[str, str]] = []
    for fn, sheets in DEFAULT_WORKBOOKS.items():
        for sn in sheets:
            out.append((fn, sn))
    return out


def all_known_category_labels() -> list[str]:
    """Ordered unique labels from the default bundle map."""
    seen: set[str] = set()
    labels: list[str] = []
    for _, (lab, _, _) in _GEM_PAIR_STYLES.items():
        if lab not in seen:
            seen.add(lab)
            labels.append(lab)
    labels.append("Other")
    return labels
