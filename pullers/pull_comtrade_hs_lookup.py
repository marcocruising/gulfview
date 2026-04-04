# ============================================================
# SCRIPT:  pull_comtrade_hs_lookup.py
# SOURCE:  UN Comtrade (reference HS classification)
# URL:     https://comtrade.un.org/data/cache/classificationHS.json
# API KEY: not required (public JSON cache)
# WRITES:  hs_code_lookup
# REFRESH: occasional (re-run if Comtrade republishes the reference file)
# NOTES:   English descriptions from "HS (as reported)" tree. HS6 codes only.
#          `category` is set when the code matches V1 scope prefixes (same as BACI loader).
# ============================================================

# --- CONFIGURATION — edit these values before running --------
# Longest prefixes first so 3102 wins over 31 for overlapping logic (not needed for current list).
HS_PREFIX_CATEGORY: list[tuple[str, str]] = [
    ("2709", "energy"),
    ("2711", "energy"),
    ("2710", "energy"),
    ("2814", "fertilizer_input"),
    ("3105", "fertilizer"),
    ("3104", "fertilizer"),
    ("3103", "fertilizer"),
    ("3102", "fertilizer"),
    ("1001", "crop"),
    ("1006", "crop"),
    ("1005", "crop"),
    ("1201", "crop"),
    ("5201", "crop"),
]
HS_CODES = ["270900", "271100"]
COUNTRIES = None
CLASSIFICATION_HS_JSON_URL = "https://comtrade.un.org/data/cache/classificationHS.json"
NOTES_PROVENANCE = "UN Comtrade classificationHS.json (HS as reported)."
# -------------------------------------------------------------

import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.pipeline_logger import finish_run, start_run
from utils.supabase_client import get_client

SCRIPT_NAME = "pull_comtrade_hs_lookup"
SOURCE_LABEL = "UN Comtrade HS classification (classificationHS.json)"
UPSERT_BATCH = 500


def _http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=(502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _category_for_hs6(hs6: str) -> str | None:
    for prefix, cat in HS_PREFIX_CATEGORY:
        if hs6.startswith(prefix):
            return cat
    return None


def _parse_hs6_entry(item: dict[str, Any]) -> tuple[str, str] | None:
    raw_id = item.get("id")
    if raw_id is None:
        return None
    hid = str(raw_id).strip()
    if len(hid) != 6 or not hid.isdigit():
        return None
    text = str(item.get("text") or "").strip()
    if " - " in text:
        desc = text.split(" - ", 1)[1].strip()
    else:
        desc = text
    if not desc:
        return None
    return hid, desc


def main() -> int:
    client = get_client()
    params: dict[str, Any] = {"url": CLASSIFICATION_HS_JSON_URL}
    run_id = start_run(client, SCRIPT_NAME, SOURCE_LABEL, params)

    try:
        session = _http_session()
        resp = session.get(CLASSIFICATION_HS_JSON_URL, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results")
        if not isinstance(results, list):
            finish_run(
                client,
                run_id,
                0,
                "error",
                "classificationHS.json: missing or invalid 'results' array.",
            )
            return 1

        rows: list[dict[str, Any]] = []
        for item in results:
            if not isinstance(item, dict):
                continue
            parsed = _parse_hs6_entry(item)
            if not parsed:
                continue
            hs6, description = parsed
            rows.append(
                {
                    "hs6_code": hs6,
                    "description": description,
                    "category": _category_for_hs6(hs6),
                    "notes": NOTES_PROVENANCE,
                }
            )

        if not rows:
            finish_run(client, run_id, 0, "error", "No HS6 rows parsed from classificationHS.json.")
            return 1

        for i in range(0, len(rows), UPSERT_BATCH):
            batch = rows[i : i + UPSERT_BATCH]
            client.table("hs_code_lookup").upsert(batch, on_conflict="hs6_code").execute()

        finish_run(client, run_id, len(rows), "success", None)
        print(f"Upserted {len(rows)} hs_code_lookup rows from UN Comtrade.")
        return 0

    except ValueError as e:
        err = f"Invalid JSON from Comtrade: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1
    except requests.HTTPError as e:
        err = f"HTTP error: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        finish_run(client, run_id, 0, "error", err)
        print(err, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
