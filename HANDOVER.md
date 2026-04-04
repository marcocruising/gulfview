# Handover — Hormuz Supply Chain Data Pipeline

This note is for the next agent or developer picking up the repo. The canonical product spec remains [README.md](README.md).

---

## Project snapshot

- **Stack:** Python 3.11+ (repo uses uv), Supabase (Postgres), `pandas` / `requests` / `openpyxl`, optional Streamlit.
- **Layout:** [pullers/](pullers/) (API/file pulls), [loaders/](loaders/) (manual files), [schema/create_tables.sql](schema/create_tables.sql), [utils/](utils/) (Supabase client + `pipeline_runs` logging).
- **Rule:** Pullers/loaders write to Supabase; the Streamlit app should read only (see README).

---

## Environment variables

Documented in [.env.example](.env.example). Pullers/loaders use [utils/supabase_client.py](utils/supabase_client.py):

| Purpose | Variables (order matters for server client) |
|--------|---------------------------------------------|
| Supabase URL | `SUPABASE_URL` |
| Server / ETL key | `SUPABASE_SERVICE_ROLE_KEY` → `SUPABASE_SECRET_KEY` → legacy `SUPABASE_KEY` |
| Read-only UI | `get_read_client()`: `SUPABASE_ANON_PUBLIC_KEY` → `SUPABASE_PUBLISHABLE_KEY` |

Additional keys:

- `EIA_API_KEY` — [pullers/pull_eia.py](pullers/pull_eia.py)
- `USDA_FAS_API_KEY` — [pullers/pull_usda_psd.py](pullers/pull_usda_psd.py)

Never commit `.env`. Do not print API keys in tracebacks or `HTTPError` URLs; redact in logs where possible.

---

## Learnings (implementation reality)

### World Bank Pink Sheet — [pullers/pull_worldbank.py](pullers/pull_worldbank.py)

- Data is **not** on sheet 1: use sheet **`Monthly Prices`**, `header=4` (see script constants).
- Columns are **commodities × dates as rows**; use **melt** to long form.
- Excel headers are **not** always the short names in the README (e.g. crude is **`Crude oil, average`**, Brent is **`Crude oil, Brent`**); the script uses **alias lists** to resolve columns.
- **Ammonia:** current monthly workbook has **no** Ammonia column — runs end **`partial`** with a clear `pipeline_runs` message; other series still load.
- **404 on XLSX URL:** World Bank changes the **doc hash** in the `thedocs` path ~1–2×/year. Script must record **`pipeline_runs.status=error`** with instructions to update `PINK_SHEET_MONTHLY_XLSX_URL` and exit cleanly (no bare crash).

### EIA — [pullers/pull_eia.py](pullers/pull_eia.py)

- **`/v2/petroleum/move/wimcli`** is **invalid**; use **`https://api.eia.gov/v2/petroleum/move/impcus/data/`**.
- **`facets[product][]=EPC0`** is required for **crude only**; without it, responses mix products.
- API returns **`data[0]=value`** (not `quantity`) for this route.
- **`value`** is **thousand barrels per month** for the non–“per Day” series. Schema field `value_kbd` is interpreted as **kbd**: convert with **`value_kb_per_month / calendar.monthrange(year, month)[1]`** (not a fixed 30).
- Row geography for country series uses **`area-name`** (often ISO3-like); skip **`NA`** aggregates and **`per Day`** duplicate series.

### USDA PSD — [pullers/pull_usda_psd.py](pullers/pull_usda_psd.py)

- Detail rows expose **`attributeId`**, not attribute names. Production / imports / exports for row crops map to **28 / 57 / 88** (verify against `/commodityAttributes` if USDA changes metadata).
- **`countryCode`** is USDA’s code, not ISO3: build a map from **`/countries`** using **`gencCode`** as ISO3; skip rows with no `gencCode` (regions/aggregates).
- **`/unitsOfMeasure`**: **`(1000 MT)`** → multiply by **1000** to get **tonnes** for `crop_production`.
- Same natural key can appear **many times** (monthly revisions). **Dedupe** by keeping the row with the latest **`calendarYear` + `month`** before upsert, or Postgres raises *“ON CONFLICT DO UPDATE cannot affect row a second time”* within one batch.

### FAOSTAT — [pullers/pull_faostat.py](pullers/pull_faostat.py)

- Crop path has been exercised successfully; **fertilizer** may still be a **separate dataset/API shape** — check script output and `pipeline_runs` for `partial` / messages.

### BACI — [loaders/load_baci.py](loaders/load_baci.py)

- Expects files matching `BACI_HS*_Y*.csv` under [data/baci/](data/baci/). No files → script exits with a clear error and logs **`pipeline_runs`**.

### Supabase / security

- Tables are in **`public`** with **RLS off** (intentional for service-role ETL). Supabase linter flags **RLS disabled** — acceptable only if **anon** is never given broad access. If Streamlit ships to the internet with the **anon** key, add **RLS + policies** before launch.

---

## Omissions / not done yet

- **`bilateral_trade`** empty until BACI CSVs are placed and `load_baci.py` is run.
- **`fertilizer_production`** may still be empty or incomplete depending on FAOSTAT script coverage.
- **`hs_code_lookup`** / **`country_lookup`** — schema exists; **seeding** not part of the initial pipeline pass (optional for UX and joins).
- **Streamlit** — [app/streamlit_app.py](app/streamlit_app.py) exists; confirm it matches README filters, uses the right `get_client` / `get_read_client`, and works against current RLS posture.
- **Pagination / offset** for EIA/USDA when responses exceed **5000** rows (currently OK for configured year ranges but fragile if ranges widen).

---

## Pitfalls

1. **`from __future__ import annotations`** must be the **first** statement (after module docstring only). The README script template puts **config assignments** first — **do not** put `__future__` after `YEARS = [...]` (SyntaxError). Either drop `__future__` (3.11+ builtins generics) or move config below imports (and adjust template consistency).
2. **`uv run python pullers/foo.py`** puts **`pullers/`** first on `sys.path`; **`import utils`** fails unless the project root is prepended (see `sys.path.insert` pattern in pullers) or the package is installed editable (uv does install `utils`).
3. **Secrets in errors:** `requests` HTTPError strings can include **full URLs with `api_key=`**. Catch non-2xx, log **status + body snippet**, redact keys.
4. **Supabase upsert batches** must not contain **duplicate rows for the same unique constraint** in one POST.
5. **Pink Sheet URL** and **EIA routes** are **external drift** risks — failures are often “update constant / facet / path”, not “random bug”.

---

## Next steps (suggested order)

1. **BACI:** Obtain CEPII files → `data/baci/` → `uv run python loaders/load_baci.py --all` (or `--year YYYY`).
2. **Verify DB:** `SELECT * FROM pipeline_runs ORDER BY completed_at DESC LIMIT 20;` and row counts per table.
3. **FAOSTAT fertilizer:** Extend [pullers/pull_faostat.py](pullers/pull_faostat.py) or add a focused pull so `fertilizer_production` matches README intent.
4. **Streamlit:** Run `uv run streamlit run app/streamlit_app.py`, align with README explorer spec, wire **`get_read_client()`** if using anon in the UI.
5. **Reference data:** Seed `hs_code_lookup` and `country_lookup` (Gulf producer flags per README).
6. **Production hardening:** RLS policies if anon access; optional CI (`ruff`, `uv sync --locked`) and smoke tests for pullers (mock HTTP or small fixtures).

---

## Quick commands

```bash
cd "/path/to/2604 supply chain"
uv sync
cp .env.example .env   # then fill secrets

uv run python pullers/pull_worldbank.py
uv run python pullers/pull_faostat.py
uv run python pullers/pull_eia.py
uv run python pullers/pull_usda_psd.py
uv run python loaders/load_baci.py --all

uv run streamlit run app/streamlit_app.py
```

---

## Key files

| File | Role |
|------|------|
| [utils/supabase_client.py](utils/supabase_client.py) | `get_client()`, `get_read_client()` |
| [utils/pipeline_logger.py](utils/pipeline_logger.py) | `start_run` / `finish_run` → `pipeline_runs` |
| [schema/create_tables.sql](schema/create_tables.sql) | Full DDL + upsert `UNIQUE` targets |

---

*Last updated for handoff context; keep this file in sync when behavior or env vars change materially.*
