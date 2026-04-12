# Handover — Hormuz Supply Chain Data Pipeline

This note is for the next agent or developer picking up the repo. The canonical product spec remains [README.md](README.md).

---

## Project snapshot

- **Stack:** Python 3.11+ (repo uses uv), Supabase (Postgres), `pandas` / `requests` / `openpyxl`, `pycountry`, `faostat` (FAO API for fertilizer pull), optional Streamlit.
- **Layout:** [pullers/](pullers/) (API/file pulls), [loaders/](loaders/) (manual files), [schema/create_tables.sql](schema/create_tables.sql), [utils/](utils/) (Supabase client + `pipeline_runs` logging).
- **Rule:** Pullers/loaders write to Supabase; the Streamlit app should read only (see README).
- **`table_catalog`:** Reference rows describing every application table (purpose, grain, keys, scripts). Seeded from SQL; update [schema/seed_table_catalog.sql](schema/seed_table_catalog.sql) when you add tables.

**Streamlit trade RPCs:** Apply [`schema/rpc_trade_dashboards.sql`](schema/rpc_trade_dashboards.sql) after `create_tables.sql`. The **Exporter & partners** section uses these for fast aggregates (HS6 totals, partner totals, supplier concentration) and is **lazy / stepwise** (Load buttons). The **Group dependencies** section uses additional RPCs (group share of world exports by HS6, importer exposure, distinct exporters per year) plus tables **`trade_group_dependency_snapshots`** / **`trade_group_dependency_rows`** for saved runs; heavy aggregates use **`plpgsql`** with **`statement_timeout = 120s`** to avoid PostgREST timeouts. Shared logic lives in [`utils/group_dependency_compute.py`](utils/group_dependency_compute.py); CLI mirror: [`scripts/run_group_dependency_snapshot.py`](scripts/run_group_dependency_snapshot.py). **`./scripts/verify.sh`** runs `py_compile` + `unittest`.

**Streamlit navigation (important):** The app uses a **sidebar section radio**, not `st.tabs` for the eight main areas. Native tabs would execute **all** tab bodies on every rerun (any slider anywhere), which looked like “the whole app reloading.” Only the selected section runs. **Group dependencies** is additionally wrapped in **`@st.fragment()`** so in-section controls do not rerun the entire script. Heavy RPCs there are gated on **Load / compute** (or same-params cache / loaded snapshot), not on every widget change. PostgREST JSON keys for RPCs must match Postgres names (**`p_data_year`**, **`p_hs6_code`**, etc.).

**GEM:** [`load_gem_xlsx.py`](loaders/load_gem_xlsx.py) → **`gem_tracker_rows`** (`payload` JSONB = row as header→value map; unique on `source_file`, `sheet_name`, `excel_row_1based`). Default bundle = eight workbooks (industrial/plant trackers + GGIT/GOIT pipelines/LNG + Global Integrated Power); canonical sheet list in script **`DEFAULT_WORKBOOKS`**. Other `.xlsx` via `--file` (skips **About** / **Metadata** unless `--include-meta-sheets`). **`--sheets`** does not work for sheet names containing **commas** — omit it and use `--file` only. **Global Integrated Power** is huge (~180k+ rows on *Power facilities*); full default run is slow. Skip `*.zip` GIS for v1. **Done:** **JODI** [`load_jodi.py`](loaders/load_jodi.py); **USGS** [`load_usgs.py`](loaders/load_usgs.py).

**Pullers (all log `pipeline_runs`):**

| Script | Tables |
|--------|--------|
| [pull_eia.py](pullers/pull_eia.py) | `energy_trade_flows` |
| [pull_faostat.py](pullers/pull_faostat.py) | `crop_production`, `fertilizer_production`, `food_balance_sheets` (`--dataset` crops / fertilizer / fbs / all) |
| [pull_worldbank.py](pullers/pull_worldbank.py) | `commodity_prices` |
| [pull_worldbank_wdi.py](pullers/pull_worldbank_wdi.py) | `country_macro_indicators` |
| [pull_usda_psd.py](pullers/pull_usda_psd.py) | `crop_production` |
| [pull_comtrade_hs_lookup.py](pullers/pull_comtrade_hs_lookup.py) | `hs_code_lookup` |

**Loaders (all log `pipeline_runs`):**

| Script | Tables |
|--------|--------|
| [load_baci.py](loaders/load_baci.py) | `bilateral_trade` — base HS filter in script; optional **`--exporter-full-hs`** (repeat per ISO3) / **`--importer-full-hs`** for full legs; **`--hs6-codes`** / **`--hs6-codes-file`** for **global** flows (all partners) for listed HS6 (needed for credible “world share” and **fair multi-member** **Group dependencies**) |
| [load_cepi_beyond_baci.py](loaders/load_cepi_beyond_baci.py) | `cepii_protee_hs6`, `cepii_geodep_import_dependence` |
| [load_jodi.py](loaders/load_jodi.py) | `jodi_energy_observations` |
| [load_usgs.py](loaders/load_usgs.py) | `usgs_mineral_statistics` (`mcs`); `usgs_myb3_production` + `usgs_country_mineral_facilities` (`facilities` / `myb3*.xlsx`) |
| [load_gem_xlsx.py](loaders/load_gem_xlsx.py) | `gem_tracker_rows` (default bundle: 8 workbooks — see README **load_gem_xlsx** table + `DEFAULT_WORKBOOKS` in script) |

**CEPII beyond BACI (what it is vs `bilateral_trade`):** BACI = reconciled **bilateral flows** (exporter, importer, HS6, value, qty). **ProTEE** = one row per HS6 **import-demand elasticity** + flags (not flows; HS **2007** in CEPII’s file). **GeoDep** = importer × HS6 × year **dependence diagnostics** (HHI, persistence, top supplier code) derived from BACI — not a replacement for flow-level rows. **WTFC/CHELEM zips** in `data/cepi/` are documented in [README.md](README.md) but have **no loader** yet (huge HS96 CSVs). Full semantics: module docstring in `load_cepi_beyond_baci.py` and README **Loaders** subsection.

**Group dependencies (Streamlit section):** Computes Gulf (or any chosen ISO3 group) **share of world exports** by HS6, within-group concentration, and importer exposure. **World denominator** requires **`bilateral_trade`** rows for **all exporters** for that HS6 × year — not only Gulf legs. If you only loaded regional BACI, shares can look wrong (e.g. 100% for a product). Fix: run `load_baci.py` with **`--hs6-codes`** (or a file of HS6) for the products you care about (e.g. top-N HS6 by group value from a first pass). Re-run when the HS6 list or year changes. UI can **save** results to **`trade_group_dependency_snapshots`** / **`trade_group_dependency_rows`**; **Saved snapshots** expander (above the compute gate) can **load** a prior run without running **Load / compute** first. **`rpc_trade_distinct_exporters_for_year`** supplies the full exporter dropdown (not a truncated table scan).

---

## USGS myb3 yearbooks (`myb3-*.xlsx`)

**Status:** Implemented. Run **`uv run python loaders/load_usgs.py facilities`** with regional workbooks under [`data/usgs/`](data/usgs/). **`pipeline_runs.source_label`** for that leg is *USGS Minerals Yearbook myb3 country xlsx*.

**Tables:** **`usgs_myb3_production`** (Table 1 melted), **`usgs_country_mineral_facilities`** (Table 2 merged blocks). Filename pattern **`myb3-{year}-{slug}.xlsx`** → `reference_year` + ISO3 via slug map (`united-arab-emirates` → ARE, etc.). Unmapped filenames are skipped with a **partial** warning.

**Parsing notes (maintain in [`loaders/load_usgs.py`](loaders/load_usgs.py)):**

- **IDE:** `.xlsx` may be hidden from workspace search — loader uses **`Path.glob("myb3*.xlsx")`**.
- **Sheets:** `Text` skipped; resolve **`Table1` vs `Table 1`** with regex **`^table\s*1$`** (case-insensitive).
- **Table 1:** Header row = first column starts with `Commodity`; year columns detected by 4-digit tokens; **`commodity_path`** = section (`METALS` / `INDUSTRIAL MINERALS`) + colon-branch stack + row label; footnotes in spacer columns.
- **Table 2:** Locate Commodity / owner / location / capacity columns by **merged header text** on rows 5–6; **merged blocks** for wrapped rows; **`Continued`** in col A = continuation; chrome / subheader fragment rows skipped; **`Do.`** in commodity → **`commodity_leaf_resolved`** and **`commodity_cell_raw`** both set to the **resolved** label (no blank commodity columns); **`facility_path`** = section stack **+** commodity leaf (so it stays populated when the stack is reset for a new root product); **`do.`** in owner/location/capacity repeats last explicit value, with **flush** carrying **full merged** O/L/C for the next ditto. Canonical spec in **`load_usgs.py` module header**; regression: **`uv run python scripts/validate_myb3_table2.py`**.
- **Reload:** `facilities` **deletes** existing `usgs_myb3_production` / `usgs_country_mineral_facilities` rows for each **`source_file`** before upsert so stale fingerprints do not leave orphan `Do.` rows. Searching **`usgs_mineral_statistics`** (MCS) for “do” mostly hits prose (“do not …”) — use **`usgs_country_mineral_facilities`** for yearbook ditto QA.

**Regional files in use:** Bahrain, Iraq, Oman, Qatar, UAE (2019); Iran, Saudi Arabia (2023).

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
- **FAOSTAT fertilizer API** — [pullers/pull_faostat.py](pullers/pull_faostat.py): `FAOSTAT_API_TOKEN` *or* `FAOSTAT_USERNAME` + `FAOSTAT_PASSWORD` (required for `--dataset fertilizer` / `--dataset all` fertilizer leg). Optional: `FAOSTAT_FERTILIZER_API_CODE` (default **`RFB`**; **`RFN`** = by nutrient), `FAOSTAT_API_PAGE_LIMIT`, `FAOSTAT_API_AREA_CHUNK`, `FAOSTAT_API_SLEEP_SEC`, `FAOSTAT_API_AREAS` (comma-separated FAO area codes), `FAOSTAT_ZIP_PATH` (crop bulk only), `FAOSTAT_FBS_ZIP_PATH` (Food Balance bulk only).

Never commit `.env`. **Do not put live JWTs or passwords in `.env.example`** (placeholders only); rotate any secret that was ever committed there. Do not print API keys in tracebacks or `HTTPError` URLs; redact in logs where possible.

---

## Learnings (implementation reality)

### World Bank WDI — [pullers/pull_worldbank_wdi.py](pullers/pull_worldbank_wdi.py)

- **Separate script** from Pink Sheet: JSON API `https://api.worldbank.org/v2/country/all/indicator/{ID}` with **one indicator per request** (multi-indicator paths return invalid-parameter errors).
- **Pagination:** follow `pages` in the first JSON object until done; `per_page=20000` is enough for `country/all` × multi-year pulls in practice.
- **Country filter:** keep only rows where `countryiso3code` is a **real ISO3** (`pycountry.countries.get(alpha_3=...)`), so WB regions (AFE, WLD, …) drop out.
- **Writes:** `country_macro_indicators` with upsert on `(country, indicator, data_year)`.

### UN Comtrade HS lookup — [pullers/pull_comtrade_hs_lookup.py](pullers/pull_comtrade_hs_lookup.py)

- **URL:** `https://comtrade.un.org/data/cache/classificationHS.json` — single JSON with `results[]`; each HS6 row has `id` (six digits) and `text` (`CODE - description`).
- **Filter:** keep entries where `id` is six digits; skip chapters/headings.
- **`category`:** optional column filled from the same V1 prefix list as BACI (`HS_PREFIX_CATEGORY` in script).

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

- **Crops:** bulk normalized ZIP (same as before). Optional **`FAOSTAT_ZIP_PATH`** to skip download.
- **Fertilizers:** **`faostat`** package → FAOSTAT REST API (`get_data_df`), **not** the Inputs Fertilizers bulk URL. Bulk fertilizer ZIPs are often **CloudFront/geo or rate blocked**; the API avoids that path but **requires FAO API credentials** (JWT or username/password — see `.env.example`).
- **`pars={'area': 'all'}` is invalid** — the API returns an **empty** frame. The puller loads **FAO country area codes** from `get_par_df(dataset, 'area')` (rows with `aggregate_type == '0'`), then queries in **chunks** of **`FAOSTAT_API_AREA_CHUNK`** (default 40) with **`year`** = configured **`YEARS`**, optional **`FAOSTAT_API_SLEEP_SEC`** between chunks, optional **`FAOSTAT_API_AREAS`** to restrict codes.
- Default API dataset code **`RFB`** (*by product*). **`RFN`** = *by nutrient*; switch with **`FAOSTAT_FERTILIZER_API_CODE`** if you extend item mapping.
- Rows use the **`Area`** (country name) column mapped to **ISO3** via `pycountry` (plus a small skip list for composite labels). M49 column is used when present.
- **FBS (`food_balance_sheets`):** bulk ZIP `FoodBalanceSheets_E_All_Data_(Normalized).zip` (production bulks host). **`FAOSTAT_FBS_ZIP_PATH`** skips HTTP. Stream CSV in chunks; filter **`Item Code`** / **`Element Code`** / **`YEARS`**; **`Area Code (M49)`** → ISO3 via `_m49_to_iso3`. Units **`1000 t`** → multiply to **tonnes** (`_fbs_value_to_tonnes`). V1 item map: wheat/rice/corn/soybeans + **cotton** from **Cottonseed** (see `FBS_ITEM_TO_COMMODITY` in script). Full ZIP is **large** (~10+ minutes possible).
- **`--dataset crops` | `fertilizer` | `fbs` | `all`:** run each leg independently so retries do not re-stream huge ZIPs. With `--dataset all`, fertilizer or FBS failure → **`pipeline_runs`** **`partial`**, other legs still written.
- **`faostat.set_requests_args`** mutates global client state — call **once** per process (the package appends `lang/` to its base URL if invoked repeatedly).

### BACI — [loaders/load_baci.py](loaders/load_baci.py)

- Discovers `BACI_HS*_Y*.csv` with **`rglob`** under [data/baci/](data/baci/) so CEPII’s versioned subfolders work. No matching files → clear error and **`pipeline_runs`**.
- **`--hs6-codes`** / **`--hs6-codes-file`:** loads **global** bilateral rows for those HS6 (all exporter–importer pairs) for the chosen year(s). Use for **Group dependencies** world shares. Large lists → long runs; chunked upsert in loader.
- **`NaN`** in qty/value must not reach JSON upserts — loader coerces missing numerics to **`None`**.

### CEPII ProTEE / GeoDep — [loaders/load_cepi_beyond_baci.py](loaders/load_cepi_beyond_baci.py)

- **ProTEE:** small CSV → `cepii_protee_hs6`. **GeoDep:** chunked read of `geodep_data.csv` (~3M rows) → `cepii_geodep_import_dependence`; optional `GEODEP_HS_PREFIXES` in script to limit HS6 prefixes. Transient network errors → re-run (upsert idempotent).

### USGS myb3 — [loaders/load_usgs.py](loaders/load_usgs.py) `facilities`

- **Separate from MCS:** Table 1 + Table 2 live in **`usgs_myb3_production`** and **`usgs_country_mineral_facilities`**, not `usgs_mineral_statistics`.
- **Sheet names:** regex match `table\s*1` / `table\s*2` (handles `Table1` vs `Table 1`).
- **Table 2:** merged blocks + **`Do.`** = ditto to previous commodity cell; column positions vary (8 vs 9) — header text search, not fixed indices.

### Supabase / security

- **ETL client:** `get_client()` should use the **service role** (or secret) key — it **bypasses RLS**, so pullers/loaders keep working even if some tables have RLS enabled.
- **Legacy posture:** tables created only from [schema/create_tables.sql](schema/create_tables.sql) had **RLS off** by design. If new tables (e.g. `country_macro_indicators`, `food_balance_sheets`) were created via **Dashboard / MCP** with RLS **on**, either add policies for read paths or disable RLS on those tables to match the rest of `public`.
- Supabase linter flags **RLS disabled** — acceptable only if **anon** is never given broad access. If Streamlit ships to the internet with the **anon** key, add **RLS + policies** before launch.

---

## Omissions / not done yet

- **`bilateral_trade`** — empty until BACI CSVs are under `data/baci/` and `load_baci.py` runs (often already done in a working env).
- **`fertilizer_production`** — needs **FAOSTAT API** credentials and `pull_faostat.py --dataset fertilizer` (or `all`). Without them, `all` ends **`partial`** but crops (and FBS) still load.
- **`country_lookup`** — schema only; no puller yet (optional manual seed for names / Gulf flags).
- **`country_macro_indicators`** / **`food_balance_sheets`** — filled by `pull_worldbank_wdi.py` and `pull_faostat.py --dataset fbs` (or `all`) after schema exists.
- **`hs_code_lookup`** — run [pull_comtrade_hs_lookup.py](pullers/pull_comtrade_hs_lookup.py) once (or after Comtrade updates the JSON).
- **Streamlit** — main tabs: prices, BACI trade, country profile, **Exporter & partners**, **Group dependencies**, crops, pipeline, **Explore more**. **Explore more** includes macro (WDI), FBS, EIA energy, fertilizer, ProTEE, GeoDep, **JODI**, **USGS** (three table modes), **GEM**, **HS6** lookup, `table_catalog`, `country_lookup` — large sources use filter + row limits in [app/streamlit_app.py](app/streamlit_app.py).
- **GEM** — extend beyond default eight workbooks: edit [`DEFAULT_WORKBOOKS`](loaders/load_gem_xlsx.py) or use `--file`. **USGS MCS CSV** — `load_usgs.py mcs` + `usgs_mineral_statistics`. **USGS myb3** — `load_usgs.py facilities` + `usgs_myb3_production` / `usgs_country_mineral_facilities` (see **USGS myb3 yearbooks**). **JODI** — `load_jodi.py` + `jodi_energy_observations`.
- **WTFC / CHELEM zips** under `data/cepi/` — no loader (entire WTFC family deferred, including CHELEM price_range/type zips).
- **GEM GIS `*.zip`** (geojson/gpkg) — intentionally **deferred**; tabular `.xlsx` first.
- **Pagination / offset** for EIA/USDA when responses exceed **5000** rows (OK for current year ranges; fragile if ranges widen).

---

## Pitfalls

1. **`from __future__ import annotations`** must be the **first** statement (after module docstring only). The README script template puts **config assignments** first — **do not** put `__future__` after `YEARS = [...]` (SyntaxError). Either drop `__future__` (3.11+ builtins generics) or move config below imports (and adjust template consistency).
2. **`uv run python pullers/foo.py`** puts **`pullers/`** first on `sys.path`; **`import utils`** fails unless the project root is prepended (see `sys.path.insert` pattern in pullers) or the package is installed editable (uv does install `utils`).
3. **Secrets in errors:** `requests` HTTPError strings can include **full URLs with `api_key=`**. Catch non-2xx, log **status + body snippet**, redact keys.
4. **Supabase upsert batches** must not contain **duplicate rows for the same unique constraint** in one POST.
5. **Pink Sheet URL** and **EIA routes** are **external drift** risks — failures are often “update constant / facet / path”, not “random bug”.
6. **FAOSTAT `faostat` client:** do not call **`set_requests_args`** multiple times in one process (package bug: **`__BASE_URL__`** keeps growing).
7. **FAOSTAT fertilizer API:** **`pars={'area': 'all'}`** (or similar) returns **no rows** — the puller must send **explicit FAO area codes** in chunks (see script).
8. **GEM `load_gem_xlsx.py`:** Do not pass **`--sheets "..., ..."`** when a sheet name **contains a comma** (e.g. *Regions, area, and countries*) — splits wrong. Use **`--file workbook.xlsx`** without `--sheets` to load all non-About/Metadata sheets, or extend **`DEFAULT_WORKBOOKS`** with explicit sheet names.

---

## GEM default bundle (`gem_tracker_rows`)

**Loader:** `uv run python loaders/load_gem_xlsx.py` (or `--dry-run`). **Table:** `gem_tracker_rows`; **DDL / catalog:** [`schema/create_tables.sql`](schema/create_tables.sql), [`schema/seed_table_catalog.sql`](schema/seed_table_catalog.sql).

**Workbooks (data sheets only)** — filenames must match under [`data/globalenergymonitor/`](data/globalenergymonitor/):

| File | Sheets |
|------|--------|
| `Global-Cement-and-Concrete-Tracker_July-2025.xlsx` | Plant Data |
| `Global-Iron-Ore-Mines-Tracker-August-2025-V1.xlsx` | Main Data |
| `Plant-level-data-Global-Chemicals-Inventory-November-2025-V1.xlsx` | Plant data |
| `Plant-level-data-Global-Iron-and-Steel-Tracker-March-2026-V1.xlsx` | Plant data; Plant capacities and status; Plant production |
| `GEM-GOIT-Oil-NGL-Pipelines-2025-03.xlsx` | Pipelines |
| `GEM-GGIT-LNG-Terminals-2025-09.xlsx` | LNG Terminals |
| `GEM-GGIT-Gas-Pipelines-2025-11.xlsx` | Pipelines |
| `Global-Integrated-Power-March-2026-II.xlsx` | Power facilities; Regions, area, and countries |

**Semantics:** Per `(source_file, sheet_name)`, existing rows are **deleted**, then data rows inserted (batched). **`openpyxl` read-only** streaming. Same physical workbook can contribute **multiple `sheet_name` values** (e.g. iron/steel three sheets).

---

## Next steps (suggested order)

1. **Schema:** Apply [schema/create_tables.sql](schema/create_tables.sql) (SQL Editor, `psql`, or Supabase **`apply_migration`** via MCP if configured).
2. **RPCs:** Apply [schema/rpc_trade_dashboards.sql](schema/rpc_trade_dashboards.sql) (Streamlit trade tabs + Group dependencies).
3. **HS reference:** `uv run python pullers/pull_comtrade_hs_lookup.py` (Comtrade JSON → `hs_code_lookup`).
4. **BACI:** CEPII CSVs → `data/baci/` → `uv run python loaders/load_baci.py --all` (or `--year YYYY`). For **Group dependencies** with correct world shares, also load global rows for the HS6 set you analyze, e.g. `uv run python loaders/load_baci.py --year 2024 --hs6-codes-file path/to/top_hs6.txt`.
5. **Pullers:** Run the rest per [README — Running Everything](README.md#running-everything-first-time). **USDA** needs **`USDA_FAS_API_KEY`**. **FAOSTAT fertilizer** needs FAO API token or user/pass for the fertilizer leg of `--dataset all`.
6. **Verify:** `SELECT * FROM pipeline_runs ORDER BY completed_at DESC LIMIT 20;` and row counts per table.
7. **Streamlit:** `uv run streamlit run app/streamlit_app.py` (optional: `--server.port 8504`) — prefer **`get_read_client()`** + anon key for anything exposed beyond localhost.
8. **Optional:** Seed `country_lookup`; production RLS; CI / smoke tests.
9. **Next data wave:** extend **GEM** (`load_gem_xlsx.py` / more `.xlsx`). **JODI:** `uv run python loaders/load_jodi.py`. **USGS:** `uv run python loaders/load_usgs.py mcs` (MCS CSV); **`uv run python loaders/load_usgs.py facilities`** (myb3 `*.xlsx`).

---

## Quick commands

```bash
cd "/path/to/2604 supply chain"
uv sync
cp .env.example .env   # then fill secrets

uv run python pullers/pull_worldbank.py
uv run python pullers/pull_worldbank_wdi.py
uv run python pullers/pull_faostat.py --dataset all   # or: crops | fertilizer | fbs
uv run python pullers/pull_eia.py
uv run python pullers/pull_usda_psd.py
uv run python pullers/pull_comtrade_hs_lookup.py
uv run python loaders/load_baci.py --all
uv run python loaders/load_cepi_beyond_baci.py protee   # optional
uv run python loaders/load_cepi_beyond_baci.py geodep   # optional; long
uv run python loaders/load_jodi.py   # optional; default min year 2020
uv run python loaders/load_usgs.py mcs          # optional; MCS CSV under data/usgs/
uv run python loaders/load_usgs.py facilities   # optional; myb3-*.xlsx under data/usgs/
uv run python loaders/load_gem_xlsx.py      # optional; default GEM bundle → gem_tracker_rows

uv run streamlit run app/streamlit_app.py
```

---

## Key files

| File | Role |
|------|------|
| [utils/supabase_client.py](utils/supabase_client.py) | `get_client()`, `get_read_client()` |
| [utils/pipeline_logger.py](utils/pipeline_logger.py) | `start_run` / `finish_run` → `pipeline_runs` |
| [schema/create_tables.sql](schema/create_tables.sql) | Full DDL + upsert `UNIQUE` targets + `table_catalog` seed |
| [schema/rpc_trade_dashboards.sql](schema/rpc_trade_dashboards.sql) | Trade RPCs for Streamlit (Exporter & partners, Group dependencies, distinct exporters) |
| [schema/seed_table_catalog.sql](schema/seed_table_catalog.sql) | Idempotent `table_catalog` row updates only |
| [app/streamlit_app.py](app/streamlit_app.py) | Streamlit explorer — trade tabs, snapshot save/load for Group dependencies |
| [loaders/load_cepi_beyond_baci.py](loaders/load_cepi_beyond_baci.py) | CEPII ProTEE / GeoDep loaders + dataset semantics |
| [loaders/load_usgs.py](loaders/load_usgs.py) | USGS MCS CSV (`mcs`); myb3 country xlsx (`facilities`) → `usgs_myb3_*` tables |
| [loaders/load_gem_xlsx.py](loaders/load_gem_xlsx.py) | GEM `.xlsx` → `gem_tracker_rows` (default 8-workbook bundle) |
| [README.md](README.md) | User-facing spec, puller docs, schema, HS scope, future roadmap |

---

*Handover note: keep this file aligned with README when you add pullers, tables, or env vars.*
