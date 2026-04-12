# Hormuz Supply Chain Data Explorer

A personal data pipeline and exploration tool mapping the supply chain dependencies
of a Strait of Hormuz closure — from energy flows through fertilizer production to
food security by country.

**V1 scope:** Energy (crude, LNG, refined products) → Fertilizers (urea, ammonia,
DAP/MAP) → Food crops (wheat, rice, corn, soybeans, cotton).

**Architecture principle:** Pull scripts and loaders write to Supabase. The Streamlit
app only ever reads from Supabase. No API calls from the UI.

---

## Project Structure

```
hormuz-supply-chain/
│
├── README.md                        # this file
├── pyproject.toml                   # uv dependency manifest
├── uv.lock                          # uv lockfile — commit this
├── .env                             # local secrets — never commit
├── .env.example                     # template for secrets — commit this
├── .gitignore
│
├── data/
│   ├── baci/                        # BACI CSVs → load_baci.py → bilateral_trade
│   ├── cepi/                        # ProTEE + GeoDep CSVs; optional WTFC/CHELEM zips (no loader)
│   ├── jodi/                        # JODI CSV exports (gas/oil); load_jodi.py → jodi_energy_observations
│   ├── usgs/                        # MCS CSV + myb3-*.xlsx yearbooks → load_usgs.py mcs | facilities
│   └── globalenergymonitor/         # GEM .xlsx → load_gem_xlsx.py → gem_tracker_rows; GIS .zip deferred
│
├── schema/
│   └── create_tables.sql            # full Supabase schema — run once
│   └── rpc_trade_dashboards.sql     # SQL RPCs used by Streamlit trade drill-down (apply to Supabase)
│
├── pullers/                         # scripts that fetch remote data (HTTP API or published file) and write to Supabase
│   ├── pull_eia.py                  # EIA: crude/LNG/refined product flows
│   ├── pull_faostat.py              # FAOSTAT: crops (bulk ZIP) + fertilizers (API) + FBS (bulk ZIP)
│   ├── pull_worldbank.py            # World Bank Pink Sheet: commodity prices
│   ├── pull_worldbank_wdi.py        # World Bank WDI API: population, GDP, GDP per capita
│   ├── pull_usda_psd.py             # USDA PSD: crop supply/demand by country
│   └── pull_comtrade_hs_lookup.py   # UN Comtrade: HS6 descriptions → hs_code_lookup
│
├── loaders/                         # scripts that ingest manually downloaded files
│   ├── load_baci.py                 # BACI: bilateral trade flows (HS6, 200 countries)
│   ├── load_cepi_beyond_baci.py     # CEPII ProTEE + GeoDep (elasticities & import-dependence)
│   ├── load_jodi.py                 # JODI oil/gas CSVs → jodi_energy_observations
│   ├── load_usgs.py                 # USGS: MCS CSV → usgs_mineral_statistics; myb3 xlsx → usgs_myb3_* tables
│   └── load_gem_xlsx.py             # GEM: selected .xlsx → gem_tracker_rows (JSON per row)
│
├── app/
│   └── streamlit_app.py             # data explorer UI — reads from Supabase only (sidebar sections; see below)
│
├── scripts/
│   ├── verify.sh                    # py_compile + unit tests (used in CI-style checks)
│   └── run_group_dependency_snapshot.py  # CLI: same snapshot write as Group dependencies tab
│
├── tests/
│   └── test_group_dependency_compute.py  # hash + RPC payload tests (no live DB)
│
└── utils/
    ├── supabase_client.py           # shared Supabase connection
    ├── pipeline_logger.py           # shared pipeline run logging
    └── group_dependency_compute.py  # shared group-dependency snapshot + RPC helpers (Streamlit + CLI)
```

### Local data folders — what is loaded vs planned

| Folder | Contents | Supabase today |
|--------|----------|----------------|
| `data/baci/` | CEPII BACI yearly CSVs | **`bilateral_trade`** via [`loaders/load_baci.py`](loaders/load_baci.py) |
| `data/cepi/` | `ProTEE_0_1.csv`, `geodep_data.csv`; optional **WTFC_HS96** / **CHELEM** zips (large) | **`cepii_protee_hs6`**, **`cepii_geodep_import_dependence`** via [`loaders/load_cepi_beyond_baci.py`](loaders/load_cepi_beyond_baci.py). **WTFC/CHELEM zips:** no loader — deferred. |
| `data/jodi/` | Flat **CSVs** (e.g. gas `STAGING_world_NewFormat.csv`, oil `primaryyear2026.csv` — same column layout); optional `jodi-oil-country-note.xlsx` (notes) | **`jodi_energy_observations`** via [`loaders/load_jodi.py`](loaders/load_jodi.py). Gas files often span **2009–present** (~18k rows/year at steady state); the loader defaults to **`data_year >= 2020`** to match BACI-style horizons — use **`--all-years`** or **`--min-year YYYY`** to change. |
| `data/usgs/` | **`MCS*_Commodities_Data.csv`**; regional **`myb3-{year}-{country}.xlsx`** (Minerals Yearbook Vol. III–style) | **`usgs_mineral_statistics`** via `load_usgs.py mcs` (**`cp1252`**, **`record_fingerprint`**). **`usgs_myb3_production`** + **`usgs_country_mineral_facilities`** via **`load_usgs.py facilities`** (Table 1 melt + Table 2 merged blocks, **`Do.`** ditto). Regional files: Bahrain, Iraq, Oman, Qatar, UAE (2019); Iran, Saudi Arabia (2023). Cursor may omit `.xlsx` from search — use **`Path.glob("myb3*.xlsx")`** or the OS folder. Details: [HANDOVER.md](HANDOVER.md) **USGS myb3 yearbooks**. |
| `data/globalenergymonitor/` | **`.xlsx`** trackers | **`gem_tracker_rows`** via [`loaders/load_gem_xlsx.py`](loaders/load_gem_xlsx.py) (default: those four trackers plus GGIT gas pipelines, GGIT LNG terminals, GOIT oil/NGL pipelines, Global Integrated Power). **`.zip` GIS**: deferred. |

**`table_catalog`** ([`schema/seed_table_catalog.sql`](schema/seed_table_catalog.sql)) describes existing tables; add rows when new tables land (e.g. GEM).

---

## Environment Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) installed: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- A Supabase project (free tier is sufficient for v1)

### 1. Clone and install dependencies

```bash
git clone <your-repo>
cd hormuz-supply-chain
uv sync
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your values (same variable names as [`.env.example`](.env.example)):

```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
SUPABASE_PUBLISHABLE_KEY=...
SUPABASE_SECRET_KEY=...
SUPABASE_ANON_PUBLIC_KEY=...
```

**Pullers and loaders** use [`utils/supabase_client.py`](utils/supabase_client.py) `get_client()`, which picks the first available server key in this order: **`SUPABASE_SERVICE_ROLE_KEY`**, **`SUPABASE_SECRET_KEY`**, then legacy **`SUPABASE_KEY`**. Use the **service role** or equivalent **secret** key so scripts can write to the database (Supabase → Project Settings → API).

**Read-only UI** (e.g. Streamlit against RLS-protected data) can use `get_read_client()` with **`SUPABASE_ANON_PUBLIC_KEY`** or **`SUPABASE_PUBLISHABLE_KEY`**.

**USDA PSD pull** needs **`USDA_FAS_API_KEY`** in `.env` ([api.fas.usda.gov](https://api.fas.usda.gov)). **FAOSTAT fertilizer** needs **`FAOSTAT_API_TOKEN`** (or username/password); optional tuning vars are in [`.env.example`](.env.example). Keep secrets out of `.env.example`.

### 3. Initialise the database

Apply [schema/create_tables.sql](schema/create_tables.sql) once to your Supabase project (creates all tables, indexes, and **seeds `table_catalog`**):

```bash
# Option A: Supabase Dashboard → SQL Editor → paste the file and run
# Option B: psql with your Postgres connection string
psql "$DATABASE_URL" -f schema/create_tables.sql
```

Apply trade dashboard RPCs used by Streamlit (safe to re-run; creates/replaces SQL functions + supporting indexes). This file includes **Exporter & partners** aggregates; **distinct lists** on `bilateral_trade` (**`rpc_trade_distinct_exporters`**, **`rpc_trade_distinct_exporters_for_year`**, **`rpc_trade_distinct_hs6_for_year`**, **`rpc_trade_distinct_data_years`**, **`rpc_trade_distinct_country_iso3_for_year`**) so dropdowns are complete without capped client scans; and **Group dependencies** RPCs. The file ends with **`GRANT EXECUTE`** on all `public.rpc_trade%` functions for **`anon`**, **`authenticated`**, and **`service_role`** so PostgREST calls from the **publishable** key succeed (without this, the app falls back to capped scans). Heavy group-share functions run as `plpgsql` with an extended **`statement_timeout`** (120s) to reduce PostgREST timeouts on large `bilateral_trade` scans.
```bash
# Option A: Supabase Dashboard → SQL Editor → run the file
# Option B: psql
psql "$DATABASE_URL" -f schema/rpc_trade_dashboards.sql
```

To refresh dictionary text only (without re-running the whole schema file), use [schema/seed_table_catalog.sql](schema/seed_table_catalog.sql).

If you use **Cursor with the Supabase MCP** connected to this project, you can apply the same DDL via **`apply_migration`** (or run the SQL from the file there) instead of pasting manually.

---

## Database Schema

**Time-series and fact tables** (`energy_trade_flows`, `bilateral_trade`, `crop_production`, etc.) follow these conventions:
- `source` — name of the script that wrote the row (e.g. `pull_eia`)
- `pulled_at` — UTC timestamp when the row was inserted/upserted
- `data_year` — the reference year the data describes (not the pull year), when applicable

**Reference tables** `hs_code_lookup` and `country_lookup` have no `source` / `pulled_at` / `data_year`; they are keyed for joins and manual or puller-driven updates.

Natural keys and `UNIQUE` constraints match puller/loader `on_conflict` targets so upserts are safe to re-run.

### Tables

#### `table_catalog`
Reference **data dictionary**: one row per application table describing **title**, **summary** (what the table contains and what it is for), **row_grain** (what one row represents), **key_columns** (join / upsert keys), **populated_by** (which script maintains it), and **sort_order** for display. Not populated by a puller; rows ship with the schema (see [schema/seed_table_catalog.sql](schema/seed_table_catalog.sql) and the tail of [schema/create_tables.sql](schema/create_tables.sql)). Re-run the seed `INSERT ... ON CONFLICT DO UPDATE` after you add a new table or change meanings.

```sql
id              SERIAL PRIMARY KEY
table_schema    TEXT NOT NULL DEFAULT 'public'
table_name      TEXT NOT NULL
title           TEXT NOT NULL
summary         TEXT NOT NULL
row_grain       TEXT
key_columns     TEXT
populated_by    TEXT
sort_order      INTEGER NOT NULL DEFAULT 0
updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
UNIQUE (table_schema, table_name)
```

#### `pipeline_runs`
Log of every script execution. Check this to know what data you have and when it was last refreshed.

```sql
id              SERIAL PRIMARY KEY
script_name     TEXT              -- e.g. 'pull_eia', 'load_baci'
source_label    TEXT              -- human readable: 'EIA Open Data API'
parameters      JSONB             -- exactly what params were used: years, hs_codes, countries
rows_written    INTEGER
status          TEXT              -- 'success' | 'partial' | 'error'
error_message   TEXT              -- null if success
started_at      TIMESTAMPTZ
completed_at    TIMESTAMPTZ
```

#### `energy_trade_flows`
Oil and gas import/export volumes by country, from EIA.

```sql
id              SERIAL PRIMARY KEY
reporter        TEXT              -- ISO3 country code
flow_type       TEXT              -- 'import' | 'export'
product         TEXT              -- 'crude_oil' | 'lng' | 'refined_products'
value_kbd       NUMERIC           -- thousand barrels per day
data_year       INTEGER
data_month      INTEGER           -- null for annual data
source          TEXT
pulled_at       TIMESTAMPTZ
UNIQUE (reporter, flow_type, product, data_year, data_month)
```

#### `bilateral_trade`
Bilateral trade flows by HS code. Populated by BACI loader.
This is the central table for supply chain dependency analysis.

```sql
id              SERIAL PRIMARY KEY
exporter        TEXT              -- ISO3
importer        TEXT              -- ISO3
hs6_code        TEXT              -- 6-digit HS code e.g. '270900'
hs_description  TEXT              -- human label from hs_code_lookup
trade_value_usd NUMERIC           -- USD thousands
quantity_tonnes NUMERIC
data_year       INTEGER
source          TEXT              -- 'baci_2022' etc.
pulled_at       TIMESTAMPTZ
UNIQUE (exporter, importer, hs6_code, data_year)
```

#### `trade_group_dependency_snapshots` / `trade_group_dependency_rows`
Persisted results for the Streamlit **Group dependencies** tab: parameter hash (`params_hash`), year, country group, and materialized export/importer-exposure rows so analyses can be reopened without recomputing. Populated by the app when you click **Load / compute** (not by a puller). See [`schema/create_tables.sql`](schema/create_tables.sql).

#### `fertilizer_production`
Fertilizer production and trade by country, from FAOSTAT.

```sql
id              SERIAL PRIMARY KEY
country         TEXT              -- ISO3
fertilizer_type TEXT              -- 'urea' | 'ammonia' | 'dap' | 'map' | 'npk'
metric          TEXT              -- 'production' | 'import' | 'export' | 'consumption'
value_tonnes    NUMERIC
data_year       INTEGER
source          TEXT
pulled_at       TIMESTAMPTZ
UNIQUE (country, fertilizer_type, metric, data_year)
```

#### `crop_production`
Crop production by country, from FAOSTAT and USDA PSD.

```sql
id              SERIAL PRIMARY KEY
country         TEXT              -- ISO3
crop            TEXT              -- 'wheat' | 'rice' | 'corn' | 'soybeans' | 'cotton'
metric          TEXT              -- 'production' | 'area_harvested' | 'yield' | 'imports' | 'exports'
value           NUMERIC
unit            TEXT              -- 'tonnes' | 'hectares' | 'tonnes_per_hectare'
data_year       INTEGER
source          TEXT
pulled_at       TIMESTAMPTZ
UNIQUE (country, crop, metric, data_year)
```

#### `commodity_prices`
Time series of commodity prices, from World Bank Pink Sheet.

```sql
id              SERIAL PRIMARY KEY
commodity       TEXT              -- 'crude_oil' | 'crude_oil_brent' | 'urea' | 'dap' | 'ammonia' | 'wheat' | 'rice' | 'corn'
price           NUMERIC
unit            TEXT              -- 'usd_per_barrel' | 'usd_per_tonne' etc.
data_year       INTEGER
data_month      INTEGER           -- null for annual
source          TEXT
pulled_at       TIMESTAMPTZ
UNIQUE (commodity, data_year, data_month)
```

#### `country_macro_indicators`
Population and national accounts from the World Bank WDI API, for normalising trade and supply data per country.

```sql
id              SERIAL PRIMARY KEY
country         TEXT              -- ISO3
indicator       TEXT              -- 'population' | 'gdp_current_usd' | 'gdp_per_capita_current_usd'
value           NUMERIC
unit            TEXT              -- 'persons' | 'current_usd'
data_year       INTEGER
source          TEXT
pulled_at       TIMESTAMPTZ
UNIQUE (country, indicator, data_year)
```

#### `food_balance_sheets`
FAOSTAT Food Balance Sheets (FBS): domestic supply, trade, and use of major food commodities in **tonnes** (imports in context of production, food, feed, etc.).

```sql
id              SERIAL PRIMARY KEY
country         TEXT              -- ISO3
commodity       TEXT              -- 'wheat' | 'rice' | 'corn' | 'soybeans' | 'cotton' (cotton = FBS Cottonseed item)
metric          TEXT              -- e.g. 'production', 'imports', 'exports', 'domestic_supply', 'food', 'feed'
value           NUMERIC           -- metric tonnes
unit            TEXT              -- 'tonnes'
data_year       INTEGER
source          TEXT
pulled_at       TIMESTAMPTZ
UNIQUE (country, commodity, metric, data_year)
```

#### `hs_code_lookup`
Reference table for HS 6-digit codes. **English descriptions** are loaded from the UN Comtrade public reference file ([`pull_comtrade_hs_lookup.py`](pullers/pull_comtrade_hs_lookup.py)). `category` is filled for codes that fall under the project’s V1 HS prefixes (same logic as the BACI loader); other codes have `category` null.

```sql
hs6_code        TEXT PRIMARY KEY
description     TEXT
category        TEXT              -- 'energy' | 'fertilizer_input' | 'fertilizer' | 'crop' (V1 scope only)
notes           TEXT
```

#### `country_lookup`
Reference table for countries.

```sql
iso3            TEXT PRIMARY KEY
iso2            TEXT
country_name    TEXT
region          TEXT
is_gulf_producer BOOLEAN         -- TRUE for: SAU, ARE, IRQ, KWT, QAT, IRN
```

#### `cepii_protee_hs6`
**Product-level trade elasticities (ProTEE 0.1)** — econometric parameters per HS6, *not* trade flows. See the **Loaders** section, subsection *`load_cepi_beyond_baci.py`*, for how this differs from `bilateral_trade`.

```sql
hs6_code                        TEXT PRIMARY KEY
flag_nonsignificant_at_1pct     BOOLEAN   -- CEPII "Zero": HS6 tariff coefficient not significant at 1%
flag_positive_significant       BOOLEAN   -- CEPII "Positive": positive significant elasticity in estimation
trade_elasticity                NUMERIC   -- published value (CSV "sigma"; may be HS4 substitute per CEPII)
hs_revision                     TEXT      -- 'HS2007' (CEPII documentation; not HS22 like current BACI)
source                          TEXT
pulled_at                       TIMESTAMPTZ
```

#### `cepii_geodep_import_dependence`
**GeoDep** — CEPII’s importer × HS6 × year **import-dependence diagnostics** (concentration, persistence, strategic sectors, top supplier), built from BACI. One summary row per `(country, hs6_code, data_year)` for 2019–2022 in the public file — *not* bilateral flows (those stay in `bilateral_trade`).

```sql
id                              SERIAL PRIMARY KEY
country                         TEXT      -- ISO3 (CSV iso_d)
hs6_code                        TEXT
data_year                       INTEGER
import_value                    NUMERIC   -- from CEPII import_dpt (import exposure in their construction)
hhi_import_concentration        NUMERIC   -- criterion 1: supplier HHI; >0.4 ⇒ concentrated imports
hhi_world_export_concentration  NUMERIC   -- criterion 2: world-export HHI; >0.4 ⇒ concentrated
import_to_export_ratio          NUMERIC   -- criterion 3: imports/exports; >1 ⇒ hard to substitute domestically; 9999 ⇒ null exports in source
flag_persistent_criteria        BOOLEAN   -- criterion 4: conditions hold ≥2 years in a 3-year window (CSV c4)
flag_import_dependent           BOOLEAN
sector_strategic_*              BOOLEAN   -- agrifood, chemicals, pharmaceuticals, steel, defense, transport, electronics, other
leading_exporter_code           TEXT      -- CEPII partner code (e.g. EUN, USA); not always ISO3
leading_exporter_share_pct      NUMERIC
source                          TEXT
pulled_at                       TIMESTAMPTZ
UNIQUE (country, hs6_code, data_year)
```

#### `usgs_mineral_statistics`
**USGS MCS** — long-form mineral commodity statistics by country and year from the MCS data CSV. Upsert key is **`record_fingerprint`** (content hash), not a natural business key.

```sql
id                          SERIAL PRIMARY KEY
record_fingerprint          TEXT UNIQUE   -- SHA-256 of row fields incl. Value + Notes
mcs_chapter                 TEXT
section                     TEXT
commodity                   TEXT
country_name                TEXT
country_iso3                TEXT          -- null for World total, Other countries, etc.
statistics                  TEXT
statistics_detail           TEXT
unit                        TEXT
data_year                   INTEGER       -- start year if Year column is a range
year_as_reported            TEXT          -- original Year cell (e.g. 2021–24)
value_numeric               NUMERIC
value_raw                   TEXT
notes                       TEXT
other_notes                 TEXT
is_critical_mineral_2025    BOOLEAN
source_file                 TEXT
source                      TEXT
pulled_at                   TIMESTAMPTZ
```

#### USGS `myb3-*.xlsx` yearbooks

Minerals Yearbook **country tables-only** releases. Each workbook has **three sheets**: narrative **`Text`** (skipped), **Table 1** (production — wide by year), **Table 2** (structure of the industry). **Sheet names differ** (`Table1` vs `Table 1`); the loader matches **`^table\s*1$`** / **`^table\s*2$`** case-insensitively.

**Loader:** `uv run python loaders/load_usgs.py facilities` — globs **`data/usgs/myb3*.xlsx`**.

| Source | Table | Grain / notes |
|--------|-------|---------------|
| Table 1 | `usgs_myb3_production` | Long rows: **melt** year columns; **`commodity_path`** (section + hierarchy); footnotes **`r` / `e`**; distinct from MCS `usgs_mineral_statistics`. |
| Table 2 | `usgs_country_mineral_facilities` | **Merged blocks** (wrapped owner/location concatenated); **`excel_row_start` / `excel_row_end`** (0-based sheet rows); columns found by **header text** (8 or 9 cols). **`Do.`** = ditto → **`commodity_leaf_resolved`** from previous non-`Do.` commodity cell. |

**Filename:** `myb3-{reference_year}-{slug}.xlsx` → slug map to ISO3 (e.g. `united-arab-emirates` → ARE). Unmapped slugs → run ends **partial** for that file.

`usgs_myb3_production`:

```
record_fingerprint   TEXT UNIQUE
country_iso3         TEXT
reference_year       INTEGER
commodity_path       TEXT
stat_year            INTEGER
value_raw            TEXT
value_numeric        NUMERIC
footnote             TEXT
unit_context         TEXT
source_file          TEXT
sheet_name           TEXT
source               TEXT
pulled_at            TIMESTAMPTZ
```

`usgs_country_mineral_facilities`:

```
record_fingerprint         TEXT UNIQUE
country_iso3               TEXT
reference_year             INTEGER
commodity_cell_raw         TEXT
commodity_leaf_resolved    TEXT
facility_path              TEXT
owner_operator             TEXT
location                   TEXT
capacity_raw               TEXT
capacity_numeric           NUMERIC
unit_note                  TEXT
sheet_name                 TEXT
excel_row_start            INTEGER
excel_row_end              INTEGER
source_file                TEXT
source                     TEXT
pulled_at                  TIMESTAMPTZ
```

**`commodity_cell_raw` / `commodity_leaf_resolved`:** For USGS **`Do.`** rows both hold the **resolved** commodity text (same value) so nothing is blank in the UI.

#### `gem_tracker_rows`
Rows from [Global Energy Monitor](https://www.globalenergymonitor.org/) **`.xlsx`** trackers loaded by [`loaders/load_gem_xlsx.py`](loaders/load_gem_xlsx.py). Each spreadsheet **data row** is one database row; column headers from row 1 become keys on **`payload`** (JSONB). Provenance: **`source_file`** (workbook filename), **`sheet_name`**, **`excel_row_1based`** (Excel row index; header is row 1). **`source`** is the loader script name; **`pulled_at`** is insert time.

```sql
id                  SERIAL PRIMARY KEY
source_file         TEXT NOT NULL
sheet_name          TEXT NOT NULL
excel_row_1based    INTEGER NOT NULL
payload             JSONB NOT NULL
source              TEXT NOT NULL
pulled_at           TIMESTAMPTZ NOT NULL
UNIQUE (source_file, sheet_name, excel_row_1based)
```

Reloading a sheet **deletes** existing rows for that `(source_file, sheet_name)` pair, then inserts fresh rows (idempotent full refresh per sheet).

---

## HS Codes in Scope (V1)

These are the codes configured across all pullers and loaders.
Edit the `HS_CODES` list at the top of each script to add or remove.

| Category | HS Code | Description |
|---|---|---|
| Energy | 2709 | Crude oil |
| Energy | 2711 | LNG / natural gas |
| Energy | 2710 | Refined products (diesel, jet fuel, naphtha) |
| Fertilizer input | 2814 | Ammonia |
| Fertilizer | 3102 | Nitrogenous fertilizers (urea, ammonium nitrate) |
| Fertilizer | 3103 | Phosphatic fertilizers |
| Fertilizer | 3104 | Potassic fertilizers |
| Fertilizer | 3105 | NPK blends, DAP, MAP |
| Crop | 1001 | Wheat |
| Crop | 1006 | Rice |
| Crop | 1005 | Corn / maize |
| Crop | 1201 | Soybeans |
| Crop | 5201 | Cotton |

---

## Pullers

Pullers fetch data from a remote source over HTTP (a JSON/XML **API**, a published **file URL**, or similar) and upsert results into Supabase.
They are designed to be run manually and are safe to re-run (idempotent).

All pullers follow this pattern:
1. Configuration block at the top — edit years, codes, countries here
2. Fetch with `requests` (basic retry for transient errors where appropriate)
3. Upsert to Supabase
4. Log the run to `pipeline_runs`

### `pull_eia.py` — EIA Open Data

**Source:** U.S. Energy Information Administration
**URL:** https://www.eia.gov/opendata/
**API key:** Optional but recommended — free at https://www.eia.gov/opendata/register.php
**Writes to:** `energy_trade_flows`
**Data available:** US and international oil/gas production, imports, exports
**Refresh cadence:** Monthly
**Known limitation:** Strong on US and OECD flows; less granular for intra-Gulf trade

```bash
uv run python pullers/pull_eia.py
```

### `pull_faostat.py` — FAOSTAT

**Source:** UN Food and Agriculture Organization  
**URL:** https://www.fao.org/faostat/

**Crops (`crop_production`):** Bulk normalized ZIP over HTTP (`Production_Crops_Livestock_E_All_Data_(Normalized).zip`). No FAO account required. Optional local file: set **`FAOSTAT_ZIP_PATH`** to skip the download.

**Fertilizers (`fertilizer_production`):** Uses the **`faostat`** Python package and the **FAOSTAT REST API** (`faostat.get_data_df`), not the bulk ZIP. The **Inputs Fertilizers** bulk URL is often **CloudFront/geo or rate blocked**; the API avoids that. **FAOSTAT API v2 requires credentials:** **`FAOSTAT_API_TOKEN`** (JWT) or **`FAOSTAT_USERNAME`** + **`FAOSTAT_PASSWORD`** in `.env` (see [`.env.example`](.env.example)).

**Important API detail:** The server does **not** accept `area=all` as a literal filter — it returns an **empty** result. The puller loads **FAO country area codes** from the API metadata (`aggregate_type` country rows), requests **`pars={'area': [...], 'year': [...]}`** in **chunks** (default **40** areas per call, **`FAOSTAT_API_AREA_CHUNK`**), and maps the **`Area`** name column to **ISO3** (with **`FAOSTAT_API_AREAS`** to override the code list if needed). **`FAOSTAT_API_SLEEP_SEC`** (default `0.2`) spaces out chunk requests.

Dataset code defaults to **`RFB`** (*fertilizers by product* — urea, DAP, MAP, etc.). Set **`FAOSTAT_FERTILIZER_API_CODE=RFN`** for *by nutrient* (N/P/K) if you extend the item mapping. **`FAOSTAT_API_PAGE_LIMIT`** (default `50000`) is the per-request pagination page size inside the `faostat` client.

**Never put real tokens in `.env.example`** — placeholders only; rotate any token that was ever committed there.

**CLI — run datasets independently** (avoids re-streaming large ZIPs when only one leg needs a retry):

```bash
uv run python pullers/pull_faostat.py --dataset all         # default: crops, fertilizers, FBS
uv run python pullers/pull_faostat.py --dataset crops       # crop bulk ZIP only
uv run python pullers/pull_faostat.py --dataset fertilizer  # API only (needs FAO auth)
uv run python pullers/pull_faostat.py --dataset fbs         # Food Balance Sheets bulk ZIP only (no FAO API)
```

**Food Balance Sheets (`food_balance_sheets`):** Normalized bulk ZIP over HTTP — `FoodBalanceSheets_E_All_Data_(Normalized).zip` under FAOSTAT production bulks (same host as crop bulk). Optional local file: **`FAOSTAT_FBS_ZIP_PATH`** skips the download. The puller keeps V1 commodities only (wheat, rice, corn, soybeans, cotton); **cotton** maps to the FBS *Cottonseed* item. Mass elements only (`1000 t` → stored as **tonnes**): production, imports, exports, domestic supply, food, feed, losses, processing, stock variation, etc. (see `FBS_ELEMENT_MAP` in the script).

**Writes to:** `crop_production`, `fertilizer_production`, `food_balance_sheets`  
**Data available:** Crop area/production/yield for V1 crops; fertilizer production / import / export / agricultural use (as mapped in script) for urea, ammonia, DAP, MAP, NPK-style items; FBS supply and use for the V1 food commodities above  
**Refresh cadence:** Annual (FAO updates; data often lags 18–24 months)  
**Known limitation:** Fertilizer consumption *by crop* is not in FAOSTAT — use IFA or other sources for that linkage (manual for V1). FBS file is large (~600MB+ CSV inside ZIP); full runs take several minutes.

If fertilizer auth is missing or the API fails, the script still upserts crops when you use `--dataset all` or `--dataset crops`, logs **`pipeline_runs.status=partial`**, and records the error message (honest split success).

### `pull_worldbank.py` — World Bank Pink Sheet

**Source:** World Bank Commodity Markets (Prospects Group — Pink Sheet data)  
**Human-facing page:** https://www.worldbank.org/en/research/commodity-markets  
**API key:** None required (public file download)

**Data file:** The script downloads the monthly historical Excel workbook with `requests.get()` — no authentication. The URL is defined in code as **`PINK_SHEET_MONTHLY_XLSX_URL`**. Current canonical link (until the World Bank republishes the bundle):

`https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx`

**Parse:** `pandas.read_excel()` (requires **`openpyxl`**). The workbook is **wide**: **dates down rows**, **commodities across columns**. The puller **melts** that layout to long form, filters to the Pink Sheet columns below, then upserts.

**Columns ingested** (Excel headers; match exactly in the workbook):

| Excel column   | Stored as `commodity` | Typical `unit`   |
|----------------|------------------------|------------------|
| Crude oil      | `crude_oil`            | `usd_per_barrel` |
| Brent          | `crude_oil_brent`      | `usd_per_barrel` |
| Urea           | `urea`                 | `usd_per_tonne`  |
| DAP            | `dap`                  | `usd_per_tonne`  |
| Ammonia        | `ammonia`              | `usd_per_tonne`  |
| Wheat, US HRW  | `wheat`                | `usd_per_tonne`  |
| Rice, Thai 5%  | `rice`                 | `usd_per_tonne`  |
| Maize          | `corn`                 | `usd_per_tonne`  |

**Writes to:** `commodity_prices`  
**Data available:** Monthly prices for those series going back decades  
**Refresh cadence:** Monthly (re-run after World Bank updates the spreadsheet)

**URL maintenance:** When the World Bank republishes the documentation bundle (roughly once or twice a year), the **document hash** in the `thedocs` URL path changes and the old link may return **404**. The script records **`status='error'`** in **`pipeline_runs`** with a message to find the new **`CMO-Historical-Data-Monthly.xlsx`** on the commodities / Pink Sheet documentation page and update **`PINK_SHEET_MONTHLY_XLSX_URL`** in `pull_worldbank.py`, then exits without crashing.

**Known limitation:** Prices only — no volume or trade flow data

```bash
uv run python pullers/pull_worldbank.py
```

### `pull_worldbank_wdi.py` — World Development Indicators (macro context)

**Source:** World Bank Open Data  
**URL:** https://data.worldbank.org/ — API `https://api.worldbank.org/v2/`  
**API key:** None required  

**Indicators (by default):** `SP.POP.TOTL` (population), `NY.GDP.MKTP.CD` (GDP, current US$), `NY.GDP.PCAP.CD` (GDP per capita, current US$). The v2 API accepts **one indicator per request**; the script issues three paginated downloads per run.

**Writes to:** `country_macro_indicators`  
**Data available:** Annual series by country (ISO 3166-1 members only; World Bank regions and aggregates are excluded via `pycountry`).  
**Refresh cadence:** Annual (re-run when you extend `YEARS` or after WDI updates).  
**Known limitation:** Indicator set is fixed in script config; extend `WDI_INDICATORS` to add more series.

```bash
uv run python pullers/pull_worldbank_wdi.py
```

### `pull_usda_psd.py` — USDA Production, Supply and Distribution

**Source:** USDA Foreign Agricultural Service  
**URL:** https://api.fas.usda.gov/api/psd (Open Data API) — human UI: https://apps.fas.usda.gov/psdonline/  
**API key:** **Required** — register at https://api.fas.usda.gov and set **`USDA_FAS_API_KEY`** in `.env` (see [`.env.example`](.env.example)).  
**Writes to:** `crop_production`
**Data available:** Production, consumption, imports, exports, ending stocks by crop and country
**Refresh cadence:** Monthly (USDA updates on a fixed schedule)
**Known limitation:** Major crops only (wheat, corn, rice, soybeans, cotton) — no minor crops

```bash
uv run python pullers/pull_usda_psd.py
```

### `pull_comtrade_hs_lookup.py` — UN Comtrade HS classification (HS6 text)

**Source:** UN Comtrade Data API reference cache  
**URL:** https://comtrade.un.org/data/cache/classificationHS.json  
**API key:** None (public JSON)

**Writes to:** `hs_code_lookup`  
**Data available:** All **6-digit HS** entries in the Comtrade “HS (as reported)” tree (~6,900 codes) with **English** `text` labels. Descriptions are suitable for joining to `bilateral_trade.hs6_code`; they follow the Comtrade reference nomenclature (close to recent WCO HS editions; your BACI files may use a specific HS revision such as HS 2022 — verify critical codes if legal precision matters).  
**Refresh cadence:** Occasional (re-run after Comtrade updates the reference file)

```bash
uv run python pullers/pull_comtrade_hs_lookup.py
```

---

## Loaders

Loaders ingest files you have downloaded manually.
They are also idempotent — safe to re-run.

### `load_baci.py` — BACI Bilateral Trade

**Source:** CEPII (Centre d'Études Prospectives et d'Informations Internationales)
**Download URL:** http://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37
**Registration:** Free, requires account creation
**Writes to:** `bilateral_trade`
**Data available:** Bilateral trade values and quantities at HS 6-digit level, 200+ countries, 1995–present
**Refresh cadence:** Annual (new year released approximately 12 months after reference year)
**File size:** ~150MB per year unzipped

#### How to get the files

1. Go to the CEPII BACI download page (link above)
2. Create a free account and log in
3. Download the HS 2022 revision files for your desired years (start with the most recent 3-5 years)
4. Unzip into `data/baci/` — filenames will look like `BACI_HS22_Y2022_V202401.csv`

#### Running the loader

```bash
# Load a specific year
uv run python loaders/load_baci.py --year 2022

# Load all CSV files found in data/baci/
uv run python loaders/load_baci.py --all

# Optional: all HS6 lines for one or more exporters — Streamlit “Exporter & partners” / fair multi-country groups
uv run python loaders/load_baci.py --all --exporter-full-hs ARE
# Repeat the flag so every country in a Gulf (or other) group has the same HS6 coverage:
# uv run python loaders/load_baci.py --all --exporter-full-hs SAU --exporter-full-hs ARE --exporter-full-hs IRQ ...

# Optional: repeat --importer-full-hs for each partner ISO3 so the app can show full supplier
# concentration (every exporter → that importer × HS6). Combine with the lines above as needed.
uv run python loaders/load_baci.py --all --exporter-full-hs ARE --importer-full-hs IND

# Optional: load global BACI flows for specific HS6 codes (all exporters × all importers)
# — use when you need a true “world exports” denominator for those products (e.g. Group dependencies).
uv run python loaders/load_baci.py --year 2024 --hs6-codes "270900,851713"
# Or list codes one per line in a file:
# uv run python loaders/load_baci.py --year 2024 --hs6-codes-file path/to/hs6.txt
```

The loader filters to only the HS codes in scope (defined at the top of the script) so it does
not try to load all 5,000 products. This keeps DB size manageable and load time fast.
**`--exporter-full-hs`** (repeat once per exporter) and **`--importer-full-hs`** *add* rows outside that list for the given ISO3 legs (other flows stay V1-filtered). Using full-HS for **one** country only makes that country dominate multi-country **Group dependencies** for HS6 outside the V1 list — repeat **`--exporter-full-hs`** for every group member you care about, or use **`--hs6-codes`** for the products you analyze.
**`--hs6-codes`** / **`--hs6-codes-file`** *add* rows for those six-digit HS6 codes globally (all partner legs), which is what makes “% of world exports” meaningful for selected products.

**Known limitation:** Annual only, no monthly granularity. Use Comtrade API (future v2)
for more recent or more specific bilateral queries.

### `load_cepi_beyond_baci.py` — CEPII ProTEE, GeoDep, zip archives

**Source:** CEPII — [Product Level Trade Elasticities (ProTEE)](https://www.cepii.fr/cepii/en/bdd_modele/bdd_modele_item.asp?id=35), [GeoDep](https://www.cepii.fr/cepii/en/bdd_modele/bdd_modele_item.asp?id=41), [WTFC](https://www.cepii.fr/cepii/en/bdd_modele/bdd_modele_item.asp?id=29), [CHELEM](https://www.cepii.fr/cepii/en/bdd_modele/bdd_modele_item.asp?id=17)  
**Registration:** Public CSV downloads for ProTEE and GeoDep; WTFC/CHELEM zips as published  
**Writes to:** `cepii_protee_hs6`, `cepii_geodep_import_dependence` (CSV legs only)

The loader’s module docstring is the full technical reference. Below is what each piece **means** and how it **differs from data you already have** in Supabase.

#### Versus BACI (`bilateral_trade`)

| Aspect | BACI in this project | ProTEE | GeoDep |
|--------|----------------------|--------|--------|
| **Question it answers** | “How much did **A export to B** in product **k** (USD, tonnes)?” | “How **price-sensitive** are imports of product **k** (elasticity)?” | “Is country **c**’s demand for product **k** **geographically concentrated / dependent**, and **who leads**?” |
| **Grain** | Exporter × importer × HS6 × year | HS6 **only** (global product parameter) | Importer × HS6 × year **summary** |
| **Nature** | **Factual flows** | **Estimated parameter** (econometrics) | **Derived indicators** from BACI |
| **HS vintage** | Your files: **HS 2022** | CEPII: **HS 2007** | Aligned with GeoDep/BACI vintage in CEPII release |
| **Joining** | Join to `hs_code_lookup`, EIA, etc. | Join to HS6 **only with a concordance** HS2007↔HS2022 if you need strict alignment | Can join to `bilateral_trade` on `(importer, hs6, year)` **approximately** (same HS6 digits; revision mismatch still applies) |

#### ProTEE (loaded from `data/cepi/ProTEE_0_1.csv`)

- **Represents:** CEPII’s published **import-demand elasticity** per HS6 from tariff variation (gravity). Values are in column `sigma` in the file; CEPII’s site calls this the elasticity.
- **Flags:** `zero` → stored as `flag_nonsignificant_at_1pct` (original HS6 estimate not significant at 1%). `positive` → `flag_positive_significant`. When those apply, CEPII **replaces** the HS6 point estimate with the **HS4 sector average** in the published column — interpret `trade_elasticity` together with the flags.
- **Not a substitute for:** `bilateral_trade`, `commodity_prices`, or tariffs in your DB — it is **meta** for simulation and sensitivity analysis.

#### GeoDep (loaded from `data/cepi/geodep_data.csv`)

- **Represents:** Official CEPII **import dependency** screening for **2019–2022** (see their methodology: four criteria — import HHI, world-export HHI, import/export ratio with **9999** when exports are missing, and persistence over a three-year window). Strategic sector dummies and **leading exporter** + **share** describe *where* dependence sits.
- **`import_value`:** Maps CSV `import_dpt` (large, skewed magnitudes — **import exposure** in CEPII’s construction, not an HHI).
- **`leading_exporter_code`:** CEPII/CHELEM-style codes (**not** always ISO3); treat as an opaque partner key unless you map CEPII’s nomenclature.
- **Not a substitute for:** BACI rows — GeoDep **aggregates** information into one diagnostic row per country–product–year; it does not list every partner.

#### WTFC and CHELEM zips in `data/cepi/` (not loaded by this script)

- **WTFC ([World Trade Flows Characterization](https://www.cepii.fr/cepii/en/bdd_modele/bdd_modele_item.asp?id=29)):** Reconciled **unit values** at exporter–importer–HS6–year plus **trade type** (e.g. one-way vs intra-industry) and **price range** vs other flows of the same product. **HS96** in your bundle — older than BACI HS22.
- **`price_range_CHELEM_*.zip` / `type_CHELEM_*.zip`:** Same **WTFC** ideas on **CHELEM** product categories (71 goods, etc.), not HS6 BACI lines.
- **Difference vs BACI:** BACI targets **consistent values and quantities** for macro bilateral trade; WTFC targets **comparable unit values and flow typology**. Complementary, not redundant.
- **Why no loader yet:** Yearly WTFC CSVs are **very large** (hundreds of MB each). Add a dedicated streaming loader with HS/year filters if you need this in Postgres.

```bash
uv run python loaders/load_cepi_beyond_baci.py protee   # ~5k HS6 rows
uv run python loaders/load_cepi_beyond_baci.py geodep # ~3M rows; long run; safe to re-run on failure
uv run python loaders/load_cepi_beyond_baci.py all
```

Optional: set `GEODEP_HS_PREFIXES` in the script (same idea as `HS_CODES` in `load_baci.py`) to load only products under selected HS prefixes and shrink runtime.

**Known limitation:** Large GeoDep uploads may hit transient HTTP/TLS errors; re-run — upserts are idempotent.

### `load_jodi.py` — JODI oil and gas CSVs

**Source:** JODI — manual CSV export (same column layout for oil and gas products).  
**Writes to:** `jodi_energy_observations`  
**Semantics:** One row per reporter (`REF_AREA` ISO2), month, `ENERGY_PRODUCT`, `FLOW_BREAKDOWN`, and `UNIT_MEASURE`. Numeric values go to `obs_value`; missing or confidential markers stay in `obs_value_raw`. `country` is ISO3 mapped from `REF_AREA` for joins to the rest of the schema.

**Year filter:** The gas staging file can cover **~2009–present** (~300k+ rows full history). By default the loader keeps **`data_year >= 2020`** (`MIN_DATA_YEAR` in the script). Use **`--all-years`** for the full file, or **`--min-year 2015`** (etc.) for a custom floor.

```bash
uv run python loaders/load_jodi.py
uv run python loaders/load_jodi.py --file STAGING_world_NewFormat.csv
uv run python loaders/load_jodi.py --all-years
uv run python loaders/load_jodi.py --min-year 2018
```

### `load_usgs.py` — USGS Mineral Commodity Summaries (MCS)

**Source:** USGS National Minerals Information Center — MCS data table (CSV).  
**Writes to:** `usgs_mineral_statistics`  
**Semantics:** One row per source line. **`record_fingerprint`** is a SHA-256 of chapter, section, commodity, country, statistic fields, **reported year label** (see below), value, and notes — required because some rows share the same commodity/country/statistic/unit/year label in the file. **`data_year`** is the calendar start year; **`year_as_reported`** keeps the original `Year` cell (e.g. `2021–24`). **`value_numeric`** is set only for plain numeric values (commas stripped); symbols like `>95`, `W`, or ranges stay **`value_raw`** only. **`country_iso3`** is null for aggregates (*World total*, *Other countries*, etc.).

```bash
uv run python loaders/load_usgs.py           # default: mcs
uv run python loaders/load_usgs.py mcs --file MCS2026_Commodities_Data.csv
uv run python loaders/load_usgs.py facilities   # myb3-*.xlsx → usgs_myb3_production + usgs_country_mineral_facilities
```

**myb3 xlsx (`facilities`):** Ingests all matching workbooks under `data/usgs/`. Semantics and edge cases: [HANDOVER.md](HANDOVER.md) **USGS myb3 yearbooks**. Schema: [`schema/create_tables.sql`](schema/create_tables.sql); catalog: [`schema/seed_table_catalog.sql`](schema/seed_table_catalog.sql). After changing the parser, run **`uv run python scripts/validate_myb3_table2.py`** (ditto expansion in O/L/C, commodity `Do.` replay, **`commodity_cell_raw`** matches **`commodity_leaf_resolved`** on ditto rows, idempotent re-parse).

**Table 2 facility geocoding (optional, for map coordinates):** Apply migration [`schema/migrations/20260419_usgs_facilities_geocode_columns.sql`](schema/migrations/20260419_usgs_facilities_geocode_columns.sql), then run **`uv run python scripts/geocode_usgs_facilities.py`**. The script queries **OpenStreetMap Nominatim** (public instance: ~1 request per second; set **`NOMINATIM_USER_AGENT`** to a contact string). It fills **`geocode_lat`**, **`geocode_lon`**, **`geocode_query`**, **`geocode_source`**, and **`geocoded_at`** on **`usgs_country_mineral_facilities`**. The Explore more USGS **Yearbook Table 2** map prefers these coordinates; follow OSM/Nominatim attribution in the app.

### `load_gem_xlsx.py` — Global Energy Monitor (default bundle)

**Writes to:** `gem_tracker_rows` — one row per Excel data line; headers become keys on **`payload`** (JSONB). **`About`** and **`Metadata`** sheets are skipped unless you use `--include-meta-sheets` with `--file`.

**Default bundle** (no args): eight workbooks under `data/globalenergymonitor/`, data sheets only — keep filenames aligned with [`DEFAULT_WORKBOOKS`](loaders/load_gem_xlsx.py) in the script.

| Workbook `.xlsx` | Sheet(s) loaded |
|------------------|-----------------|
| `Global-Cement-and-Concrete-Tracker_July-2025.xlsx` | Plant Data |
| `Global-Iron-Ore-Mines-Tracker-August-2025-V1.xlsx` | Main Data |
| `Plant-level-data-Global-Chemicals-Inventory-November-2025-V1.xlsx` | Plant data |
| `Plant-level-data-Global-Iron-and-Steel-Tracker-March-2026-V1.xlsx` | Plant data; Plant capacities and status; Plant production |
| `GEM-GOIT-Oil-NGL-Pipelines-2025-03.xlsx` | Pipelines |
| `GEM-GGIT-LNG-Terminals-2025-09.xlsx` | LNG Terminals |
| `GEM-GGIT-Gas-Pipelines-2025-11.xlsx` | Pipelines |
| `Global-Integrated-Power-March-2026-II.xlsx` | Power facilities; Regions, area, and countries |

**Performance:** `Global-Integrated-Power-*.xlsx` is large (~180k+ data rows on **Power facilities** alone); a full default run can take **many minutes**. Use `--dry-run` first, or `--file Some.xlsx` to refresh one workbook.

**CLI notes:** With `--file`, every sheet except **About** / **Metadata** is loaded. **`--sheets`** accepts comma-separated names and does **not** support sheet titles that contain commas (e.g. use `--file` without `--sheets` for *Regions, area, and countries*). **`--sheets`** is only valid with a **single** `--file`.

```bash
uv run python loaders/load_gem_xlsx.py --dry-run   # parse only; print row counts
uv run python loaders/load_gem_xlsx.py             # full default bundle; needs gem_tracker_rows in schema
uv run python loaders/load_gem_xlsx.py --file Global-Integrated-Power-March-2026-II.xlsx
```

---

## Running Everything (First Time)

```bash
# 1. Set up environment
uv sync
cp .env.example .env
# edit .env with your Supabase credentials

# 2. Initialise schema (SQL Editor, psql, or Supabase MCP migration — see §3 above)

# 3. Reference HS text (optional before BACI; improves labels in the app)
uv run python pullers/pull_comtrade_hs_lookup.py

# 4. Load BACI (largest bilateral dataset) — download CSVs to data/baci/ first
uv run python loaders/load_baci.py --all

# 4b. Optional — CEPII ProTEE + GeoDep CSVs in data/cepi/ (see Loaders section)
# uv run python loaders/load_cepi_beyond_baci.py all

# 4c. Optional — USGS in data/usgs/
# uv run python loaders/load_usgs.py mcs
# uv run python loaders/load_usgs.py facilities   # myb3-*.xlsx

# 4d. Optional — GEM .xlsx under data/globalenergymonitor/ (see load_gem_xlsx default bundle)
# uv run python loaders/load_gem_xlsx.py --dry-run
# uv run python loaders/load_gem_xlsx.py

# 5. Run remaining pullers (FAOSTAT --dataset all needs FAO API creds for the fertilizer leg; use --dataset crops|fbs to skip)
uv run python pullers/pull_eia.py
uv run python pullers/pull_faostat.py --dataset all
uv run python pullers/pull_worldbank.py
uv run python pullers/pull_worldbank_wdi.py
uv run python pullers/pull_usda_psd.py

# 6. Launch the explorer
uv run streamlit run app/streamlit_app.py
```

---

## Refreshing Data

| Script | When to re-run |
|---|---|
| `load_baci.py` | Annually, when CEPII releases new year |
| `load_cepi_beyond_baci.py` | When CEPII refreshes ProTEE / GeoDep CSVs |
| `load_jodi.py` | When you refresh JODI CSV exports under `data/jodi/` |
| `load_usgs.py` | New MCS CSV (`mcs`); when `myb3-*.xlsx` change, re-run `facilities` |
| `load_gem_xlsx.py` | When default GEM `.xlsx` files change under `data/globalenergymonitor/` |
| `pull_eia.py` | Monthly |
| `pull_faostat.py` | Annually; `--dataset` crops / fertilizer / fbs / all (FBS bulk is large) |
| `pull_worldbank.py` | Monthly |
| `pull_worldbank_wdi.py` | Annual (or when extending years) |
| `pull_usda_psd.py` | Monthly |
| `pull_comtrade_hs_lookup.py` | When Comtrade updates HS reference, or once after schema init |

Check what is in your database and when it was last pulled:

```sql
SELECT script_name, parameters, rows_written, status, completed_at
FROM pipeline_runs
ORDER BY completed_at DESC;
```

---

## Streamlit Data Explorer (V1)

The V1 app is **not** a generic “pick any table” grid. Navigation is a **sidebar “Section” radio** (eight areas). That matters for performance: Streamlit’s `st.tabs` runs **every** tab’s Python on **each** widget interaction, which used to reload the whole dashboard (all eight areas) whenever you moved a slider. The sidebar pattern runs **only the selected section** on each rerun.

The **Group dependencies** section is also wrapped in **`@st.fragment()`**, so sliders and controls inside it can update with a **partial rerun** instead of re-executing the rest of the app. Heavy Supabase RPCs there run on **Load / compute** (or when viewing a cached result for the same parameters / a loaded snapshot), not on every widget tweak.

There is no derived Hormuz exposure score yet — the goal is to sanity-check data and joins.

| Section | What it shows |
|-----|----------------|
| **Prices over time** | `commodity_prices` (World Bank Pink Sheet monthly series) |
| **Who trades what** | Top exporters/importers from `bilateral_trade` by HS6 and year (server-filtered by year × HS6) |
| **Country profile** | Per-country import/export totals by HS6 for one year (server-filtered; joins `hs_code_lookup` when present) |
| **Exporter & partners** | Pick an **exporter** (Gulf pinned at top), year, HS search, and top‑N, then click **Load data**. Partner and supplier panels are **stepwise** buttons to avoid unwanted queries. Uses trade RPCs in [`schema/rpc_trade_dashboards.sql`](schema/rpc_trade_dashboards.sql) for fast aggregates. Best‑fidelity supplier concentration requires importer legs loaded via `load_baci.py --importer-full-hs`. |
| **Group dependencies** | Choose a **country group** (default Gulf ISO3), year, HS filter, and top‑N HS6 by group export share of **world** flows. Shows within‑group concentration and importer exposure to the group for a selected HS6; can **save snapshots** to `trade_group_dependency_*` tables; **Saved snapshots** expander can load a prior run without recomputing. PostgREST RPC calls must use Postgres parameter names **`p_data_year`** / **`p_hs6_code`** (not `data_year` / `hs6_code`). Requires [`schema/rpc_trade_dashboards.sql`](schema/rpc_trade_dashboards.sql) (group RPCs). For credible world shares per HS6, load global flows for those codes via `load_baci.py --hs6-codes` (or the UI will reflect only what is in `bilateral_trade`). |
| **Crop production** | `crop_production` rankings by crop, metric, year |
| **Pipeline status** | Recent `pipeline_runs` and latest run per script |
| **Explore more** | Additional datasets (nested tabs — all read Supabase with **bounded** queries where tables are large) |

**Explore more** sub-tabs: **`table_catalog`** (data dictionary); **`energy_trade_flows`** (EIA); **`fertilizer_production`**; **`country_macro_indicators`** (WDI); **`food_balance_sheets`**; **`cepii_protee_hs6`** / **`cepii_geodep_import_dependence`** (GeoDep filtered + row limit); **`jodi_energy_observations`** (requires country and/or product/flow filters); **USGS** — `usgs_mineral_statistics`, `usgs_myb3_production`, `usgs_country_mineral_facilities` (each mode requires ISO3 and/or other filters); **`gem_tracker_rows`** (exact workbook filename + sheet + limit); **`hs_code_lookup`** (browse/filter); **`country_lookup`**.

There is still no generic “dump any table” grid. **Not in Postgres yet (no loader):** CEPII WTFC/CHELEM zips. **GEM GIS** `.zip` assets are deferred.

```bash
uv run streamlit run app/streamlit_app.py
```

The app should read Supabase settings from `.env` via python-dotenv — use `get_read_client()` with anon/publishable keys for safe reads, or `get_client()` with a server key only for a trusted local explorer (see Environment Setup above).

---

## Known Gaps and Limitations (V1)

| Gap | Impact | Planned fix |
|---|---|---|
| FAOSTAT crop data lags 18-24 months | Production figures are not current year | Supplement with USDA PSD (more timely) |
| FAOSTAT fertilizer bulk ZIP blocked; API needs real `area`/`year` params | Empty frames if `area` is wrong (e.g. string `all`) | Puller chunks FAO area codes + years; use API + credentials; `--dataset fertilizer` |
| Fertilizer consumption by crop not available | Cannot directly link fertilizer imports to specific crops | Requires IFA reports — manual for now |
| No vessel-level Hormuz transit data | Cannot count actual tankers transiting | Use EIA factsheet numbers as static reference |
| BACI is annual only | No monthly trade flow granularity | Add Comtrade API in v2 for recent months |
| Petrochemicals excluded | Plastics and chemicals exposure not mapped | Add HS 2901-2902, 3901-3904 in v2 |
| Exposure scores not computed | Raw data only, no derived Hormuz dependency index | Build iteratively once data is validated |
| Pink Sheet XLSX URL can 404 after WB republish | Pull fails until `PINK_SHEET_MONTHLY_XLSX_URL` is updated; check `pipeline_runs.error_message` | Manual URL refresh from World Bank doc page (script logs instructions) |
| USGS myb3 not loaded | Empty `usgs_myb3_*` tables | Place `myb3-*.xlsx` under `data/usgs/` and run `uv run python loaders/load_usgs.py facilities` |
| GEM default bundle not loaded | Empty or stale `gem_tracker_rows` | Ensure DDL from [`schema/create_tables.sql`](schema/create_tables.sql); place default `.xlsx` files under `data/globalenergymonitor/`; run `uv run python loaders/load_gem_xlsx.py` (or `--file` for one workbook) |
| JODI gas CSV is long history | Large row count if you use `--all-years` | Default `load_jodi.py` keeps `data_year >= 2020` (~39% of current gas export); override with `--min-year` / `--all-years` |

---

## Dependencies

Managed by uv. Key packages:

```
supabase          # Supabase Python client
pandas            # data manipulation
openpyxl          # Excel (.xlsx) for Pink Sheet puller (pandas read_excel)
requests          # HTTP fetch (APIs and published files)
python-dotenv     # .env loading
streamlit         # data explorer UI
tqdm              # progress bars for loaders
pycountry         # ISO numeric / alpha country codes (e.g. BACI, FAOSTAT)
faostat           # FAOSTAT official API client (fertilizer path in pull_faostat)
```

Full dependency list lives in `pyproject.toml`. After `uv sync` the exact versions
are pinned in `uv.lock` — commit the lockfile so the environment is fully reproducible.

---

## Adding a New Data Source

To add a new puller, copy `pullers/pull_worldbank.py` as your starting template.
It is the simplest puller and the most heavily commented. That template uses a **published file URL** and `pandas.read_excel`; other pullers may call a JSON API instead — the header block and `pipeline_runs` logging pattern stay the same.

Every script must follow this structure:

```python
# ============================================================
# SCRIPT:  pull_sourcename.py
# SOURCE:  Full source name
# URL:     https://...
# API KEY: required / not required / where to get one
# WRITES:  table_name
# REFRESH: monthly / annual
# NOTES:   any known limitations or quirks
# ============================================================

# --- CONFIGURATION — edit these values before running --------
YEARS     = [2020, 2021, 2022, 2023]
HS_CODES  = ["270900", "271100"]   # only relevant for trade scripts
COUNTRIES = None                    # None = all available countries
# -------------------------------------------------------------

# ... fetch → transform → upsert → log pattern below
```

---

## Future Versions

**V2 — Enrichment**
- Add UN Comtrade API for recent bilateral data (fills BACI annual lag)
- Add petrochemicals (HS 2901-2902, 3901-3904)
- Add IFA fertilizer consumption data (manual PDF extraction)
- **UNCTAD:** Merchandise Trade Matrix and product / import concentration indices (free UNCTAD data) for country-risk and exposure scoring — deferred as likely overkill until the V3 analysis layer

**V3 — Analysis layer**
- Compute Hormuz exposure scores per country
- Fertilizer import dependency index
- Food security cascade model: fertilizer shock → crop yield impact → import dependency
- Network graph of supply chain dependencies

**V4 — Dashboards**
- Built on top of this data layer using Claude Code or Cursor
- Choropleth maps, dependency trees, price shock scenario modelling
- Hosted Streamlit deployment