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
│   └── baci/                        # manually downloaded BACI CSV files go here
│       └── .gitkeep                 # keeps the folder in git, ignores contents
│
├── schema/
│   └── create_tables.sql            # full Supabase schema — run once
│
├── pullers/                         # scripts that fetch remote data (HTTP API or published file) and write to Supabase
│   ├── pull_eia.py                  # EIA: crude/LNG/refined product flows
│   ├── pull_faostat.py              # FAOSTAT: crop production + food trade
│   ├── pull_worldbank.py            # World Bank Pink Sheet: commodity prices
│   └── pull_usda_psd.py             # USDA PSD: crop supply/demand by country
│
├── loaders/                         # scripts that ingest manually downloaded files
│   └── load_baci.py                 # BACI: bilateral trade flows (HS6, 200 countries)
│
├── app/
│   └── streamlit_app.py             # data explorer UI — reads from Supabase only
│
└── utils/
    ├── supabase_client.py           # shared Supabase connection
    └── pipeline_logger.py           # shared pipeline run logging
```

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

### 3. Initialise the database

Run the schema file once against your Supabase project:

```bash
# Option A: paste schema/create_tables.sql into Supabase SQL Editor and run
# Option B: use psql with your connection string
psql "$DATABASE_URL" -f schema/create_tables.sql
```

---

## Database Schema

All tables follow these conventions:
- `source` — name of the script that wrote the row (e.g. `pull_eia`)
- `pulled_at` — UTC timestamp when the row was inserted/upserted
- `data_year` — the reference year the data describes (not the pull year)
- Primary keys are composite natural keys so upserts are safe to re-run

### Tables

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

#### `hs_code_lookup`
Reference table for HS codes used in this project.

```sql
hs6_code        TEXT PRIMARY KEY
description     TEXT
category        TEXT              -- 'energy' | 'fertilizer_input' | 'fertilizer' | 'crop'
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
**API key:** None required
**Writes to:** `fertilizer_production`, `crop_production`
**Data available:** Crop production by country, fertilizer production and trade, food balance sheets
**Refresh cadence:** Annual (data typically lags 18-24 months)
**Known limitation:** Fertilizer consumption *by crop* is not directly available — production
and trade only. Consumption-by-crop breakdowns require IFA reports (manual PDF).

```bash
uv run python pullers/pull_faostat.py
```

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

### `pull_usda_psd.py` — USDA Production, Supply and Distribution

**Source:** USDA Foreign Agricultural Service
**URL:** https://apps.fas.usda.gov/psdonline/
**API key:** None required
**Writes to:** `crop_production`
**Data available:** Production, consumption, imports, exports, ending stocks by crop and country
**Refresh cadence:** Monthly (USDA updates on a fixed schedule)
**Known limitation:** Major crops only (wheat, corn, rice, soybeans, cotton) — no minor crops

```bash
uv run python pullers/pull_usda_psd.py
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
```

The loader filters to only the HS codes in scope (defined at the top of the script) so it does
not try to load all 5,000 products. This keeps DB size manageable and load time fast.

**Known limitation:** Annual only, no monthly granularity. Use Comtrade API (future v2)
for more recent or more specific bilateral queries.

---

## Running Everything (First Time)

```bash
# 1. Set up environment
uv sync
cp .env.example .env
# edit .env with your Supabase credentials

# 2. Initialise schema
# paste schema/create_tables.sql into Supabase SQL Editor and run

# 3. Load BACI first (largest, most important bilateral dataset)
# download files manually to data/baci/ first — see loader section above
uv run python loaders/load_baci.py --all

# 4. Run all pullers
uv run python pullers/pull_eia.py
uv run python pullers/pull_faostat.py
uv run python pullers/pull_worldbank.py
uv run python pullers/pull_usda_psd.py

# 5. Launch the explorer
uv run streamlit run app/streamlit_app.py
```

---

## Refreshing Data

| Script | When to re-run |
|---|---|
| `load_baci.py` | Annually, when CEPII releases new year |
| `pull_eia.py` | Monthly |
| `pull_faostat.py` | Annually (or when FAO releases updates) |
| `pull_worldbank.py` | Monthly |
| `pull_usda_psd.py` | Monthly |

Check what is in your database and when it was last pulled:

```sql
SELECT script_name, parameters, rows_written, status, completed_at
FROM pipeline_runs
ORDER BY completed_at DESC;
```

---

## Streamlit Data Explorer (V1)

The V1 app is intentionally a simple data explorer — no computed metrics,
no derived scores, no charts. The goal is to verify the data is complete and
queryable before building analytical dashboards on top.

**Sidebar filters:**
- Table selector (choose which table to explore)
- Year range slider
- Country multi-select (ISO3)
- HS code multi-select (for `bilateral_trade` table)
- Product or crop multi-select

**Main panel:**
- Row count and data freshness banner (shows `pulled_at` from most recent pipeline run for selected table)
- Paginated data table with all columns visible
- Basic summary stats (min, max, sum, count for numeric columns)
- CSV download button for current filtered view

**Pipeline status tab:**
- Table of recent `pipeline_runs` entries
- Last successful run per script, row counts, any errors

```bash
uv run streamlit run app/streamlit_app.py
```

The app should read Supabase settings from `.env` via python-dotenv — use `get_read_client()` with anon/publishable keys for safe reads, or `get_client()` with a server key only for a trusted local explorer (see Environment Setup above).

---

## Known Gaps and Limitations (V1)

| Gap | Impact | Planned fix |
|---|---|---|
| FAOSTAT crop data lags 18-24 months | Production figures are not current year | Supplement with USDA PSD (more timely) |
| Fertilizer consumption by crop not available | Cannot directly link fertilizer imports to specific crops | Requires IFA reports — manual for now |
| No vessel-level Hormuz transit data | Cannot count actual tankers transiting | Use EIA factsheet numbers as static reference |
| BACI is annual only | No monthly trade flow granularity | Add Comtrade API in v2 for recent months |
| Petrochemicals excluded | Plastics and chemicals exposure not mapped | Add HS 2901-2902, 3901-3904 in v2 |
| Exposure scores not computed | Raw data only, no derived Hormuz dependency index | Build iteratively once data is validated |
| Pink Sheet XLSX URL can 404 after WB republish | Pull fails until `PINK_SHEET_MONTHLY_XLSX_URL` is updated; check `pipeline_runs.error_message` | Manual URL refresh from World Bank doc page (script logs instructions) |

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

**V3 — Analysis layer**
- Compute Hormuz exposure scores per country
- Fertilizer import dependency index
- Food security cascade model: fertilizer shock → crop yield impact → import dependency
- Network graph of supply chain dependencies

**V4 — Dashboards**
- Built on top of this data layer using Claude Code or Cursor
- Choropleth maps, dependency trees, price shock scenario modelling
- Hosted Streamlit deployment