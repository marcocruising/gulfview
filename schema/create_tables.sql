-- Hormuz supply chain — full schema for Supabase (run once in SQL Editor or psql)
-- Upsert-friendly UNIQUE constraints match pullers/loaders on_conflict targets.

-- Human-readable reference for every application table (not Postgres system catalogs).
CREATE TABLE IF NOT EXISTS table_catalog (
    id              SERIAL PRIMARY KEY,
    table_schema    TEXT NOT NULL DEFAULT 'public',
    table_name      TEXT NOT NULL,
    title           TEXT NOT NULL,
    summary         TEXT NOT NULL,
    row_grain       TEXT,
    key_columns     TEXT,
    populated_by    TEXT,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (table_schema, table_name)
);

CREATE INDEX IF NOT EXISTS idx_table_catalog_sort
    ON table_catalog (sort_order, table_name);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    script_name     TEXT NOT NULL,
    source_label    TEXT NOT NULL,
    parameters      JSONB,
    rows_written    INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL,
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS energy_trade_flows (
    id              SERIAL PRIMARY KEY,
    reporter        TEXT NOT NULL,
    flow_type       TEXT NOT NULL,
    product         TEXT NOT NULL,
    value_kbd       NUMERIC,
    data_year       INTEGER NOT NULL,
    data_month      INTEGER,
    source          TEXT NOT NULL,
    pulled_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (reporter, flow_type, product, data_year, data_month)
);

CREATE TABLE IF NOT EXISTS bilateral_trade (
    id              SERIAL PRIMARY KEY,
    exporter        TEXT NOT NULL,
    importer        TEXT NOT NULL,
    hs6_code        TEXT NOT NULL,
    hs_description  TEXT,
    trade_value_usd NUMERIC,
    quantity_tonnes NUMERIC,
    data_year       INTEGER NOT NULL,
    source          TEXT NOT NULL,
    pulled_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (exporter, importer, hs6_code, data_year)
);

CREATE INDEX IF NOT EXISTS idx_bilateral_trade_exporter_data_year ON bilateral_trade (exporter, data_year);
CREATE INDEX IF NOT EXISTS idx_bilateral_trade_importer_hs6_year ON bilateral_trade (importer, hs6_code, data_year);
CREATE INDEX IF NOT EXISTS idx_bilateral_trade_data_year ON bilateral_trade (data_year);

-- Calendar years present in bilateral_trade (cache for rpc_trade_distinct_data_years; loaders/load_baci.py maintains).
CREATE TABLE IF NOT EXISTS bilateral_trade_data_years (
    data_year integer NOT NULL PRIMARY KEY
);
GRANT SELECT ON bilateral_trade_data_years TO anon, authenticated, service_role;
GRANT INSERT, DELETE, UPDATE ON bilateral_trade_data_years TO service_role;

-- Persisted trade dependency snapshots (computed aggregates to avoid recomputation in the UI).
CREATE TABLE IF NOT EXISTS trade_group_dependency_snapshots (
    id              SERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_year       INTEGER NOT NULL,
    group_iso3      TEXT[] NOT NULL,
    params_json     JSONB NOT NULL,
    params_hash     TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'success',
    row_counts      JSONB,
    started_at      TIMESTAMPTZ,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_trade_group_dependency_snapshots_year
    ON trade_group_dependency_snapshots (data_year, computed_at DESC);

CREATE TABLE IF NOT EXISTS trade_group_dependency_rows (
    id                              SERIAL PRIMARY KEY,
    snapshot_id                     INTEGER NOT NULL REFERENCES trade_group_dependency_snapshots(id) ON DELETE CASCADE,
    view_type                       TEXT NOT NULL, -- 'export_world_share' | 'importer_exposure'
    hs6_code                        TEXT NOT NULL,
    importer_iso3                   TEXT, -- null for export view

    -- Common metrics
    group_export_usd_k              NUMERIC,
    world_export_usd_k              NUMERIC,
    group_share_pct                 NUMERIC,

    -- Export-side single-point-of-failure inside group
    top_group_exporter_iso3         TEXT,
    top_group_exporter_share_pct    NUMERIC,
    group_member_hhi                NUMERIC,
    group_exporter_count            INTEGER,

    -- Importer exposure metrics
    importer_total_import_usd_k     NUMERIC,
    imports_from_group_usd_k        NUMERIC,
    exposure_pct                    NUMERIC,
    supplier_total_hhi              NUMERIC,
    supplier_cr1_pct                NUMERIC,
    supplier_cr3_pct                NUMERIC,
    group_supplier_hhi              NUMERIC,
    group_supplier_cr1_pct          NUMERIC,

    extra_json                      JSONB,

    UNIQUE (snapshot_id, view_type, hs6_code, importer_iso3)
);

CREATE INDEX IF NOT EXISTS idx_trade_group_dependency_rows_snapshot_view
    ON trade_group_dependency_rows (snapshot_id, view_type);

CREATE INDEX IF NOT EXISTS idx_trade_group_dependency_rows_hs6
    ON trade_group_dependency_rows (hs6_code);

CREATE TABLE IF NOT EXISTS fertilizer_production (
    id              SERIAL PRIMARY KEY,
    country         TEXT NOT NULL,
    fertilizer_type TEXT NOT NULL,
    metric          TEXT NOT NULL,
    value_tonnes    NUMERIC,
    data_year       INTEGER NOT NULL,
    source          TEXT NOT NULL,
    pulled_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (country, fertilizer_type, metric, data_year)
);

CREATE TABLE IF NOT EXISTS crop_production (
    id              SERIAL PRIMARY KEY,
    country         TEXT NOT NULL,
    crop            TEXT NOT NULL,
    metric          TEXT NOT NULL,
    value           NUMERIC,
    unit            TEXT NOT NULL,
    data_year       INTEGER NOT NULL,
    source          TEXT NOT NULL,
    pulled_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (country, crop, metric, data_year)
);

CREATE TABLE IF NOT EXISTS commodity_prices (
    id              SERIAL PRIMARY KEY,
    commodity       TEXT NOT NULL,
    price           NUMERIC NOT NULL,
    unit            TEXT NOT NULL,
    data_year       INTEGER NOT NULL,
    data_month      INTEGER,
    source          TEXT NOT NULL,
    pulled_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (commodity, data_year, data_month)
);

CREATE TABLE IF NOT EXISTS country_macro_indicators (
    id              SERIAL PRIMARY KEY,
    country         TEXT NOT NULL,
    indicator       TEXT NOT NULL,
    value           NUMERIC,
    unit            TEXT NOT NULL,
    data_year       INTEGER NOT NULL,
    source          TEXT NOT NULL,
    pulled_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (country, indicator, data_year)
);

CREATE TABLE IF NOT EXISTS food_balance_sheets (
    id              SERIAL PRIMARY KEY,
    country         TEXT NOT NULL,
    commodity       TEXT NOT NULL,
    metric          TEXT NOT NULL,
    value           NUMERIC,
    unit            TEXT NOT NULL,
    data_year       INTEGER NOT NULL,
    source          TEXT NOT NULL,
    pulled_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (country, commodity, metric, data_year)
);

CREATE TABLE IF NOT EXISTS hs_code_lookup (
    hs6_code        TEXT PRIMARY KEY,
    description     TEXT,
    category        TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS country_lookup (
    iso3            TEXT PRIMARY KEY,
    iso2            TEXT,
    country_name    TEXT,
    region          TEXT,
    is_gulf_producer BOOLEAN
);

-- CEPII ProTEE 0.1 — product-level import-demand elasticities (NOT trade flows).
-- See loaders/load_cepi_beyond_baci.py module docstring and README for meaning vs BACI.
CREATE TABLE IF NOT EXISTS cepii_protee_hs6 (
    hs6_code                        TEXT PRIMARY KEY,
    flag_nonsignificant_at_1pct     BOOLEAN NOT NULL DEFAULT FALSE,
    flag_positive_significant       BOOLEAN NOT NULL DEFAULT FALSE,
    trade_elasticity                NUMERIC,
    hs_revision                     TEXT NOT NULL DEFAULT 'HS2007',
    source                          TEXT NOT NULL,
    pulled_at                       TIMESTAMPTZ NOT NULL
);

-- CEPII GeoDep — import-dependency diagnostics from BACI trade (NOT raw flows).
CREATE TABLE IF NOT EXISTS cepii_geodep_import_dependence (
    id                              SERIAL PRIMARY KEY,
    country                         TEXT NOT NULL,
    hs6_code                        TEXT NOT NULL,
    data_year                       INTEGER NOT NULL,
    import_value                    NUMERIC,
    hhi_import_concentration        NUMERIC,
    hhi_world_export_concentration  NUMERIC,
    import_to_export_ratio          NUMERIC,
    flag_persistent_criteria        BOOLEAN NOT NULL DEFAULT FALSE,
    flag_import_dependent           BOOLEAN NOT NULL DEFAULT FALSE,
    sector_strategic_agrifood       BOOLEAN NOT NULL DEFAULT FALSE,
    sector_strategic_chemicals      BOOLEAN NOT NULL DEFAULT FALSE,
    sector_strategic_pharmaceuticals BOOLEAN NOT NULL DEFAULT FALSE,
    sector_strategic_steel          BOOLEAN NOT NULL DEFAULT FALSE,
    sector_strategic_defense        BOOLEAN NOT NULL DEFAULT FALSE,
    sector_strategic_transport      BOOLEAN NOT NULL DEFAULT FALSE,
    sector_strategic_electronics    BOOLEAN NOT NULL DEFAULT FALSE,
    sector_strategic_other          BOOLEAN NOT NULL DEFAULT FALSE,
    leading_exporter_code           TEXT,
    leading_exporter_share_pct      NUMERIC,
    source                          TEXT NOT NULL,
    pulled_at                       TIMESTAMPTZ NOT NULL,
    UNIQUE (country, hs6_code, data_year)
);

-- JODI — country-reported monthly oil/gas statistics (SDMX-style CSV exports).
-- Complements energy_trade_flows (EIA); definitions differ — do not merge without harmonisation.
CREATE TABLE IF NOT EXISTS jodi_energy_observations (
    id                  SERIAL PRIMARY KEY,
    ref_area            TEXT NOT NULL,
    country             TEXT,
    data_year           INTEGER NOT NULL,
    data_month          INTEGER NOT NULL,
    energy_product      TEXT NOT NULL,
    flow_breakdown      TEXT NOT NULL,
    unit_measure        TEXT NOT NULL,
    obs_value           NUMERIC,
    obs_value_raw       TEXT,
    assessment_code     INTEGER,
    source_file         TEXT NOT NULL,
    source              TEXT NOT NULL,
    pulled_at           TIMESTAMPTZ NOT NULL,
    UNIQUE (ref_area, data_year, data_month, energy_product, flow_breakdown, unit_measure)
);

CREATE INDEX IF NOT EXISTS idx_jodi_country_year_month
    ON jodi_energy_observations (country, data_year, data_month);

CREATE INDEX IF NOT EXISTS idx_jodi_energy_product_year
    ON jodi_energy_observations (energy_product, data_year);

-- USGS Mineral Commodity Summaries (MCS) — long-form tables; fingerprint disambiguates duplicate keys in source.
CREATE TABLE IF NOT EXISTS usgs_mineral_statistics (
    id                          SERIAL PRIMARY KEY,
    record_fingerprint          TEXT NOT NULL UNIQUE,
    mcs_chapter                 TEXT NOT NULL,
    section                     TEXT NOT NULL,
    commodity                   TEXT NOT NULL,
    country_name                TEXT NOT NULL,
    country_iso3                TEXT,
    statistics                  TEXT NOT NULL,
    statistics_detail           TEXT NOT NULL,
    unit                        TEXT NOT NULL,
    data_year                   INTEGER NOT NULL,
    year_as_reported            TEXT NOT NULL,
    value_numeric               NUMERIC,
    value_raw                   TEXT,
    notes                       TEXT,
    other_notes                 TEXT,
    is_critical_mineral_2025    BOOLEAN,
    source_file                 TEXT NOT NULL,
    source                      TEXT NOT NULL,
    pulled_at                   TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_usgs_mineral_country_year
    ON usgs_mineral_statistics (country_iso3, data_year);

CREATE INDEX IF NOT EXISTS idx_usgs_mineral_commodity_year
    ON usgs_mineral_statistics (commodity, data_year);

-- USGS Minerals Yearbook Vol. III — country myb3-*.xlsx Table 1 (production matrix, melted).
CREATE TABLE IF NOT EXISTS usgs_myb3_production (
    id                      SERIAL PRIMARY KEY,
    record_fingerprint      TEXT NOT NULL UNIQUE,
    country_iso3            TEXT NOT NULL,
    reference_year          INTEGER NOT NULL,
    commodity_path          TEXT NOT NULL,
    stat_year               INTEGER NOT NULL,
    value_raw               TEXT,
    value_numeric           NUMERIC,
    footnote                TEXT,
    unit_context            TEXT,
    source_file             TEXT NOT NULL,
    sheet_name              TEXT NOT NULL,
    source                  TEXT NOT NULL,
    pulled_at               TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_usgs_myb3_prod_country_ref
    ON usgs_myb3_production (country_iso3, reference_year);

CREATE INDEX IF NOT EXISTS idx_usgs_myb3_prod_stat_year
    ON usgs_myb3_production (stat_year);

-- USGS myb3 Table 2 — structure of mineral industry (merged facility blocks).
CREATE TABLE IF NOT EXISTS usgs_country_mineral_facilities (
    id                      SERIAL PRIMARY KEY,
    record_fingerprint      TEXT NOT NULL UNIQUE,
    country_iso3            TEXT NOT NULL,
    reference_year          INTEGER NOT NULL,
    commodity_cell_raw      TEXT,
    commodity_leaf_resolved TEXT NOT NULL,
    facility_path           TEXT NOT NULL,
    owner_operator          TEXT,
    location                TEXT,
    capacity_raw            TEXT,
    capacity_numeric        NUMERIC,
    unit_note               TEXT,
    sheet_name              TEXT NOT NULL,
    excel_row_start         INTEGER NOT NULL,
    excel_row_end           INTEGER NOT NULL,
    source_file             TEXT NOT NULL,
    source                  TEXT NOT NULL,
    pulled_at               TIMESTAMPTZ NOT NULL,
    geocode_lat             DOUBLE PRECISION,
    geocode_lon             DOUBLE PRECISION,
    geocode_query           TEXT,
    geocode_source          TEXT,
    geocoded_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_usgs_facilities_country_ref
    ON usgs_country_mineral_facilities (country_iso3, reference_year);

ALTER TABLE usgs_myb3_production DISABLE ROW LEVEL SECURITY;
ALTER TABLE usgs_country_mineral_facilities DISABLE ROW LEVEL SECURITY;

-- Global Energy Monitor — tabular tracker rows as JSON (one row per Excel data row; header row excluded).
CREATE TABLE IF NOT EXISTS gem_tracker_rows (
    id                  SERIAL PRIMARY KEY,
    source_file         TEXT NOT NULL,
    sheet_name          TEXT NOT NULL,
    excel_row_1based    INTEGER NOT NULL,
    payload             JSONB NOT NULL,
    source              TEXT NOT NULL,
    pulled_at           TIMESTAMPTZ NOT NULL,
    UNIQUE (source_file, sheet_name, excel_row_1based)
);

CREATE INDEX IF NOT EXISTS idx_gem_tracker_source_sheet
    ON gem_tracker_rows (source_file, sheet_name);

-- Keyset pagination (WHERE source_file AND sheet_name AND id > $n ORDER BY id) for full-sheet GEM loads.
CREATE INDEX IF NOT EXISTS idx_gem_tracker_source_sheet_id
    ON gem_tracker_rows (source_file, sheet_name, id);

CREATE INDEX IF NOT EXISTS idx_gem_tracker_payload_gin
    ON gem_tracker_rows USING GIN (payload);

ALTER TABLE gem_tracker_rows DISABLE ROW LEVEL SECURITY;

ALTER TABLE trade_group_dependency_snapshots DISABLE ROW LEVEL SECURITY;
ALTER TABLE trade_group_dependency_rows DISABLE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS idx_cepii_geodep_country_year
    ON cepii_geodep_import_dependence (country, data_year);

CREATE INDEX IF NOT EXISTS idx_cepii_geodep_hs6_year
    ON cepii_geodep_import_dependence (hs6_code, data_year);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_completed_at
    ON pipeline_runs (completed_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_country_macro_indicators_country_year
    ON country_macro_indicators (country, data_year);

CREATE INDEX IF NOT EXISTS idx_country_macro_indicators_data_year
    ON country_macro_indicators (data_year);

CREATE INDEX IF NOT EXISTS idx_food_balance_sheets_country_year
    ON food_balance_sheets (country, data_year);

CREATE INDEX IF NOT EXISTS idx_food_balance_sheets_data_year
    ON food_balance_sheets (data_year);

-- Dictionary rows (idempotent). Canonical copy also in schema/seed_table_catalog.sql.
INSERT INTO table_catalog (
    table_schema,
    table_name,
    title,
    summary,
    row_grain,
    key_columns,
    populated_by,
    sort_order
) VALUES
(
    'public',
    'table_catalog',
    'Data dictionary',
    'Human-readable description of each application table in this project: purpose, grain, keys, and which script fills it. Use for onboarding, SQL exploration, and UI tooltips.',
    'One row per physical table in public schema that holds pipeline data.',
    'table_schema, table_name',
    'schema/seed_table_catalog.sql (manual or migration)',
    5
),
(
    'public',
    'pipeline_runs',
    'Pipeline run log',
    'Every execution of a puller or loader: script name, parameters JSON, rows written, status (success/partial/error), timestamps, and error text. Source of truth for when data was refreshed.',
    'One row per script run.',
    'id; query by script_name, completed_at',
    'utils/pipeline_logger.py (all pullers and loaders)',
    10
),
(
    'public',
    'energy_trade_flows',
    'Energy trade volumes (EIA)',
    'Oil and gas import/export volumes by reporting country from the U.S. EIA Open Data API. Products include crude, LNG, refined products; values stored as thousand barrels per day (kbd) after monthly-to-daily conversion where applicable.',
    'One row per reporter, flow direction, product, year, and optional month.',
    'reporter, flow_type, product, data_year, data_month',
    'pullers/pull_eia.py',
    20
),
(
    'public',
    'bilateral_trade_data_years',
    'BACI years index (cache)',
    'One row per calendar year that appears in bilateral_trade. Used by rpc_trade_distinct_data_years for fast year dropdowns; loaders/load_baci.py upserts after each successful load. Call refresh_bilateral_trade_data_years_cache() to rebuild if rows were deleted.',
    'One row per data_year.',
    'data_year',
    'loaders/load_baci.py; optional refresh_bilateral_trade_data_years_cache() in SQL',
    25
),
(
    'public',
    'bilateral_trade',
    'Bilateral merchandise trade (BACI)',
    'CEPII BACI reconciled bilateral flows at HS6: trade value in USD thousands and quantity in metric tonnes. Filtered in the loader to V1 HS chapters (energy, fertilizers, crops). Central table for who trades what with whom.',
    'One row per exporter, importer, HS6 code, calendar year.',
    'exporter, importer, hs6_code, data_year',
    'loaders/load_baci.py',
    30
),
(
    'public',
    'trade_group_dependency_snapshots',
    'Trade group dependency snapshots',
    'Saved parameter sets and metadata for precomputed trade dependency analyses (country group + year + filters). Used by Streamlit to load results instantly without recomputing.',
    'One row per parameter hash (group + year + filters).',
    'params_hash',
    'Streamlit UI (app/streamlit_app.py) using SQL RPC aggregates',
    31
),
(
    'public',
    'trade_group_dependency_rows',
    'Trade group dependency results',
    'Materialized results for trade group dependency snapshots: export-side world-share by HS6 with single-point-of-failure metrics, and importer exposure slices for a selected HS6.',
    'Many rows per snapshot (view_type × HS6, optionally importer).',
    'snapshot_id, view_type, hs6_code, importer_iso3',
    'Streamlit UI (app/streamlit_app.py) using SQL RPC aggregates',
    32
),
(
    'public',
    'fertilizer_production',
    'Fertilizer supply and use (FAOSTAT)',
    'Production, import, export, and consumption-style metrics for nitrogenous, phosphatic, potassic, and NPK-style products by country and year, in tonnes.',
    'One row per country, fertilizer_type, metric, year.',
    'country, fertilizer_type, metric, data_year',
    'pullers/pull_faostat.py (--dataset fertilizer or all)',
    40
),
(
    'public',
    'crop_production',
    'Crop production and trade (FAOSTAT + USDA)',
    'Area, production, yield, imports, exports for V1 crops (wheat, rice, corn, soybeans, cotton) from FAOSTAT bulk data and USDA PSD API. Units vary by metric (tonnes, hectares, etc.).',
    'One row per country, crop, metric, year.',
    'country, crop, metric, data_year',
    'pullers/pull_faostat.py, pullers/pull_usda_psd.py',
    50
),
(
    'public',
    'commodity_prices',
    'Commodity prices (World Bank Pink Sheet)',
    'Monthly (or annual where applicable) prices for crude, Brent, urea, DAP, ammonia, wheat, rice, corn from the World Bank commodity markets workbook.',
    'One row per commodity, year, optional month.',
    'commodity, data_year, data_month',
    'pullers/pull_worldbank.py',
    60
),
(
    'public',
    'country_macro_indicators',
    'Macro context (World Bank WDI)',
    'Population, GDP (current US$), GDP per capita by country and year for ISO3 members. Used to normalise trade and production per head or per economy.',
    'One row per country, indicator, year.',
    'country, indicator, data_year',
    'pullers/pull_worldbank_wdi.py',
    70
),
(
    'public',
    'food_balance_sheets',
    'Food balance sheets (FAOSTAT FBS)',
    'Domestic supply, trade, food, feed, and other use elements for V1 food commodities in metric tonnes from FAOSTAT Food Balance Sheets bulk ZIP.',
    'One row per country, commodity, metric, year.',
    'country, commodity, metric, data_year',
    'pullers/pull_faostat.py (--dataset fbs or all)',
    80
),
(
    'public',
    'hs_code_lookup',
    'HS6 code reference',
    'Six-digit Harmonized System codes with English descriptions from UN Comtrade reference JSON. Optional project category tag for V1 HS prefixes (energy, fertilizer, crop).',
    'One row per HS6 code.',
    'hs6_code',
    'pullers/pull_comtrade_hs_lookup.py',
    90
),
(
    'public',
    'country_lookup',
    'Country reference',
    'ISO3 primary key with optional ISO2, name, region, and Gulf producer flag for Hormuz-relevant analytics. Schema exists; seeding is optional/manual.',
    'One row per ISO3.',
    'iso3',
    '(none yet — manual or future loader)',
    100
),
(
    'public',
    'cepii_protee_hs6',
    'Product trade elasticities (CEPII ProTEE)',
    'Import-demand elasticity estimates and quality flags per HS6 (CEPII HS 2007 nomenclature). Not trade flows — use for sensitivity analysis. Read flags with elasticity (HS4 substitution when non-significant or positive per CEPII).',
    'One row per HS6 product.',
    'hs6_code',
    'loaders/load_cepi_beyond_baci.py protee',
    110
),
(
    'public',
    'cepii_geodep_import_dependence',
    'Import dependence indicators (CEPII GeoDep)',
    'Country × HS6 × year diagnostics from BACI: import HHI, world export HHI, import/export ratio, persistence flag, CEPII dependency flag, strategic sector dummies, leading exporter code and share. Complements bilateral_trade; does not list every partner.',
    'One row per importer country, HS6, year (2019–2022 in public file).',
    'country, hs6_code, data_year',
    'loaders/load_cepi_beyond_baci.py geodep',
    120
),
(
    'public',
    'jodi_energy_observations',
    'Oil and gas statistics (JODI)',
    'Monthly country-reported oil and natural gas observations from JODI CSV exports: REF_AREA (ISO2), product (e.g. CRUDEOIL, NATGAS), flow breakdown (balance concept), unit, optional numeric value, assessment code. Maps REF_AREA to ISO3 as country for joins. Complements EIA energy_trade_flows with different coverage and definitions.',
    'One row per reporter (ISO2), month, energy product, flow breakdown, and unit.',
    'ref_area, data_year, data_month, energy_product, flow_breakdown, unit_measure',
    'loaders/load_jodi.py',
    125
),
(
    'public',
    'usgs_mineral_statistics',
    'Mineral commodity summaries (USGS MCS)',
    'Long-form Mineral Commodity Summaries data: chapter, section, commodity, country, statistic type and detail, unit, year (data_year is start year when CSV gives a range), numeric value when parseable, raw value text, notes, and 2025 critical-mineral flag. country_iso3 mapped from country_name where possible; aggregates (World total, Other countries) have null ISO3. Upsert key is record_fingerprint (hash of row including Value and Notes) because the source repeats some logical keys.',
    'One row per fingerprinted MCS table line (country × commodity × statistic × unit × reported year label).',
    'record_fingerprint',
    'loaders/load_usgs.py mcs',
    130
),
(
    'public',
    'usgs_myb3_production',
    'USGS myb3 Table 1 — production by year',
    'Melted Minerals Yearbook country xlsx Table 1: country (ISO3 from filename), reference year from filename, commodity_path (section + branch + row label), stat_year from column header, value and footnote. Distinct from MCS usgs_mineral_statistics. Upsert key record_fingerprint.',
    'One row per country × commodity_path × stat_year × value cell.',
    'record_fingerprint',
    'loaders/load_usgs.py facilities',
    135
),
(
    'public',
    'usgs_country_mineral_facilities',
    'USGS myb3 Table 2 — mineral industry structure',
    'Merged blocks from Minerals Yearbook Table 2: operating companies, locations, annual capacity; Do. resolved as ditto to previous commodity cell; facility_path includes colon-based hierarchy. excel_row_start/end trace source rows. Upsert record_fingerprint.',
    'One row per merged facility block.',
    'record_fingerprint',
    'loaders/load_usgs.py facilities',
    140
),
(
    'public',
    'gem_tracker_rows',
    'Global Energy Monitor (GEM) tracker rows',
    'Rows from selected GEM Excel data downloads: each row is one spreadsheet line with column names as JSON keys. Default bundle: cement/concrete, iron ore mines, chemicals inventory, iron/steel plant-level sheets. Provenance via source_file and sheet_name; excel_row_1based matches Excel (header row 1).',
    'One row per workbook file, sheet, and Excel data row.',
    'source_file, sheet_name, excel_row_1based',
    'loaders/load_gem_xlsx.py',
    145
)
ON CONFLICT (table_schema, table_name) DO UPDATE SET
    title = EXCLUDED.title,
    summary = EXCLUDED.summary,
    row_grain = EXCLUDED.row_grain,
    key_columns = EXCLUDED.key_columns,
    populated_by = EXCLUDED.populated_by,
    sort_order = EXCLUDED.sort_order,
    updated_at = NOW();
