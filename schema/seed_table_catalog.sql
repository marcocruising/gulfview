-- Reference copy for table_catalog row descriptions. Applied via migration or:
--   psql "$DATABASE_URL" -f schema/seed_table_catalog.sql
-- Idempotent: safe to re-run (upserts by table_name).
-- Keep in sync with the INSERT block at the end of create_tables.sql.

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
)
ON CONFLICT (table_schema, table_name) DO UPDATE SET
    title = EXCLUDED.title,
    summary = EXCLUDED.summary,
    row_grain = EXCLUDED.row_grain,
    key_columns = EXCLUDED.key_columns,
    populated_by = EXCLUDED.populated_by,
    sort_order = EXCLUDED.sort_order,
    updated_at = NOW();
