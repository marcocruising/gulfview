-- Hormuz supply chain — full schema for Supabase (run once in SQL Editor or psql)
-- Upsert-friendly UNIQUE constraints match pullers/loaders on_conflict targets.

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
