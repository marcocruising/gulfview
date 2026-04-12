-- Full distinct data_year list + exporter∪importer ISO3 per year (Streamlit dropdowns; no capped client scans).
-- See rpc_trade_dashboards.sql for context.

CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_data_years()
RETURNS TABLE (
    data_year integer
)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT bt.data_year AS data_year
FROM public.bilateral_trade bt
ORDER BY 1;
$$;

CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_country_iso3_for_year(p_data_year integer)
RETURNS TABLE (
    country_iso3 text
)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT UPPER(TRIM(v)) AS country_iso3
FROM (
    SELECT bt.exporter AS v
    FROM public.bilateral_trade bt
    WHERE bt.data_year = p_data_year
      AND bt.exporter IS NOT NULL
      AND TRIM(bt.exporter) <> ''
    UNION
    SELECT bt.importer AS v
    FROM public.bilateral_trade bt
    WHERE bt.data_year = p_data_year
      AND bt.importer IS NOT NULL
      AND TRIM(bt.importer) <> ''
) x(v)
ORDER BY 1;
$$;
