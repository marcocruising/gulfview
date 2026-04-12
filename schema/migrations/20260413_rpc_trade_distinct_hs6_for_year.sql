-- Distinct HS6 for "Who trades what" — full list per year without scanning N bilateral rows in the client.
-- See rpc_trade_dashboards.sql for context.

CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_hs6_for_year(p_data_year integer)
RETURNS TABLE (
    hs6_code text
)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT bt.hs6_code AS hs6_code
FROM public.bilateral_trade bt
WHERE bt.data_year = p_data_year
  AND bt.hs6_code IS NOT NULL AND TRIM(bt.hs6_code) <> ''
ORDER BY bt.hs6_code;
$$;
