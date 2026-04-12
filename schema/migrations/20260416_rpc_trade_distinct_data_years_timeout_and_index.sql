-- rpc_trade_distinct_data_years: avoid PostgREST 57014 (statement timeout) on large bilateral_trade.
-- Re-run schema/rpc_trade_dashboards.sql for full context.

CREATE INDEX IF NOT EXISTS idx_bilateral_trade_data_year
    ON public.bilateral_trade (data_year);

CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_data_years()
RETURNS TABLE (
    data_year integer
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  PERFORM set_config('statement_timeout', '120000', true);
  RETURN QUERY
  SELECT DISTINCT bt.data_year AS data_year
  FROM public.bilateral_trade bt
  ORDER BY 1;
END;
$$;
