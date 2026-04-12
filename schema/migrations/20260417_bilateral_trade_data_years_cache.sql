-- Fast year dropdown: cache distinct data_year in bilateral_trade_data_years (maintained by load_baci.py).
-- rpc_trade_distinct_data_years reads this table when non-empty; otherwise falls back to DISTINCT once.

CREATE TABLE IF NOT EXISTS public.bilateral_trade_data_years (
    data_year integer NOT NULL PRIMARY KEY
);

GRANT SELECT ON public.bilateral_trade_data_years TO anon, authenticated, service_role;
GRANT INSERT, DELETE, UPDATE ON public.bilateral_trade_data_years TO service_role;

SET statement_timeout = '120000';
INSERT INTO public.bilateral_trade_data_years (data_year)
SELECT DISTINCT bt.data_year
FROM public.bilateral_trade bt
ORDER BY 1
ON CONFLICT (data_year) DO NOTHING;

CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_data_years()
RETURNS TABLE (
    data_year integer
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.bilateral_trade_data_years LIMIT 1) THEN
    RETURN QUERY
    SELECT y.data_year
    FROM public.bilateral_trade_data_years y
    ORDER BY 1;
  ELSE
    PERFORM set_config('statement_timeout', '120000', true);
    RETURN QUERY
    SELECT DISTINCT bt.data_year AS data_year
    FROM public.bilateral_trade bt
    ORDER BY 1;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.refresh_bilateral_trade_data_years_cache()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  PERFORM set_config('statement_timeout', '120000', true);
  TRUNCATE public.bilateral_trade_data_years;
  INSERT INTO public.bilateral_trade_data_years (data_year)
  SELECT DISTINCT bt.data_year
  FROM public.bilateral_trade bt
  ORDER BY 1;
END;
$$;

REVOKE ALL ON FUNCTION public.refresh_bilateral_trade_data_years_cache() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.refresh_bilateral_trade_data_years_cache() TO service_role;
