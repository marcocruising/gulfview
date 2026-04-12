-- rpc_trade_distinct_exporters / _for_year / hs6_for_year: plpgsql + 120s statement_timeout.
-- Plain SQL DISTINCT on large bilateral_trade can exceed default limits; PostgREST/clients may
-- report RemoteProtocolError (“Server disconnected”) instead of 57014. Re-run full
-- schema/rpc_trade_dashboards.sql for grants and surrounding context.

CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_exporters()
RETURNS TABLE (
    exporter_iso3 text
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  PERFORM set_config('statement_timeout', '120000', true);
  RETURN QUERY
  SELECT DISTINCT bt.exporter AS exporter_iso3
  FROM public.bilateral_trade bt
  WHERE bt.exporter IS NOT NULL AND TRIM(bt.exporter) <> ''
  ORDER BY bt.exporter;
END;
$$;

CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_exporters_for_year(p_data_year integer)
RETURNS TABLE (
    exporter_iso3 text
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  PERFORM set_config('statement_timeout', '120000', true);
  RETURN QUERY
  SELECT DISTINCT bt.exporter AS exporter_iso3
  FROM public.bilateral_trade bt
  WHERE bt.data_year = p_data_year
    AND bt.exporter IS NOT NULL AND TRIM(bt.exporter) <> ''
  ORDER BY bt.exporter;
END;
$$;

CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_hs6_for_year(p_data_year integer)
RETURNS TABLE (
    hs6_code text
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  PERFORM set_config('statement_timeout', '120000', true);
  RETURN QUERY
  SELECT DISTINCT bt.hs6_code AS hs6_code
  FROM public.bilateral_trade bt
  WHERE bt.data_year = p_data_year
    AND bt.hs6_code IS NOT NULL AND TRIM(bt.hs6_code) <> ''
  ORDER BY bt.hs6_code;
END;
$$;
