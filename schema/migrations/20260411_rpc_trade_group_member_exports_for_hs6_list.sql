-- See rpc_trade_dashboards.sql for full project context.

CREATE OR REPLACE FUNCTION public.rpc_trade_group_member_exports_for_hs6_list(
    p_data_year integer,
    group_iso3 text[],
    p_hs6_codes text[]
)
RETURNS TABLE (
    hs6_code text,
    exporter_iso3 text,
    export_usd_k numeric,
    share_of_group_product_pct numeric
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  PERFORM set_config('statement_timeout', '120000', true);
  RETURN QUERY
  WITH grp AS (
    SELECT DISTINCT UPPER(TRIM(x)) AS iso3
    FROM unnest(COALESCE(group_iso3, ARRAY[]::text[])) AS x
    WHERE TRIM(COALESCE(x, '')) <> ''
  ),
  codes AS (
    SELECT DISTINCT UPPER(TRIM(c)) AS hs6
    FROM unnest(COALESCE(p_hs6_codes, ARRAY[]::text[])) AS c
    WHERE TRIM(COALESCE(c, '')) <> ''
  ),
  agg AS (
    SELECT
        bt.hs6_code AS hs6_code,
        bt.exporter AS exporter_iso3,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS export_usd_k
    FROM public.bilateral_trade bt
    JOIN grp g ON g.iso3 = UPPER(TRIM(bt.exporter))
    INNER JOIN codes co ON co.hs6 = UPPER(TRIM(bt.hs6_code))
    WHERE bt.data_year = p_data_year
    GROUP BY bt.hs6_code, bt.exporter
    HAVING SUM(COALESCE(bt.trade_value_usd, 0)) > 0
  ),
  tot AS (
    SELECT a.hs6_code, SUM(a.export_usd_k) AS group_product_total
    FROM agg a
    GROUP BY a.hs6_code
  )
  SELECT
    a.hs6_code,
    a.exporter_iso3,
    a.export_usd_k,
    CASE
      WHEN t.group_product_total > 0 THEN (a.export_usd_k / t.group_product_total) * 100
      ELSE 0::numeric
    END AS share_of_group_product_pct
  FROM agg a
  JOIN tot t ON t.hs6_code = a.hs6_code
  ORDER BY a.hs6_code, a.export_usd_k DESC NULLS LAST;
END;
$$;
