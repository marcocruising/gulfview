-- rpc_trade_group_world_share_by_hs6: compute full filt universe, then ROW_NUMBER, then limit_n (cap 10000).
-- Re-apply full rpc_trade_dashboards.sql in production if you prefer one file; this migration is idempotent.

CREATE OR REPLACE FUNCTION public.rpc_trade_group_world_share_by_hs6(
    p_data_year integer,
    group_iso3 text[],
    hs_query_text text DEFAULT NULL,
    limit_n integer DEFAULT 200
)
RETURNS TABLE (
    hs6_code text,
    group_export_usd_k numeric,
    world_export_usd_k numeric,
    world_exporter_count integer,
    group_share_pct numeric,
    top_group_exporter_iso3 text,
    top_group_exporter_share_pct numeric,
    group_member_hhi numeric,
    group_exporter_count integer
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
world AS (
    SELECT
        bt.hs6_code,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS world_export_usd_k,
        COUNT(DISTINCT bt.exporter)::integer AS world_exporter_count
    FROM public.bilateral_trade bt
    WHERE bt.data_year = p_data_year
    GROUP BY bt.hs6_code
),
group_by_exporter AS (
    SELECT
        bt.hs6_code,
        bt.exporter,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS export_usd_k
    FROM public.bilateral_trade bt
    JOIN grp g ON g.iso3 = UPPER(TRIM(bt.exporter))
    WHERE bt.data_year = p_data_year
    GROUP BY bt.hs6_code, bt.exporter
),
group_tot AS (
    SELECT
        gbe.hs6_code,
        SUM(gbe.export_usd_k) AS group_export_usd_k,
        COUNT(*)::integer AS group_exporter_count
    FROM group_by_exporter gbe
    WHERE gbe.export_usd_k > 0
    GROUP BY gbe.hs6_code
),
shares AS (
    SELECT
        gbe.hs6_code,
        gbe.exporter,
        gbe.export_usd_k,
        gt.group_export_usd_k,
        CASE WHEN gt.group_export_usd_k > 0 THEN (gbe.export_usd_k / gt.group_export_usd_k) ELSE 0 END AS share_in_group
    FROM group_by_exporter gbe
    JOIN group_tot gt ON gt.hs6_code = gbe.hs6_code
    WHERE gbe.export_usd_k > 0
),
ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (PARTITION BY s.hs6_code ORDER BY s.export_usd_k DESC NULLS LAST) AS rn
    FROM shares s
),
within_group AS (
    SELECT
        r.hs6_code,
        MAX(CASE WHEN r.rn = 1 THEN r.exporter END) AS top_group_exporter_iso3,
        MAX(CASE WHEN r.rn = 1 THEN r.share_in_group END) * 100 AS top_group_exporter_share_pct,
        SUM(r.share_in_group * r.share_in_group) AS group_member_hhi
    FROM ranked r
    GROUP BY r.hs6_code
),
joined AS (
    SELECT
        gt.hs6_code,
        gt.group_export_usd_k,
        w.world_export_usd_k,
        w.world_exporter_count,
        CASE WHEN w.world_export_usd_k > 0 THEN (gt.group_export_usd_k / w.world_export_usd_k) * 100 ELSE 0 END AS group_share_pct,
        wg.top_group_exporter_iso3,
        wg.top_group_exporter_share_pct,
        wg.group_member_hhi,
        gt.group_exporter_count
    FROM group_tot gt
    JOIN world w ON w.hs6_code = gt.hs6_code
    LEFT JOIN within_group wg ON wg.hs6_code = gt.hs6_code
),
filt AS (
    SELECT j.*
    FROM joined j
    LEFT JOIN public.hs_code_lookup hl ON hl.hs6_code = j.hs6_code
    WHERE
      hs_query_text IS NULL
      OR TRIM(hs_query_text) = ''
      OR j.hs6_code ILIKE '%' || TRIM(hs_query_text) || '%'
      OR COALESCE(hl.description, '') ILIKE '%' || TRIM(hs_query_text) || '%'
),
hs6_ranked_by_share AS (
    SELECT
        f.hs6_code,
        f.group_export_usd_k,
        f.world_export_usd_k,
        f.world_exporter_count,
        f.group_share_pct,
        f.top_group_exporter_iso3,
        f.top_group_exporter_share_pct,
        f.group_member_hhi,
        f.group_exporter_count,
        ROW_NUMBER() OVER (
            ORDER BY f.group_share_pct DESC NULLS LAST, f.group_export_usd_k DESC NULLS LAST
        ) AS rn_by_group_share_of_world
    FROM filt f
)
SELECT
    r.hs6_code,
    r.group_export_usd_k,
    r.world_export_usd_k,
    r.world_exporter_count,
    r.group_share_pct,
    r.top_group_exporter_iso3,
    r.top_group_exporter_share_pct,
    r.group_member_hhi,
    r.group_exporter_count
FROM hs6_ranked_by_share r
WHERE r.rn_by_group_share_of_world
      <= GREATEST(1, LEAST(COALESCE(limit_n, 200), 10000))
ORDER BY r.rn_by_group_share_of_world;
END;
$$;
