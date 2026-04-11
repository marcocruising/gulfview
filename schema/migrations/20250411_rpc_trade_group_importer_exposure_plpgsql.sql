-- Top importers exposed to the group for one HS6 (bounded drill-down).
CREATE OR REPLACE FUNCTION public.rpc_trade_group_importer_exposure_for_hs6(
    p_data_year integer,
    p_hs6_code text,
    group_iso3 text[],
    limit_n integer DEFAULT 50
)
RETURNS TABLE (
    importer_iso3 text,
    importer_total_import_usd_k numeric,
    imports_from_group_usd_k numeric,
    exposure_pct numeric,
    supplier_total_hhi numeric,
    supplier_cr1_pct numeric,
    supplier_cr3_pct numeric,
    group_supplier_hhi numeric,
    group_supplier_cr1_pct numeric
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
imp_group AS (
    SELECT
        bt.importer,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS imports_from_group_usd_k
    FROM public.bilateral_trade bt
    JOIN grp g ON g.iso3 = UPPER(TRIM(bt.exporter))
    WHERE bt.data_year = p_data_year
      AND bt.hs6_code = TRIM(p_hs6_code)
    GROUP BY bt.importer
),
imp_total AS (
    SELECT
        bt.importer,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS importer_total_import_usd_k
    FROM public.bilateral_trade bt
    WHERE bt.data_year = p_data_year
      AND bt.hs6_code = TRIM(p_hs6_code)
    GROUP BY bt.importer
),
exposure AS (
    SELECT
        t.importer AS importer_iso3,
        t.importer_total_import_usd_k,
        COALESCE(g.imports_from_group_usd_k, 0) AS imports_from_group_usd_k,
        CASE
            WHEN t.importer_total_import_usd_k > 0
            THEN (COALESCE(g.imports_from_group_usd_k, 0) / t.importer_total_import_usd_k) * 100
            ELSE 0
        END AS exposure_pct
    FROM imp_total t
    LEFT JOIN imp_group g ON g.importer = t.importer
),
top_imp AS (
    SELECT e.*
    FROM exposure e
    WHERE e.imports_from_group_usd_k > 0
    ORDER BY e.exposure_pct DESC NULLS LAST, e.imports_from_group_usd_k DESC NULLS LAST
    LIMIT GREATEST(1, LEAST(COALESCE(limit_n, 50), 500))
),
flows AS (
    SELECT
        bt.importer,
        bt.exporter,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS trade_value_usd_k,
        CASE WHEN g.iso3 IS NOT NULL THEN TRUE ELSE FALSE END AS is_in_group
    FROM public.bilateral_trade bt
    JOIN top_imp ti ON ti.importer_iso3 = bt.importer
    LEFT JOIN grp g ON g.iso3 = UPPER(TRIM(bt.exporter))
    WHERE bt.data_year = p_data_year
      AND bt.hs6_code = TRIM(p_hs6_code)
    GROUP BY bt.importer, bt.exporter, (g.iso3 IS NOT NULL)
),
pos AS (
    SELECT *
    FROM flows
    WHERE trade_value_usd_k > 0
),
totals AS (
    SELECT
        p.importer,
        SUM(p.trade_value_usd_k) AS total_usd_k
    FROM pos p
    GROUP BY p.importer
),
shares AS (
    SELECT
        p.importer,
        p.exporter,
        p.trade_value_usd_k,
        p.is_in_group,
        CASE WHEN t.total_usd_k > 0 THEN (p.trade_value_usd_k / t.total_usd_k) ELSE 0 END AS share_all
    FROM pos p
    JOIN totals t ON t.importer = p.importer
),
ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (PARTITION BY s.importer ORDER BY s.trade_value_usd_k DESC NULLS LAST) AS rn
    FROM shares s
),
overall_metrics AS (
    SELECT
        r.importer,
        SUM(r.share_all * r.share_all) AS supplier_total_hhi,
        MAX(CASE WHEN r.rn = 1 THEN r.share_all END) * 100 AS supplier_cr1_pct,
        SUM(CASE WHEN r.rn <= 3 THEN r.share_all ELSE 0 END) * 100 AS supplier_cr3_pct
    FROM ranked r
    GROUP BY r.importer
),
group_totals AS (
    SELECT
        p.importer,
        SUM(CASE WHEN p.is_in_group THEN p.trade_value_usd_k ELSE 0 END) AS group_total_usd_k
    FROM pos p
    GROUP BY p.importer
),
group_shares AS (
    SELECT
        p.importer,
        p.exporter,
        p.trade_value_usd_k,
        CASE WHEN gt.group_total_usd_k > 0 THEN (p.trade_value_usd_k / gt.group_total_usd_k) ELSE 0 END AS share_in_group
    FROM pos p
    JOIN group_totals gt ON gt.importer = p.importer
    WHERE p.is_in_group
),
group_ranked AS (
    SELECT
        gs.*,
        ROW_NUMBER() OVER (PARTITION BY gs.importer ORDER BY gs.trade_value_usd_k DESC NULLS LAST) AS rn
    FROM group_shares gs
),
group_metrics AS (
    SELECT
        gr.importer,
        SUM(gr.share_in_group * gr.share_in_group) AS group_supplier_hhi,
        MAX(CASE WHEN gr.rn = 1 THEN gr.share_in_group END) * 100 AS group_supplier_cr1_pct
    FROM group_ranked gr
    GROUP BY gr.importer
)
SELECT
    ti.importer_iso3,
    ti.importer_total_import_usd_k,
    ti.imports_from_group_usd_k,
    ti.exposure_pct,
    om.supplier_total_hhi,
    om.supplier_cr1_pct,
    om.supplier_cr3_pct,
    gm.group_supplier_hhi,
    gm.group_supplier_cr1_pct
FROM top_imp ti
LEFT JOIN overall_metrics om ON om.importer = ti.importer_iso3
LEFT JOIN group_metrics gm ON gm.importer = ti.importer_iso3
ORDER BY ti.exposure_pct DESC NULLS LAST, ti.imports_from_group_usd_k DESC NULLS LAST;
END;
$$;
