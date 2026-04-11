-- RPC functions for fast Streamlit dashboards (trade drill-down).
-- These compute on-demand in Postgres so the UI fetches aggregated results.
--
-- Parameters are named p_data_year / p_hs6_code (not data_year / hs6_code) so they never
-- collide with table columns or RETURNS TABLE output names — avoids "ambiguous column
-- reference" errors when PostgREST invokes these functions.

-- HS6 totals for an exporter and year (optional search by HS6 code substring or description).
CREATE OR REPLACE FUNCTION public.rpc_trade_exporter_hs6_totals(
    exporter_iso3 text,
    p_data_year integer,
    hs_query_text text DEFAULT NULL,
    limit_n integer DEFAULT 200
)
RETURNS TABLE (
    hs6_code text,
    trade_value_usd_k numeric,
    row_count bigint
)
LANGUAGE sql
STABLE
AS $$
WITH base AS (
    SELECT
        bt.hs6_code,
        bt.trade_value_usd
    FROM public.bilateral_trade bt
    WHERE bt.exporter = UPPER(TRIM(exporter_iso3))
      AND bt.data_year = p_data_year
),
filt AS (
    SELECT
        b.hs6_code,
        b.trade_value_usd
    FROM base b
    LEFT JOIN public.hs_code_lookup hl
        ON hl.hs6_code = b.hs6_code
    WHERE
      hs_query_text IS NULL
      OR TRIM(hs_query_text) = ''
      OR b.hs6_code ILIKE '%' || TRIM(hs_query_text) || '%'
      OR COALESCE(hl.description, '') ILIKE '%' || TRIM(hs_query_text) || '%'
)
SELECT
    f.hs6_code,
    SUM(COALESCE(f.trade_value_usd, 0)) AS trade_value_usd_k,
    COUNT(*) AS row_count
FROM filt f
GROUP BY f.hs6_code
ORDER BY trade_value_usd_k DESC NULLS LAST
LIMIT GREATEST(1, LEAST(COALESCE(limit_n, 200), 2000));
$$;


-- Partner totals for an exporter → importer for one HS6 and year.
CREATE OR REPLACE FUNCTION public.rpc_trade_exporter_partner_totals(
    exporter_iso3 text,
    p_data_year integer,
    p_hs6_code text,
    limit_n integer DEFAULT 50
)
RETURNS TABLE (
    partner_iso3 text,
    trade_value_usd_k numeric,
    share_pct numeric
)
LANGUAGE sql
STABLE
AS $$
WITH agg AS (
    SELECT
        bt.importer AS partner_iso3,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS trade_value_usd_k
    FROM public.bilateral_trade bt
    WHERE bt.exporter = UPPER(TRIM(exporter_iso3))
      AND bt.data_year = p_data_year
      AND bt.hs6_code = TRIM(p_hs6_code)
    GROUP BY bt.importer
),
tot AS (
    SELECT SUM(a.trade_value_usd_k) AS total_usd_k
    FROM agg a
)
SELECT
    a.partner_iso3,
    a.trade_value_usd_k,
    CASE WHEN t.total_usd_k > 0 THEN (a.trade_value_usd_k / t.total_usd_k) * 100 ELSE 0 END AS share_pct
FROM agg a
CROSS JOIN tot t
ORDER BY a.trade_value_usd_k DESC NULLS LAST
LIMIT GREATEST(1, LEAST(COALESCE(limit_n, 50), 500));
$$;


-- Supplier breakdown for an importer: every exporter → importer for one HS6 and year.
CREATE OR REPLACE FUNCTION public.rpc_trade_importer_supplier_breakdown(
    importer_iso3 text,
    p_data_year integer,
    p_hs6_code text,
    limit_n integer DEFAULT 50
)
RETURNS TABLE (
    supplier_iso3 text,
    trade_value_usd_k numeric,
    share_pct numeric
)
LANGUAGE sql
STABLE
AS $$
WITH agg AS (
    SELECT
        bt.exporter AS supplier_iso3,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS trade_value_usd_k
    FROM public.bilateral_trade bt
    WHERE bt.importer = UPPER(TRIM(importer_iso3))
      AND bt.data_year = p_data_year
      AND bt.hs6_code = TRIM(p_hs6_code)
    GROUP BY bt.exporter
),
tot AS (
    SELECT SUM(a.trade_value_usd_k) AS total_usd_k
    FROM agg a
)
SELECT
    a.supplier_iso3,
    a.trade_value_usd_k,
    CASE WHEN t.total_usd_k > 0 THEN (a.trade_value_usd_k / t.total_usd_k) * 100 ELSE 0 END AS share_pct
FROM agg a
CROSS JOIN tot t
ORDER BY a.trade_value_usd_k DESC NULLS LAST
LIMIT GREATEST(1, LEAST(COALESCE(limit_n, 50), 500));
$$;


-- Supplier concentration metrics for an importer × HS6 × year.
CREATE OR REPLACE FUNCTION public.rpc_trade_importer_supplier_metrics(
    importer_iso3 text,
    p_data_year integer,
    p_hs6_code text
)
RETURNS TABLE (
    total_usd_k numeric,
    n_suppliers integer,
    hhi numeric,
    cr1_pct numeric,
    cr3_pct numeric
)
LANGUAGE sql
STABLE
AS $$
WITH agg AS (
    SELECT
        bt.exporter AS supplier_iso3,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS trade_value_usd_k
    FROM public.bilateral_trade bt
    WHERE bt.importer = UPPER(TRIM(importer_iso3))
      AND bt.data_year = p_data_year
      AND bt.hs6_code = TRIM(p_hs6_code)
    GROUP BY bt.exporter
),
pos AS (
    SELECT a.*
    FROM agg a
    WHERE a.trade_value_usd_k > 0
),
tot AS (
    SELECT
        COALESCE(SUM(p.trade_value_usd_k), 0) AS total_usd_k,
        COUNT(*)::integer AS n_suppliers
    FROM pos p
),
shares AS (
    SELECT
        p.supplier_iso3,
        p.trade_value_usd_k,
        CASE WHEN t.total_usd_k > 0 THEN (p.trade_value_usd_k / t.total_usd_k) ELSE 0 END AS share
    FROM pos p
    CROSS JOIN tot t
),
ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (ORDER BY s.trade_value_usd_k DESC NULLS LAST) AS rn
    FROM shares s
)
SELECT
    t.total_usd_k,
    t.n_suppliers,
    CASE WHEN t.total_usd_k > 0 THEN SUM(r.share * r.share) ELSE NULL END AS hhi,
    CASE WHEN t.total_usd_k > 0 THEN MAX(CASE WHEN r.rn = 1 THEN r.share END) * 100 ELSE NULL END AS cr1_pct,
    CASE WHEN t.total_usd_k > 0 THEN SUM(CASE WHEN r.rn <= 3 THEN r.share ELSE 0 END) * 100 ELSE NULL END AS cr3_pct
FROM tot t
LEFT JOIN ranked r ON TRUE
GROUP BY t.total_usd_k, t.n_suppliers;
$$;

-- Distinct exporters present in BACI (any year).
CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_exporters()
RETURNS TABLE (
    exporter_iso3 text
)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT bt.exporter AS exporter_iso3
FROM public.bilateral_trade bt
WHERE bt.exporter IS NOT NULL AND TRIM(bt.exporter) <> ''
ORDER BY bt.exporter;
$$;

-- Distinct exporters present in BACI for one year (complete list; avoids capped client scans).
CREATE OR REPLACE FUNCTION public.rpc_trade_distinct_exporters_for_year(p_data_year integer)
RETURNS TABLE (
    exporter_iso3 text
)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT bt.exporter AS exporter_iso3
FROM public.bilateral_trade bt
WHERE bt.data_year = p_data_year
  AND bt.exporter IS NOT NULL AND TRIM(bt.exporter) <> ''
ORDER BY bt.exporter;
$$;

-- Available years for a given exporter (for exporter-first UI).
CREATE OR REPLACE FUNCTION public.rpc_trade_years_for_exporter(exporter_iso3 text)
RETURNS TABLE (
    data_year integer
)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT bt.data_year AS data_year
FROM public.bilateral_trade bt
WHERE bt.exporter = UPPER(TRIM(exporter_iso3))
ORDER BY 1;
$$;

-- ============================================================
-- Group dependency RPCs (exports + importer exposure)
-- ============================================================

-- Group share of world exports, by HS6, with within-group single-point-of-failure metrics.
-- plpgsql + extended statement_timeout: full scans on bilateral_trade exceed PostgREST defaults.
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
)
SELECT
    f.hs6_code,
    f.group_export_usd_k,
    f.world_export_usd_k,
    f.world_exporter_count,
    f.group_share_pct,
    f.top_group_exporter_iso3,
    f.top_group_exporter_share_pct,
    f.group_member_hhi,
    f.group_exporter_count
FROM filt f
ORDER BY f.group_share_pct DESC NULLS LAST, f.group_export_usd_k DESC NULLS LAST
LIMIT GREATEST(1, LEAST(COALESCE(limit_n, 200), 2000));
END;
$$;


-- Breakdown of group members’ exports for one HS6 (within-group shares).
CREATE OR REPLACE FUNCTION public.rpc_trade_group_member_breakdown_for_hs6(
    p_data_year integer,
    p_hs6_code text,
    group_iso3 text[]
)
RETURNS TABLE (
    exporter_iso3 text,
    export_usd_k numeric,
    share_in_group_pct numeric
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
agg AS (
    SELECT
        bt.exporter AS exporter_iso3,
        SUM(COALESCE(bt.trade_value_usd, 0)) AS export_usd_k
    FROM public.bilateral_trade bt
    JOIN grp g ON g.iso3 = UPPER(TRIM(bt.exporter))
    WHERE bt.data_year = p_data_year
      AND bt.hs6_code = TRIM(p_hs6_code)
    GROUP BY bt.exporter
),
tot AS (
    SELECT SUM(a.export_usd_k) AS total_usd_k
    FROM agg a
)
SELECT
    a.exporter_iso3,
    a.export_usd_k,
    CASE WHEN t.total_usd_k > 0 THEN (a.export_usd_k / t.total_usd_k) * 100 ELSE 0 END AS share_in_group_pct
FROM agg a
CROSS JOIN tot t
WHERE a.export_usd_k > 0
ORDER BY a.export_usd_k DESC NULLS LAST;
END;
$$;


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

