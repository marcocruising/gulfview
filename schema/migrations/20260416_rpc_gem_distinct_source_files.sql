-- Distinct GEM workbook filenames in DB (Streamlit Explore more → GEM tab).
-- Small cardinality; avoids scanning the table client-side for unique source_file values.

CREATE OR REPLACE FUNCTION public.rpc_gem_distinct_source_files()
RETURNS TABLE (
    source_file text
)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT g.source_file
FROM public.gem_tracker_rows g
ORDER BY 1;
$$;

GRANT EXECUTE ON FUNCTION public.rpc_gem_distinct_source_files() TO anon, authenticated, service_role;
