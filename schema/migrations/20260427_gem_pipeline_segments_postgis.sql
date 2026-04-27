-- GEM pipeline line geometry (GOIT oil/NGL + GGIT gas) stored in PostGIS for Streamlit maps.
-- Loader inserts raw GeoJSON geometry into geom_geojson; trigger materializes `geom` for spatial index + bbox filters.

CREATE TABLE IF NOT EXISTS public.gem_pipeline_segments (
    id bigserial PRIMARY KEY,
    dataset text NOT NULL, -- 'goit_oil_ngl' | 'ggit_gas'
    pulled_at timestamptz NOT NULL DEFAULT now(),

    project_id text,
    pipeline_name text,
    segment_name text,
    status text,
    fuel text,

    start_location text,
    end_location text,
    countries text,

    capacity text,
    capacity_units text,
    diameter text,
    length_estimate_km double precision,
    length_known_km double precision,

    -- Full original GEM row attributes (for tooltips / future joins).
    properties jsonb NOT NULL DEFAULT '{}'::jsonb,

    -- Raw GeoJSON geometry input (LineString or MultiLineString as JSON object).
    geom_geojson jsonb,

    -- Materialized PostGIS geometry for indexing + server-side filtering.
    geom geometry(MultiLineString, 4326)
);

CREATE INDEX IF NOT EXISTS idx_gem_pipeline_segments_dataset ON public.gem_pipeline_segments (dataset);
CREATE INDEX IF NOT EXISTS idx_gem_pipeline_segments_project_id ON public.gem_pipeline_segments (project_id);
CREATE INDEX IF NOT EXISTS idx_gem_pipeline_segments_status ON public.gem_pipeline_segments (status);
CREATE INDEX IF NOT EXISTS idx_gem_pipeline_segments_geom_gist ON public.gem_pipeline_segments USING gist (geom);

CREATE OR REPLACE FUNCTION public._gem_pipeline_segments_set_geom()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    gtype text;
BEGIN
    -- Treat null/empty geometry as missing.
    IF NEW.geom_geojson IS NULL THEN
        NEW.geom := NULL;
        RETURN NEW;
    END IF;

    gtype := COALESCE(NEW.geom_geojson->>'type', '');
    IF gtype = '' OR gtype = 'GeometryCollection' THEN
        NEW.geom := NULL;
        RETURN NEW;
    END IF;

    -- Build geometry from GeoJSON and force MultiLineString(4326).
    NEW.geom := ST_Multi(
        ST_SetSRID(
            ST_GeomFromGeoJSON(NEW.geom_geojson::text),
            4326
        )
    );
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    -- If geometry parsing fails, keep row but leave geom null so loads don't abort.
    NEW.geom := NULL;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_gem_pipeline_segments_set_geom ON public.gem_pipeline_segments;
CREATE TRIGGER trg_gem_pipeline_segments_set_geom
BEFORE INSERT OR UPDATE OF geom_geojson
ON public.gem_pipeline_segments
FOR EACH ROW
EXECUTE FUNCTION public._gem_pipeline_segments_set_geom();

-- Server-side bbox query returning GeoJSON strings for Streamlit PathLayer.
CREATE OR REPLACE FUNCTION public.rpc_gem_pipeline_segments_bbox(
    p_min_lon double precision,
    p_min_lat double precision,
    p_max_lon double precision,
    p_max_lat double precision,
    p_dataset text DEFAULT NULL
)
RETURNS TABLE (
    id bigint,
    dataset text,
    project_id text,
    pipeline_name text,
    segment_name text,
    status text,
    fuel text,
    stroke_rgba jsonb,
    geom_geojson_text text,
    properties jsonb
)
LANGUAGE sql
STABLE
AS $$
SELECT
    s.id,
    s.dataset,
    s.project_id,
    s.pipeline_name,
    s.segment_name,
    s.status,
    s.fuel,
    CASE
        WHEN s.dataset = 'goit_oil_ngl' THEN jsonb_build_array(220, 38, 38, 170)
        WHEN s.dataset = 'ggit_gas' THEN jsonb_build_array(38, 110, 220, 170)
        ELSE jsonb_build_array(160, 160, 160, 170)
    END AS stroke_rgba,
    ST_AsGeoJSON(s.geom)::text AS geom_geojson_text,
    s.properties
FROM public.gem_pipeline_segments s
WHERE s.geom IS NOT NULL
  AND (
      p_dataset IS NULL
      OR s.dataset = p_dataset
  )
  AND ST_Intersects(
      s.geom,
      ST_MakeEnvelope(p_min_lon, p_min_lat, p_max_lon, p_max_lat, 4326)
  );
$$;

GRANT EXECUTE ON FUNCTION public.rpc_gem_pipeline_segments_bbox(
    double precision,
    double precision,
    double precision,
    double precision,
    text
) TO anon, authenticated, service_role;

