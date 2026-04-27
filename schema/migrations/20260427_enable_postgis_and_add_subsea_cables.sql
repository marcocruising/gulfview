-- Enable PostGIS and add undersea internet cable tables (TeleGeography Submarine Cable Map).
-- License: CC BY-NC-SA 3.0 (store attribution fields; show attribution in UI).

CREATE EXTENSION IF NOT EXISTS postgis;

-- Landing points (Points)
CREATE TABLE IF NOT EXISTS subsea_landing_points (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    is_tbd      BOOLEAN NOT NULL DEFAULT FALSE,
    lon         DOUBLE PRECISION NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    geom        geometry(Point, 4326),
    source      TEXT NOT NULL,
    source_url  TEXT NOT NULL,
    license     TEXT NOT NULL,
    pulled_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subsea_landing_points_name
    ON subsea_landing_points (name);

CREATE INDEX IF NOT EXISTS idx_subsea_landing_points_geom
    ON subsea_landing_points USING GIST (geom);

ALTER TABLE subsea_landing_points DISABLE ROW LEVEL SECURITY;

GRANT SELECT ON subsea_landing_points TO anon, authenticated;
GRANT INSERT, UPDATE, DELETE ON subsea_landing_points TO service_role;

-- Cable systems (metadata)
CREATE TABLE IF NOT EXISTS subsea_cable_systems (
    slug        TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    rfs_year    INTEGER,
    length_km   NUMERIC,
    owners      TEXT,
    website     TEXT,
    source      TEXT NOT NULL,
    source_url  TEXT NOT NULL,
    license     TEXT NOT NULL,
    pulled_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subsea_cable_systems_name
    ON subsea_cable_systems (name);

ALTER TABLE subsea_cable_systems DISABLE ROW LEVEL SECURITY;

GRANT SELECT ON subsea_cable_systems TO anon, authenticated;
GRANT INSERT, UPDATE, DELETE ON subsea_cable_systems TO service_role;

-- Cable routes (MultiLineString as JSON + optional PostGIS geometry)
CREATE TABLE IF NOT EXISTS subsea_cable_routes (
    id          BIGSERIAL PRIMARY KEY,
    cable_slug  TEXT NOT NULL REFERENCES subsea_cable_systems(slug) ON DELETE CASCADE,
    color       TEXT,
    -- List of line strings; each line string is a list of [lon,lat] points.
    path_coords JSONB NOT NULL,
    geom        geometry(MultiLineString, 4326),
    source      TEXT NOT NULL,
    source_url  TEXT NOT NULL,
    license     TEXT NOT NULL,
    pulled_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cable_slug)
);

CREATE INDEX IF NOT EXISTS idx_subsea_cable_routes_slug
    ON subsea_cable_routes (cable_slug);

CREATE INDEX IF NOT EXISTS idx_subsea_cable_routes_path_coords_gin
    ON subsea_cable_routes USING GIN (path_coords);

CREATE INDEX IF NOT EXISTS idx_subsea_cable_routes_geom
    ON subsea_cable_routes USING GIST (geom);

ALTER TABLE subsea_cable_routes DISABLE ROW LEVEL SECURITY;

GRANT SELECT ON subsea_cable_routes TO anon, authenticated;
GRANT INSERT, UPDATE, DELETE ON subsea_cable_routes TO service_role;
GRANT USAGE, SELECT ON SEQUENCE subsea_cable_routes_id_seq TO service_role;

-- Cable ↔ landing point relationship (many-to-many)
CREATE TABLE IF NOT EXISTS subsea_cable_landing_points (
    cable_slug          TEXT NOT NULL REFERENCES subsea_cable_systems(slug) ON DELETE CASCADE,
    landing_point_id    TEXT NOT NULL REFERENCES subsea_landing_points(id) ON DELETE CASCADE,
    ordinal             INTEGER,
    pulled_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cable_slug, landing_point_id)
);

CREATE INDEX IF NOT EXISTS idx_subsea_cable_landing_points_lp
    ON subsea_cable_landing_points (landing_point_id);

ALTER TABLE subsea_cable_landing_points DISABLE ROW LEVEL SECURITY;

GRANT SELECT ON subsea_cable_landing_points TO anon, authenticated;
GRANT INSERT, UPDATE, DELETE ON subsea_cable_landing_points TO service_role;

