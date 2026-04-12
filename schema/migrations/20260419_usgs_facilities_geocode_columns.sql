-- USGS myb3 Table 2: optional geocoded coordinates (batch job: scripts/geocode_usgs_facilities.py).
ALTER TABLE usgs_country_mineral_facilities
    ADD COLUMN IF NOT EXISTS geocode_lat DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS geocode_lon DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS geocode_query TEXT,
    ADD COLUMN IF NOT EXISTS geocode_source TEXT,
    ADD COLUMN IF NOT EXISTS geocoded_at TIMESTAMPTZ;

COMMENT ON COLUMN usgs_country_mineral_facilities.geocode_lat IS 'WGS84 latitude from geocoder when resolved.';
COMMENT ON COLUMN usgs_country_mineral_facilities.geocode_lon IS 'WGS84 longitude from geocoder when resolved.';
COMMENT ON COLUMN usgs_country_mineral_facilities.geocode_query IS 'Winning search string sent to the geocoder.';
COMMENT ON COLUMN usgs_country_mineral_facilities.geocode_source IS 'e.g. nominatim';
COMMENT ON COLUMN usgs_country_mineral_facilities.geocoded_at IS 'When coordinates were last written.';
