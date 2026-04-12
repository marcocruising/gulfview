-- Speed keyset pagination for GEM map loads: (source_file, sheet_name, id > cursor) ORDER BY id.
CREATE INDEX IF NOT EXISTS idx_gem_tracker_source_sheet_id
    ON public.gem_tracker_rows (source_file, sheet_name, id);
