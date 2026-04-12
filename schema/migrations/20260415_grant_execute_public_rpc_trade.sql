-- Allow PostgREST (anon / Streamlit publishable key) to EXECUTE trade RPCs.
-- See rpc_trade_dashboards.sql footer for context.

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT p.oid::regprocedure AS fn
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname LIKE 'rpc_trade%'
    LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO anon, authenticated, service_role', r.fn);
    END LOOP;
END;
$$;
