-- Supporting indexes for these RPC query patterns (safe to apply repeatedly).
CREATE INDEX IF NOT EXISTS idx_bilateral_trade_data_year
    ON public.bilateral_trade (data_year);

CREATE INDEX IF NOT EXISTS idx_bilateral_trade_exporter_year_hs6
    ON public.bilateral_trade (exporter, data_year, hs6_code);

CREATE INDEX IF NOT EXISTS idx_bilateral_trade_year_hs6
    ON public.bilateral_trade (data_year, hs6_code);

CREATE INDEX IF NOT EXISTS idx_bilateral_trade_year_hs6_exporter
    ON public.bilateral_trade (data_year, hs6_code, exporter);

