CREATE TABLE IF NOT EXISTS public.bbo (
    coin TEXT NOT NULL, 
    time BIGINT NOT NULL, 
    bid_px NUMERIC, 
    bid_sz NUMERIC,
    bid_n INTEGER, 
    ask_px NUMERIC, 
    ask_sz NUMERIC, 
    ask_n INTEGER,
    
    created_at TIMESTAMPTZ NOT NULL default now(),
    PRIMARY KEY (coin, time) 
);

-- index for cross-coin time-range queries (per-coin queries already covered by PK)
CREATE INDEX IF NOT EXISTS bbo_time_idx ON public.bbo (time DESC);
