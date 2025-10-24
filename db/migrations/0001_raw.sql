CREATE TABLE IF NOT EXISTS public.ws_raw (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL default now(),
    channel TEXT NOT NULL, 
    payload JSONB NOT NULL
); 

CREATE INDEX IF NOT EXISTS ws_raw_channel_ts_idx  
    ON public.ws_raw (channel, ts DESC);

