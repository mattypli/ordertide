CREATE TABLE IF NOT EXISTS public.trades (
    -- main transaction data 
    coin TEXT NOT NULL,
    side TEXT NOT NULL, 
    price NUMERIC NOT NULL,
    size NUMERIC NOT NULL,
    hash TEXT NOT NULL, 

    -- time and identificator 
    time BIGINT NOT NULL, 
    tid BIGINT NOT NULL, 

    -- users buer and seller split 
    buyer TEXT NOT NULL,
    seller TEXT NOT NULL,

    -- our timestamp 
    created_at TIMESTAMPTZ NOT NULL default now(), 

    -- unique key to avoid duplicates
    PRIMARY KEY (time, coin, tid)
);

-- index, coins sorted by time, descending so newest trades are on top 
CREATE INDEX trades_coin_time_idx ON public.trades (coin, time DESC); 

