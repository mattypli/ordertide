INSERT INTO public.ws_raw (channel, payload)
VALUES ('trades:BTC-USDT', '{"p":"65000.5","q":"0.1"}');

SELECT count(*) FROM public.ws_raw; # counts all records in ws_raw table
SELECT id, channel, ts, payload FROM public.ws_raw ORDER BY id DESC LIMIT 1; 
