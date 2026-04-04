# DB connections
Host DSN: postgresql://<user>:<pass>@localhost:5432/<db>   (for psql on host)
In-Compose DSN: postgresql://<user>:<pass>@db:5432/<db>    (service-to-service)
Used in M2: Adminer via Server=db, System=PostgreSQL
Verified: inserted 1 row into public.ws_raw and read it back
Health cmds: up/down/logs in infra/README.md
