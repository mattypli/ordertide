Start: docker compose -f infra/compose/docker-compose.db.yml up -d 
Stop: docker compose -f infra/compose/docker-compose.db.yml down
Wipe data: docker compose -f infra/compose/docker-compose.db.yml down -v
Data volume: db_data
Status: docker ps # shows the list of 
UI: http://localhost:8080  (System: PostgreSQL, Server: db)

FAST RESET
docker compose -f infra/compose/docker-compose.db.yml down
docker compose -f infra/compose/docker-compose.db.yml up -d 
