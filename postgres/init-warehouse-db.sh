#!/bin/bash
# Eseguito automaticamente da Postgres al primo avvio (docker-entrypoint-initdb.d).
# POSTGRES_DB crea già il database metadata di Airflow: qui creiamo in più
# il database "warehouse" usato da bronze/silver/gold.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ${WAREHOUSE_DB};
EOSQL
