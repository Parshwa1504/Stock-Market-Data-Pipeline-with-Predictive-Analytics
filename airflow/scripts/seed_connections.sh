#!/usr/bin/env bash
set -e

# Example: set postgres_default to the Airflow metadata DB (fine for demos),
# or point to your own warehouse if you have one.
airflow connections delete postgres_default || true
airflow connections add 'postgres_default' \
  --conn-uri "postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}"

# If you have an external Postgres on your Windows host:
# airflow connections add 'warehouse_postgres' \
#   --conn-uri "postgresql+psycopg2://user:pass@host.docker.internal:5432/your_db"
