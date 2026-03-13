# steam_dwh

Minimal dbt project for the local `MinIO -> Trino -> dbt` pipeline.

## Expected flow

1. Airflow writes raw NDJSON envelopes into MinIO under `steam/raw/...`.
2. Trino exposes those paths as `hive.raw.*` external tables.
3. dbt reads `raw` sources, parses payload JSON in `staging`, and materializes curated tables in `stg` and `ods`.

## Run order

1. Create the raw schemas and external tables from `../trino/sql/landing_tables.sql`.
2. Copy `profiles.yml.example` into your dbt profile location and adjust credentials if needed.
3. From this folder run:

```bash
dbt debug
dbt run
dbt test
```
