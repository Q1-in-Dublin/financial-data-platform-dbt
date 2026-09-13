/

Readme · MD

# Airflow Financial Data Pipeline

A local, end-to-end ETL pipeline built with **Apache Airflow 3.x** and **PostgreSQL**, using synthetic financial transaction data. This project was built as a hands-on portfolio piece to demonstrate practical Airflow orchestration skills for a Data Engineer role transition.

## Project Purpose

Rather than only completing tutorials, this project validates, transforms, and loads realistic (Faker-generated) financial transaction data through a working Airflow DAG — including deliberately injected data quality issues (missing values, negative amounts) to exercise real validation logic.

## Architecture

```
CSV files (Faker-generated)
      ↓
   Airflow DAG (orchestration)
      ↓
   Extract   → read CSV with pandas
      ↓
   Validate  → check schema, missing values, outliers (pushes report via explicit XCom)
      ↓
   Transform → split into clean vs. rejected records
      ↓
   Load      → append clean rows to `transactions`, rejected rows to `rejected_transactions`
      ↓
   dbt (separate service, run independently of the DAG)
      ↓
   source → staging → mart models, schema tests, business rule tests
```

## Tech Stack

- **Apache Airflow 3.3.0** (Docker Compose, official quick-start setup)
- **PostgreSQL 16** (separate `financial_pipeline` database, isolated from Airflow's own metadata DB)
- **dbt-postgres 1.8.2** (Docker service, custom-built image — see `dbt/`) — SQL transformation and data quality layer on top of the raw `transactions`/`rejected_transactions` tables
- **Python** — pandas, Faker, SQLAlchemy
- **TaskFlow API** (`@dag`, `@task` decorators)

## dbt Layer

Airflow owns ingestion and orchestration; dbt owns SQL transformation and data quality on the same `financial_pipeline` Postgres database. This is a deliberate separation of responsibilities — see `PRD.md` for the full design rationale.

- Project lives under `dbt/` (`dbt_project.yml`, `profiles.yml`, `Dockerfile`, `macros/`)
- Runs as its own Docker Compose service (`dbt`), built from a plain `python:3.11-slim` image (the official `ghcr.io/dbt-labs/dbt-postgres` image has no arm64/Apple Silicon build)
- Connects to the same `financial_pipeline` database Airflow loads into, targeting an `analytics` schema (kept separate from the raw `public` schema)
- Run commands inside the container, e.g.:
  ```bash
  docker compose exec dbt dbt debug
  docker compose exec dbt dbt run
  docker compose exec dbt dbt test
  ```

## Additional DAGs (Day 7-8)

Beyond the main `financial_pipeline_dynamic` pipeline, this repo includes two focused DAGs that each demonstrate one Airflow concept in isolation, since these are common interview topics and easier to reason about separately from the full pipeline:

| DAG            | Concept                | What it demonstrates                                                                                                                                                                                                                                                                                                                              |
| -------------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sensor_demo`   | **Sensors**              | A `FileSensor` polls for a file's arrival (`poke_interval=10s`) rather than assuming the file already exists, mirroring how a real pipeline would wait on an upstream system.                                                                                                                                                                     |
| `backfill_demo` | **Backfill / Catchup**   | Demonstrates that a DAG Run's `data_interval_start` — not its actual execution time — determines which date it processes. Running `airflow backfill create --from-date ... --to-date ...` regenerates historical runs on demand, confirmed by task logs showing the target date (e.g. `2026-08-25`) even though the run executed on `2026-09-01`. |

## DAG: `financial_pipeline_dynamic`

This is the main pipeline. An earlier, non-dynamic version (`financial_pipeline`, with a hardcoded `pick_target_file` step) was retired once dynamic task mapping replaced it — everything below reflects the current, only-running DAG.

| Task        | Description                                                                                                                                                                                                                     |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `list_files`  | Scans `data/` for source CSVs (filters out `clean_*`/`rejected_*` derivatives)                                                                                                                                                    |
| `extract`     | `.expand()`s over every file found; loads each into a pandas DataFrame and logs its row count                                                                                                                                     |
| `validate`    | `.expand()`s per file; checks schema conformity, missing values (`amount`, `customer_id`), and negative amounts. Pushes a report to a dedicated XCom key (`validation_report`) and inserts one row per file into `pipeline_run_metrics` |
| `transform`   | `.expand()`s per file; splits rows into clean and rejected sets, tags rejected rows with a `rejection_reason`, writes both to separate CSVs                                                                                       |
| `load_all`    | Reduces the mapped per-file results back into a single task; bulk-loads all clean rows into `transactions` and rejected rows into `rejected_transactions` via `PostgresHook` (`chunksize=10000, method="multi"`), append-only    |

`extract`, `validate`, and `transform` fan out automatically over however many CSV files exist in `data/` — no DAG code changes needed as file count grows. `validate` also demonstrates **explicit XCom usage** (`ti.xcom_push(key=..., value=...)`) alongside TaskFlow's automatic return-value XCom, to push a structured report under its own key rather than overloading the task's return value.

## Data

Synthetic transaction data generated with the `Faker` library (`src/generate_data.py`), with the following schema:

| Column           | Description                   |
| ---------------- | ----------------------------- |
| `transaction_id` | Unique transaction ID         |
| `trade_date`     | Trade date                    |
| `customer_id`    | Customer ID                   |
| `account_id`     | Account ID                    |
| `security_id`    | Fake ISIN-style security code |
| `amount`         | Transaction amount            |
| `currency`       | Currency (EUR/USD/GBP)        |
| `country`        | Country code                  |

`src/generate_data.py` generates 100 files (`transactions_001.csv` … `transactions_100.csv`, 50,000 rows each — 5,000,000 rows total). `transaction_id` is a single counter shared across all files, so IDs are globally unique (no two files ever contain the same transaction).

Every file has the same small, realistic error rate injected directly into it — roughly 1% missing `amount` and 0.5% negative `amount` per file — rather than concentrating all bad rows into one dedicated "dirty" file. This forces `validate`/`transform` to actually filter each batch on its contents, not on which file it happens to be.

**Note:** No real company data, schemas, or field names are used anywhere in this project. All data is synthetic and generated locally.

## Result

> The counts below are from an early validation run against a small (500-row-per-file) synthetic dataset, kept here as a record that the append-only and dynamic-mapping behavior work as intended. The project has since scaled up to 100 files × 50,000 rows (5,000,000 rows total, ~1%/0.5% error rates spread across every file — see [Data](#data)); this section will be refreshed with current-scale numbers once dbt's data quality layer is in place to validate them properly.

Running the pipeline against `transactions_dirty.csv` (200 rows, with injected issues):

- Validation detected 12 rows with missing `amount` and 3 rows with negative `amount` (15 rows total flagged)
- Transform split the data into clean and rejected sets, tagging each rejected row with a `rejection_reason`
- Load appended the clean rows to `transactions` and the rejected rows to `rejected_transactions`, so re-running the DAG accumulates history in both tables rather than overwriting it
  `transactions` reflects two accumulated runs against `transactions_dirty.csv` (185 clean rows each); `rejected_transactions` was introduced in this iteration, so it currently reflects one run:

```sql
SELECT COUNT(*) FROM transactions;
-- 370  (185 clean rows × 2 runs, confirming append-only behavior)

SELECT COUNT(*) FROM rejected_transactions;
-- 15   (12 missing amount + 3 negative amount, from the latest run)
```

**Dynamic Task Mapping** (`financial_pipeline_dynamic`) was verified against all four source files at once, after resetting both tables:

```sql
SELECT COUNT(*) FROM transactions;
-- 1685  (500 + 500 + 500 + 185 clean rows across all four files)

SELECT COUNT(*) FROM rejected_transactions;
-- 15    (all from transactions_dirty.csv, the only file with injected issues)
```

This confirms `extract`/`validate`/`transform` correctly fanned out over all four files without any hardcoded file selection.

## Setup

```bash
# 1. Create virtual environment and install dependencies
python3 -m venv venv
source venv/bin/activate
pip install faker pandas

# 2. Generate synthetic data
./venv/bin/python src/generate_data.py

# 3. Start Airflow (Docker Compose)
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d

# 4. Create a dedicated Postgres database for pipeline data
docker compose exec postgres psql -U airflow -c "CREATE DATABASE financial_pipeline;"

# 5. Register a Postgres connection in Airflow UI
# Admin > Connections > Add:
#   Connection Id: financial_pipeline_db
#   Connection Type: Postgres
#   Host: postgres
#   Login: airflow
#   Password: airflow
#   Port: 5432
#   Database: financial_pipeline
```

Airflow UI: [http://localhost:8080](http://localhost:8080) (login: `airflow` / `airflow`)

## Repository Structure

```
financial-data-platform-dbt/
├── dags/
│   ├── financial_pipeline_dynamic.py   # Dynamic Task Mapping (main pipeline)
│   ├── sensor_demo.py                  # Sensors
│   └── backfill_demo.py                # Backfill / Catchup
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── Dockerfile                      # dbt-postgres, built locally (no arm64 official image)
│   └── macros/
├── migrations/                         # hand-run SQL, no migration tool wired up yet
├── src/
│   └── generate_data.py                # 100 files x 50,000 rows, globally unique transaction_id
├── data/
│   └── transactions_001.csv … transactions_100.csv
├── config/
├── plugins/
├── docs/
├── tests/            # planned
├── docker-compose.yaml                 # postgres, redis, airflow-*, dbt
├── PRD.md                              # full design doc (gitignored, local only)
├── .gitignore
└── README.md
```

## What's Next

- **dbt staging dedup** — `financial_pipeline_dynamic` stays append-only by design (see `PRD.md`); deduplication on `transaction_id` now happens downstream in the dbt staging layer instead of at load time
- **dbt source → staging → intermediate → fact → mart models**, schema tests and business rule tests (in progress — see `dbt/` and `PRD.md`)
- Unit tests for `transform`/`validate` logic
- Optional: package the local Docker Compose setup more formally for one-command reproducibility

## Learning Context

This project was built over a condensed 8-day self-study plan covering:

1. Airflow architecture fundamentals (DAG, Task, DAG Run, Scheduler, Executor, Worker)
2. Writing DAGs with sequential, parallel, and branching task dependencies
3. Scheduling concepts (data intervals, `catchup`, retries)
4. A full extract → validate → transform → load implementation
5. Explicit XCom usage and splitting output into clean vs. rejected data stores
6. Dynamic Task Mapping, Sensors, and Backfill/Catchup — verified with `financial_pipeline_dynamic`, `sensor_demo`, and `backfill_demo`
   Along the way, this also involved troubleshooting real-world issues: Docker volume mounts, `venv`/`PATH` conflicts, Git repository scope mistakes, GitHub credential/authentication issues, connection configuration errors (e.g. a stray whitespace character in a hostname causing a DNS resolution failure), an accidentally committed secret (`fernet_key`) that required rewriting Git history, DAG files landing in the wrong directory or under a mismatched `dag_id`, and CLI argument names that shifted between Airflow versions — all fixed through log-driven debugging.
