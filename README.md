# Nginx access.log → MySQL 8.0 + GCS

Daily batch pipeline: nginx access.log → DuckDB (parse → Parquet) → GCS (archive) → MySQL 8.0 (structured) → dbt (marts).
Volume target: ~20 GB raw logs/day, daily batch after logrotate.

## Stack
- Airflow 3.0 (orchestration, TaskFlow API) — tested in Docker (`apache/airflow:3.0.0`)
- DuckDB (parsing → Parquet, MySQL load via `mysql` extension)
- MySQL 8.0 (no `LOAD DATA INFILE` — batched INSERTs only)
- dbt-core 1.7 + dbt-mysql (staging view + marts)
- `gcloud storage rsync` (GCS upload)

## Flow
```
logrotate → access.log-YYYYMMDD
   │
   ├─ wait_for_rotated_log   FileSensor
   ├─ parse_to_parquet       DuckDB: regex parse → Parquet (zstd, PARTITION_BY status)
   ├─ dq_check               raw vs parsed line count, threshold in config/pipeline.yaml
   ├─ upload_to_gcs          gcloud storage rsync → gs://<bucket>/nginx-logs/date=YYYY-MM-DD/
   ├─ load_to_mysql          DELETE day (mysql-connector) + INSERT via DuckDB mysql extension
   ├─ dbt_run                staging view → marts (fct_requests_hourly, fct_errors_daily)
   └─ cleanup                remove local staging parquet
```

## Layout
```
dags/nginx_logs_pipeline.py   Airflow 3.0 DAG (TaskFlow)
sql/parse_log.sql             DuckDB: access.log → Parquet
sql/mysql_load.sql            DuckDB: Parquet → stg_access_log
sql/init_schema.sql           MySQL DDL (stg_access_log partitioned, pipeline_runs audit)
scripts/run_duckdb.py         SQL template runner ($param substitution)
scripts/upload_gcs.ps1        gcloud storage rsync wrapper
dbt/                          dbt project (staging + marts), profiles.yml uses env vars
config/pipeline.yaml          thresholds, chunk sizes, retention
testdata/logs/                sample log for local/Docker testing
docker-compose.yml            mysql:8.0 + airflow:3.0.0 test stack
```

## Local setup (Windows, .venv)
```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env   # fill MYSQL_*, GCS_BUCKET, LOG_DIR
```

Note: Airflow 3.0 has no native Windows support — run it in Docker/WSL2/Linux. DuckDB and dbt steps work natively on Windows.

## GCS setup (permissions to push Parquet)

Upload is done by `gcloud storage rsync` (task `upload_to_gcs`), running wherever Airflow workers run. It needs a GCP identity with write access to the target bucket.

### 1. Create bucket
```bash
gcloud storage buckets create gs://YOUR_BUCKET \
  --project=YOUR_PROJECT --location=europe-west1 --uniform-bucket-level-access
```
Uniform bucket-level access is required for the IAM approach below (no per-object ACLs).

### 2. Create a dedicated service account
```bash
gcloud iam service-accounts create nginx-logs-uploader \
  --project=YOUR_PROJECT --display-name="Nginx logs pipeline uploader"
```

### 3. Grant minimal permissions on the bucket
The pipeline only writes objects (and lists them for rsync). Grant on the **bucket**, not the project:
```bash
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET \
  --member="serviceAccount:nginx-logs-uploader@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"
```
`roles/storage.objectUser` = `storage.objects.create/get/list/delete` — enough for rsync (delete needed only because rsync syncs removals; use `roles/storage.objectCreator` if you switch to plain `cp` and want write-only).

Do NOT use `roles/storage.admin` — it allows bucket deletion and IAM changes.

### 4. Credentials for Airflow workers
Pick one:

**a) Workload Identity / attached SA (recommended, prod on GCP)**
Attach `nginx-logs-uploader` to the GCE/GKE/Cloud Run workload running Airflow. Nothing to configure — `gcloud` picks up metadata server credentials. Skip step 5.

**b) Service account key JSON (local/Docker testing)**
```bash
gcloud iam service-accounts keys create sa-key.json \
  --iam-account=nginx-logs-uploader@YOUR_PROJECT.iam.gserviceaccount.com
```
Store outside the repo (it is a secret). Point `GOOGLE_APPLICATION_CREDENTIALS` at it (see `.env.example`).

### 5. Wire it into the pipeline
`.env`:
```
GCS_BUCKET=YOUR_BUCKET
GCS_PREFIX=nginx-logs
GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```
For Docker, mount the key and activate it in the container:
```yaml
volumes:
  - C:\secure\sa-key.json:/secrets/sa-key.json:ro
environment:
  GOOGLE_APPLICATION_CREDENTIALS: /secrets/sa-key.json
```
and add to the airflow command before the DAG run:
```bash
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
```
(The `apache/airflow` image does not ship `gcloud` — install it in a custom Dockerfile, or switch the upload task to the `google-cloud-storage` Python client, which reads `GOOGLE_APPLICATION_CREDENTIALS` natively without the SDK.)

### 6. Verify
```bash
gcloud storage cp testfile.parquet gs://YOUR_BUCKET/nginx-logs/test/
gcloud storage ls gs://YOUR_BUCKET/nginx-logs/test/
```

### Test without GCS
Set `SKIP_GCS=1` — the `upload_to_gcs` task becomes a no-op (used in docker-compose for local E2E).

## MySQL setup
```bash
mysql < sql/init_schema.sql
```
Creates:
- `stg_access_log` — `PRIMARY KEY (line_hash, ts)`, `PARTITION BY RANGE (TO_DAYS(ts))` with `p_max` catch-all. For retention/performance add daily partitions (e.g. via a small cron/event) and drop old ones instead of DELETE.
- `pipeline_runs` — append-only audit, one row per step per run.

## Docker E2E test
```powershell
docker compose up --abort-on-container-exit   # mysql:8.0 + airflow:3.0.0, SKIP_GCS=1
docker compose exec mysql mysql -uetl -petl logs -e "SELECT count(*) FROM stg_access_log"
docker compose down -v                        # full reset (drops MySQL data)
```
MySQL is exposed on `localhost:3307` (etl/etl, db `logs`). The compose command runs `airflow db migrate`, `airflow dags reserialize` (required in Airflow 3.0 before `dags test`), then `airflow dags test nginx_logs_pipeline 2026-07-22`.

## Idempotency
- Parse: `COPY ... OVERWRITE_OR_IGNORE true` — safe to re-run for the same date.
- MySQL load: task deletes rows for the log date (native `mysql-connector-python`) then inserts — re-runs never duplicate.
- Dedup safety net: `line_hash` (md5 of the raw line) is part of the PK.
- dbt models are full-refresh tables — safe to re-run.

## Astronomer (astro dev) deployment

The DAG runs on Astronomer Runtime 3.x (Airflow 3.0). To avoid breaking existing DAGs (e.g. `DbtDag`/cosmos with their own dbt-core):

### requirements.txt in your astro project — add ONLY these
```
duckdb>=1.1.0
mysql-connector-python>=8.0
PyYAML>=6.0
python-dotenv>=1.0
```
**Do NOT add `dbt-core`/`dbt-mysql` here.** `dbt-mysql` pins `dbt-core~=1.7.0` and would downgrade the global dbt-core your existing cosmos DAGs depend on.

### Isolated dbt venv — add to your astro Dockerfile
```dockerfile
RUN python -m venv /usr/local/airflow/dbt_nginx_venv && \
    /usr/local/airflow/dbt_nginx_venv/bin/pip install --no-cache-dir dbt-mysql
```
Then set in astro `.env`:
```
DBT_BINARY=/usr/local/airflow/dbt_nginx_venv/bin/dbt
AIRFLOW_CONN_FS_DEFAULT={"conn_type":"fs","extra":{"path":"/"}}
LOG_DIR=/usr/local/airflow/logs/nginx
STAGING_DIR=/usr/local/airflow/staging
PIPELINE_ROOT=/usr/local/airflow/dags/KINGINXLOGS
MYSQL_HOST=... (etc.)
```
The DAG's `dbt_run` task uses `DBT_BINARY` (defaults to global `dbt`), so your dbt-mysql runs fully isolated — zero impact on `DbtDag` DAGs and their dbt version.

### Other astro notes
- No need for `airflow dags reserialize` — astro's dag processor handles serialization (that step was only for the bare-container CLI test).
- `SKIP_GCS` unset → real GCS upload; astro dev container needs `gcloud` + creds (custom Dockerfile or switch the upload task to `google-cloud-storage` Python client).
- DuckDB downloads its `mysql` extension from the internet on first `INSTALL mysql` — needs outbound network from the container.
- Copy this repo into your astro project (e.g. under `dags/KINGINXLOGS/`) and point `PIPELINE_ROOT` there.

### Option A: standalone dbt project (this repo's `dbt/`)
Set `DBT_BINARY` to a venv with `dbt-mysql` (dbt-core 1.7.x). The DAG runs `dbt run --project-dir <this repo>/dbt --profiles-dir <this repo>/dbt`.

### Option B: models inside your existing dbt project
Copy `dbt/models/staging/stg_nginx_logs.sql` + `dbt/models/marts/*.sql` into your project, and merge the `stg_access_log` table into your existing `sources.yml` (adjust the source name and the `{{ source('raw', 'stg_access_log') }}` reference in the staging model). Models are tagged `nginx_logs`, so the DAG can build just them:
```
DBT_BINARY=/path/to/your/dbt_venv/bin/dbt
DBT_PROJECT_DIR=/usr/local/airflow/dags/your_dbt_project
DBT_SELECT=tag:nginx_logs
# DBT_PROFILES_DIR unset -> uses your default ~/.dbt/profiles.yml
```
Notes: keep the profile's schema = the same MySQL database that holds `stg_access_log`; check for model name collisions (`stg_nginx_logs`, `fct_requests_hourly`, `fct_errors_daily`); `fct_*` marts are full-refresh tables, rebuilt daily — fine at this size, but exclude them from any global `dbt build` if you don't want them refreshed by other DAGs (or select them by tag only here).

## Airflow CLI cheatsheet (daily ops)

Run inside the Airflow container: `astro dev bash` (Astronomer) or `docker compose exec airflow bash`.

```bash
# -- monitoring
airflow dags list-runs -d nginx_logs_pipeline                # recent runs + states
airflow dags list-runs -d nginx_logs_pipeline --state failed # failed runs only
airflow tasks states-for-dag-run nginx_logs_pipeline 2026-07-22T00:00:00+00:00
airflow dags show nginx_logs_pipeline                        # rendered DAG graph (text)

# -- manual runs
airflow dags trigger nginx_logs_pipeline --logical-date 2026-07-22   # backfill single day
airflow dags backfill -d nginx_logs_pipeline -s 2026-07-20 -e 2026-07-22  # range reprocess
airflow dags test nginx_logs_pipeline 2026-07-22             # one-off local run (no scheduler)

# -- retry / clear (idempotent tasks make this safe)
airflow tasks clear nginx_logs_pipeline -s 2026-07-22 -e 2026-07-22            # re-run whole day
airflow tasks clear nginx_logs_pipeline -t load_to_mysql -s 2026-07-22 -e 2026-07-22  # single task
airflow tasks test nginx_logs_pipeline parse_to_parquet 2026-07-22             # debug one task locally

# -- pause / resume
airflow dags pause nginx_logs_pipeline
airflow dags unpause nginx_logs_pipeline

# -- config & connections
airflow connections get fs_default
airflow connections add fs_default --conn-type fs --conn-extra '{"path":"/"}'
airflow variables list
airflow config get-value core dags_folder

# -- health / maintenance
airflow db check
airflow db migrate
airflow dags reserialize          # force re-parse after DAG code change without scheduler restart
airflow dags list-import-errors   # broken DAGs
airflow jobs check --job-type SchedulerJob
```

Pipeline-specific notes:
- Re-running a day is safe at every step: parse overwrites parquet, MySQL load deletes-then-inserts the day, dbt marts are full-refresh.
- Audit trail: `SELECT * FROM pipeline_runs WHERE log_date = '2026-07-22' ORDER BY id;` in MySQL.

## Known limitations
- **DuckDB `mysql` extension pushdown bug**: DELETE/SELECT with temporal filters generates `'literal'::TIMESTAMP` casts that MySQL rejects. Workaround in place: all temporal DELETEs go through the native MySQL client; DuckDB is used only for INSERT (which translates correctly).
- DuckDB mysql extension has no `INSERT IGNORE` support — hence delete-then-insert idempotency.
- Parquet output is partitioned by `status`; if you see many tiny files at 20 GB/day scale, drop `PARTITION_BY` in `sql/parse_log.sql` or partition by hour.
- dbt-mysql pins `dbt-core` to 1.7.x (see requirements.txt).

## Config reference
| File | What |
|---|---|
| `.env` | secrets/env: MYSQL_*, GCS_BUCKET, GCS_PREFIX, GOOGLE_APPLICATION_CREDENTIALS, LOG_DIR, STAGING_DIR, SKIP_GCS |
| `config/pipeline.yaml` | log filename pattern, gzip flag, parquet options, DQ threshold, staging retention |
| `dbt/profiles.yml` | dbt MySQL connection (env-var driven) |
