"""Daily nginx access.log pipeline: parse -> Parquet -> GCS -> MySQL -> dbt."""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

ROOT = Path(os.environ.get("PIPELINE_ROOT", Path(__file__).resolve().parent.parent))
LOG_DIR = Path(os.environ["LOG_DIR"])
STAGING_DIR = Path(os.environ.get("STAGING_DIR", ROOT / "staging"))


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {cmd}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    return proc


def _cfg() -> dict:
    with open(ROOT / "config" / "pipeline.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)["pipeline"]


def _record_step(log_date: str, filename: str, step: str, status: str, **metrics) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL mysql; LOAD mysql;")
    con.execute(
        f"ATTACH 'host={os.environ['MYSQL_HOST']} user={os.environ['MYSQL_USER']} "
        f"password={os.environ['MYSQL_PASSWORD']} database={os.environ['MYSQL_DATABASE']}' AS my (TYPE mysql)"
    )
    cols = {"log_date": log_date, "filename": filename, "step": step, "status": status, **metrics}
    params = {k: cols.get(k) for k in ("log_date", "filename", "step", "status", "raw_lines", "parsed_lines", "gcs_path", "mysql_rows")}
    con.execute(
        "INSERT INTO my.pipeline_runs (log_date, filename, step, status, raw_lines, parsed_lines, gcs_path, mysql_rows) "
        "VALUES ($log_date, $filename, $step, $status, $raw_lines, $parsed_lines, $gcs_path, $mysql_rows)",
        params,
    )
    con.close()


@dag(
    dag_id="nginx_logs_pipeline",
    schedule="30 2 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["nginx", "etl"],
)
def nginx_logs_pipeline():
    cfg = _cfg()

    log_file = FileSensor(
        task_id="wait_for_rotated_log",
        filepath=str(LOG_DIR / "access.log-{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"),
        poke_interval=300,
        timeout=3 * 3600,
    )

    @task()
    def parse_to_parquet(ds: str) -> dict:
        log_date = ds
        filename = f"access.log-{datetime.strptime(ds, '%Y-%m-%d').strftime('%Y%m%d')}"
        src = LOG_DIR / filename
        if cfg["gzip"]:
            src = src.with_suffix(src.suffix + ".gz")
        out_dir = STAGING_DIR / f"date={log_date}"
        out_dir.mkdir(parents=True, exist_ok=True)

        result = _run(
            [sys.executable, str(ROOT / "scripts" / "run_duckdb.py"), "sql/parse_log.sql",
             "--param", f"log_file={src}", "--param", f"out_dir={out_dir}"],
        )
        counts = {}
        for line in result.stdout.splitlines():
            kind, _, n = line.partition("\t")
            if kind in ("raw", "parsed"):
                counts[kind] = int(n)
        return {"log_date": log_date, "filename": src.name, "out_dir": str(out_dir), **counts}

    @task()
    def dq_check(parse_result: dict) -> dict:
        raw, parsed = parse_result["raw"], parse_result["parsed"]
        loss_pct = (raw - parsed) / raw * 100 if raw else 0
        if loss_pct > cfg["dq"]["max_loss_pct"]:
            raise ValueError(f"Parse loss {loss_pct:.3f}% > {cfg['dq']['max_loss_pct']}%")
        _record_step(parse_result["log_date"], parse_result["filename"], "parse", "success",
                     raw_lines=raw, parsed_lines=parsed)
        return parse_result

    @task()
    def upload_to_gcs(parse_result: dict) -> dict:
        if os.environ.get("SKIP_GCS"):
            parse_result["gcs_path"] = "skipped"
            return parse_result
        bucket, prefix = os.environ["GCS_BUCKET"], os.environ.get("GCS_PREFIX", "nginx-logs")
        gcs_path = f"gs://{bucket}/{prefix}/date={parse_result['log_date']}/"
        script = ROOT / "scripts" / ("upload_gcs.ps1" if os.name == "nt" else "upload_gcs.sh")
        if os.name == "nt":
            cmd = ["powershell", "-File", str(script), "-LocalDir", parse_result["out_dir"],
                   "-LogDate", parse_result["log_date"]]
        else:
            cmd = ["gcloud", "storage", "rsync", parse_result["out_dir"], gcs_path, "--recursive"]
        subprocess.run(cmd, check=True)
        _record_step(parse_result["log_date"], parse_result["filename"], "gcs", "success", gcs_path=gcs_path)
        return parse_result

    @task()
    def load_to_mysql(parse_result: dict) -> dict:
        log_date = parse_result["log_date"]
        table = cfg["mysql"]["staging_table"]

        import mysql.connector

        conn = mysql.connector.connect(
            host=os.environ["MYSQL_HOST"], port=int(os.environ.get("MYSQL_PORT", 3306)),
            user=os.environ["MYSQL_USER"], password=os.environ["MYSQL_PASSWORD"],
            database=os.environ["MYSQL_DATABASE"],
        )
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE ts >= %s AND ts < %s + INTERVAL 1 DAY",
                    (log_date, log_date))
        conn.commit()
        cur.close()
        conn.close()

        glob_path = str(Path(parse_result["out_dir"]) / "**" / "*.parquet").replace("\\", "/")
        result = _run(
            [sys.executable, str(ROOT / "scripts" / "run_duckdb.py"), "sql/mysql_load.sql",
             "--param", f"staging_glob={glob_path}",
             "--param", f"staging_table={cfg['mysql']['staging_table']}",
             "--param", f"log_date={parse_result['log_date']}"],
        )
        rows = int(result.stdout.strip().splitlines()[-1])
        _record_step(parse_result["log_date"], parse_result["filename"], "mysql", "success", mysql_rows=rows)
        return parse_result

    @task()
    def dbt_run(parse_result: dict) -> dict:
        subprocess.run(["dbt", "run", "--project-dir", str(ROOT / "dbt"),
                        "--profiles-dir", str(ROOT / "dbt")], check=True)
        _record_step(parse_result["log_date"], parse_result["filename"], "dbt", "success")
        return parse_result

    @task()
    def cleanup(parse_result: dict) -> None:
        if not cfg["retention"]["keep_staging_files"]:
            import shutil
            shutil.rmtree(parse_result["out_dir"], ignore_errors=True)

    parsed = dq_check(parse_to_parquet())
    uploaded = upload_to_gcs(parsed)
    loaded = load_to_mysql(uploaded)
    cleanup(dbt_run(loaded))

    log_file >> parsed


nginx_logs_pipeline()
