"""Render DuckDB SQL templates and execute them via the duckdb CLI-equivalent API."""
from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parent.parent


def load_config() -> dict:
    with open(ROOT / "config" / "pipeline.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def render(template: str, params: dict) -> str:
    sql = template
    for key, value in params.items():
        sql = sql.replace(f"${key}", str(value))
    leftovers = re.findall(r"\$\w+", sql)
    if leftovers:
        raise ValueError(f"Unrendered params: {leftovers}")
    return sql


def strip_comments(sql: str) -> str:
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return "\n".join(lines)


def run(sql_path: str, params: dict) -> list[tuple]:
    template = (ROOT / sql_path).read_text(encoding="utf-8")
    sql = render(strip_comments(template), params)
    con = duckdb.connect()
    try:
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        result = []
        for stmt in statements:
            res = con.execute(stmt)
            if res is not None:
                result = res.fetchall()
        return result
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql", help="path to SQL template, relative to repo root")
    ap.add_argument("--param", action="append", default=[], help="key=value")
    args = ap.parse_args()

    params = dict(p.split("=", 1) for p in args.param)
    params.setdefault("mysql_host", os.environ.get("MYSQL_HOST", "localhost"))
    params.setdefault("mysql_port", os.environ.get("MYSQL_PORT", "3306"))
    if os.environ.get("MYSQL_USER"):
        params.setdefault("mysql_user", os.environ["MYSQL_USER"])
    if os.environ.get("MYSQL_PASSWORD"):
        params.setdefault("mysql_password", os.environ["MYSQL_PASSWORD"])
    params.setdefault("mysql_database", os.environ.get("MYSQL_DATABASE", "logs"))

    rows = run(args.sql, params)
    for row in rows:
        print("\t".join(map(str, row)))


if __name__ == "__main__":
    main()
