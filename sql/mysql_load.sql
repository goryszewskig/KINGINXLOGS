-- Params: $staging_glob, $chunk_rows, $mysql_host, $mysql_port,
--         $mysql_user, $mysql_password, $mysql_database, $staging_table
-- Loads Parquet staging files into MySQL via the DuckDB mysql extension.
-- Idempotent: deletes the log date's rows before inserting; line_hash PK is the dedup safety net.

INSTALL mysql; LOAD mysql;

ATTACH 'host=$mysql_host port=$mysql_port user=$mysql_user password=$mysql_password database=$mysql_database'
    AS my (TYPE mysql);

CREATE OR REPLACE TEMP TABLE to_load AS
SELECT remote_ip, ts, method, path, status, bytes, user_agent, line_hash
FROM read_parquet('$staging_glob', hive_partitioning = true);

-- idempotency: the DAG task deletes this log date's rows via a native MySQL
-- client before running this script (duckdb mysql extension generates
-- 'literal'::TIMESTAMP casts in pushdown, which MySQL rejects).
INSERT INTO my.$staging_table
BY NAME
SELECT * FROM to_load;

SELECT count(*) AS rows_to_load FROM to_load;
