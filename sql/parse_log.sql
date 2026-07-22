-- Params: $log_file, $out_dir
-- Parses a combined-format nginx access log into partitioned Parquet.
-- ignore_errors keeps malformed lines from killing the job; DQ task verifies counts.

SET TimeZone = 'UTC';

CREATE OR REPLACE VIEW raw_lines AS
SELECT line
FROM read_csv(
    '$log_file',
    columns = {line: 'VARCHAR'},
    header = false,
    ignore_errors = true,
    max_line_size = 10000000
)
WHERE line IS NOT NULL AND length(line) > 10;

CREATE OR REPLACE TEMP TABLE parsed AS
SELECT
    regexp_extract(line, '^(\S+)', 1)                                        AS remote_ip,
    try_strptime(regexp_extract(line, '\[([^\]]+)\]', 1), '%d/%b/%Y:%H:%M:%S %z')::TIMESTAMP AS ts,
    regexp_extract(line, '"([A-Z]+) ', 1)                                    AS method,
    regexp_extract(line, '"[A-Z]+ (\S+)', 1)                                 AS path,
    TRY_CAST(regexp_extract(line, '" (\d{3}) ', 1) AS SMALLINT)              AS status,
    TRY_CAST(regexp_extract(line, '" \d{3} (\d+)', 1) AS BIGINT)             AS bytes,
    nullif(regexp_extract(line, '"([^"]*)"\s*$', 1), '')                     AS user_agent,
    md5(line)                                                                AS line_hash
FROM raw_lines
WHERE try_strptime(regexp_extract(line, '\[([^\]]+)\]', 1), '%d/%b/%Y:%H:%M:%S %z') IS NOT NULL;

COPY parsed TO '$out_dir'
    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000,
     PARTITION_BY (status), OVERWRITE_OR_IGNORE true);

-- counts for DQ check: written to stdout by runner
SELECT 'raw' AS kind, count(*) AS n FROM raw_lines
UNION ALL
SELECT 'parsed', count(*) FROM parsed;
