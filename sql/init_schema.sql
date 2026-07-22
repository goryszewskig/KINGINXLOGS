CREATE DATABASE IF NOT EXISTS logs CHARACTER SET utf8mb4;
USE logs;

CREATE TABLE IF NOT EXISTS stg_access_log (
    line_hash   CHAR(32)      NOT NULL,
    remote_ip   VARCHAR(45)   NOT NULL,
    ts          DATETIME      NOT NULL,
    method      VARCHAR(10)   NULL,
    path        TEXT          NULL,
    status      SMALLINT      NULL,
    bytes       BIGINT        NULL,
    user_agent  TEXT          NULL,
    PRIMARY KEY (line_hash, ts)
) ENGINE=InnoDB
PARTITION BY RANGE (TO_DAYS(ts)) (
    PARTITION p_init VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p_max VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    log_date      DATE         NOT NULL,
    filename      VARCHAR(255) NOT NULL,
    step          VARCHAR(32)  NOT NULL,   -- parse | dq | gcs | mysql | dbt
    status        VARCHAR(16)  NOT NULL,   -- success | failed
    raw_lines     BIGINT       NULL,
    parsed_lines  BIGINT       NULL,
    gcs_path      VARCHAR(512) NULL,
    mysql_rows    BIGINT       NULL,
    started_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at   DATETIME     NULL,
    error         TEXT         NULL,
    KEY idx_run (log_date, step)
) ENGINE=InnoDB;
