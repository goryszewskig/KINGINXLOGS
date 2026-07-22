{{ config(materialized='table') }}

select
    date_format(ts, '%Y-%m-%d %H:00:00') as hour_bucket,
    count(*) as requests,
    count(distinct remote_ip) as unique_ips,
    sum(bytes) as total_bytes,
    sum(case when status_class = '5xx' then 1 else 0 end) as errors_5xx,
    sum(case when status_class = '4xx' then 1 else 0 end) as errors_4xx,
    round(sum(case when status_class = '5xx' then 1 else 0 end) / count(*) * 100, 2) as error_5xx_pct
from {{ ref('stg_nginx_logs') }}
group by date_format(ts, '%Y-%m-%d %H:00:00')
