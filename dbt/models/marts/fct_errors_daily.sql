{{ config(materialized='table') }}

select
    date(ts) as log_date,
    path_clean,
    status,
    count(*) as error_count,
    count(distinct remote_ip) as affected_ips
from {{ ref('stg_nginx_logs') }}
where status >= 400
group by date(ts), path_clean, status
order by error_count desc
