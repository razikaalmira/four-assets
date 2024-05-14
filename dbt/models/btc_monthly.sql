{{ config(materialized='view') }}

select
    cast(date_trunc('month',date) as date) monthdate,
    avg(closing) avg_closing
from {{ ref('btc_daily') }}
group by 1