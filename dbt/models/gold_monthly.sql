{{ config(materialized='view') }}

select
    cast(date_trunc('month',date) as date) monthdate,
    avg(closing) avg_closing
from {{ ref('gold_daily') }}
group by 1