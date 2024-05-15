{{ config(materialized='view') }}

with
cpi_year as (
    select
        cast(date_trunc('year',monthdate) as date) yeardate,
        avg(cpi) avg_cpi
    from {{ ref('cpi_monthly') }}
    group by 1
),

btc_year as (
    select
        cast(date_trunc('year',date) as date) yeardate,
        avg(closing) avg_closing
    from {{ ref('btc_daily')}}
    group by 1
),

usd_year as (
    select
        cast(date_trunc('year',date) as date) yeardate,
        avg(closing) avg_closing
    from {{ ref('usd_daily')}}
    group by 1
),

gold_year as (
    select
        cast(date_trunc('year',date) as date) yeardate,
        avg(closing) avg_closing
    from {{ ref('gold_daily')}}
    group by 1
),

ihsg_year as (
    select
        cast(date_trunc('year',date) as date) yeardate,
        avg(closing) avg_closing
    from {{ ref('ihsg_daily')}}
    group by 1
),

cpi as (
    select 
        yeardate,
        avg_cpi / lag(avg_cpi) over (order by yeardate) - 1 as cpi_yoy_change
    from cpi_year
    order by 1
),

btc as (
    select
        yeardate,
        avg_closing / lag(avg_closing) over (order by yeardate) - 1 as btc_yoy_change
    from btc_year
    order by 1
),

gold as (
    select
        yeardate,
        avg_closing / lag(avg_closing) over (order by yeardate) - 1 as gold_yoy_change
    from gold_year
    order by 1
),
ihsg as (
    select
        yeardate,
        avg_closing / lag(avg_closing) over (order by yeardate) - 1 as ihsg_yoy_change
    from ihsg_year
    order by 1
),
usd as (
    select
        yeardate,
        avg_closing / lag(avg_closing) over (order by yeardate) - 1 as usd_yoy_change
    from usd_year
    order by 1
)

select
    c.yeardate,
    c.cpi_yoy_change,
    b.btc_yoy_change,
    u.usd_yoy_change,
    i.ihsg_yoy_change,
    g.gold_yoy_change
from cpi c
left join btc b
on c.yeardate = b.yeardate
left join usd u
on c.yeardate = u.yeardate
left join ihsg i
on c.yeardate = i.yeardate
left join gold g
on c.yeardate = g.yeardate
