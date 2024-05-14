{{ config(materialized='view') }}

with
btc as (
    select
        monthdate,
        avg_closing / lag(avg_closing) over (order by monthdate) - 1 as btc_mom_change
    from {{ ref('btc_monthly')}}
    order by 1
),
cpi as (
    select
        monthdate,
        cpi / lag(cpi) over (order by monthdate) - 1 as cpi_mom_change
    from {{ ref('cpi_monthly')}}
    order by 1
),
gold as (
    select
        monthdate,
        avg_closing / lag(avg_closing) over (order by monthdate) - 1 as gold_mom_change
    from {{ ref('gold_monthly')}}
    order by 1
),
ihsg as (
    select
        monthdate,
        avg_closing / lag(avg_closing) over (order by monthdate) - 1 as ihsg_mom_change
    from {{ ref('ihsg_monthly')}}
    order by 1
),
usd as (
    select
        monthdate,
        avg_closing / lag(avg_closing) over (order by monthdate) - 1 as usd_mom_change
    from {{ ref('usd_monthly')}}
    order by 1
)

select
    c.monthdate,
    c.cpi_mom_change,
    b.btc_mom_change,
    u.usd_mom_change,
    i.ihsg_mom_change,
    g.gold_mom_change
from cpi c
left join btc b
on c.monthdate = b.monthdate
left join usd u
on c.monthdate = u.monthdate
left join ihsg i
on c.monthdate = i.monthdate
left join gold g
on c.monthdate = g.monthdate
