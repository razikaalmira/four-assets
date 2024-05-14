{{ config(materialized='table') }}

select
    "CPI" as cpi,
    to_date(
    case 
        when month_code between 1 and 9 then concat("Year",'-0',month_code,'-01')
        else concat("Year",'-',month_code,'-01')
    end,
    'YYY-MM-DD') as monthdate
from {{ source('dev','cpi') }}