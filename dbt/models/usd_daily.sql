{{ config(materialized='table') }}

select
    to_date(tanggal,'DD/MM/YY') as date,
    cast(replace(replace(terakhir,'.',''),',','.') as float) as closing,
    cast(replace(replace(pembukaan,'.',''),',','.') as float) as opening,
    cast(replace(replace(tertinggi,'.',''),',','.') as float) as high,
    cast(replace(replace(terendah,'.',''),',','.') as float) as low
    
from {{ source('dev','usd') }}