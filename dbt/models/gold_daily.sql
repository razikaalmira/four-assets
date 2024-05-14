{{ config(materialized='table') }}

select
    to_date(tanggal,'DD/MM/YY') as date,
    cast(replace(terakhir,'.','') as int) as closing,
    cast(replace(pembukaan,'.','') as int) as opening,
    cast(replace(tertinggi,'.','') as int) as high,
    cast(replace(terendah,'.','') as int) as low
    
from {{ source('dev','gold') }}