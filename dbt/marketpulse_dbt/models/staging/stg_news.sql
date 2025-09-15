with src as (
    select * from {{ source('raw', 'RAW_NEWS') }}
)
select
    symbol,
    published_at,
    headline,
    sentiment
from src
