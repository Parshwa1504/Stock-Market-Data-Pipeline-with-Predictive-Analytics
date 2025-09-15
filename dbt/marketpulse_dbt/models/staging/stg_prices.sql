with src as (
    select * from {{ source('raw', 'RAW_PRICES') }}
)
select
    symbol,
    to_timestamp(ts) as trade_time,
    open,
    high,
    low,
    close,
    volume
from src
