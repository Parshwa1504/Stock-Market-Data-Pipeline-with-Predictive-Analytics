with src as (
    select * from {{ source('raw', 'RAW_EARNINGS') }}
)
select
    symbol,
    report_date,
    actual_eps,
    consensus_eps,
    surprise_pct
from src
