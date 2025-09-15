with p as (
  select
    symbol,
    cast(trade_time as date) as dt,
    open, high, low, close, volume
  from {{ ref('stg_prices') }}
),

returns as (
  select
    symbol, dt, open, high, low, close, volume,
    close / lag(close) over (partition by symbol order by dt) - 1     as ret_d1,
    close / lag(close, 5) over (partition by symbol order by dt) - 1  as ret_5d
  from p
),

vol as (
  select
    *,
    avg(abs(ret_d1)) over (
      partition by symbol
      order by dt
      rows between 19 preceding and current row
    ) as vol_20d
  from returns
)

select * from vol
