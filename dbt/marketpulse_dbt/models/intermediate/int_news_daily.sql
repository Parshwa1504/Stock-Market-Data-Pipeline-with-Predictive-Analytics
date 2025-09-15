with n as (
  select
    symbol,
    cast(published_at as date) as dt,
    count(*) as articles_1d
  from {{ ref('stg_news') }}
  group by 1,2
),

roll as (
  select
    symbol, dt, articles_1d,
    sum(articles_1d) over (
      partition by symbol order by dt
      rows between 2 preceding and current row
    ) as articles_3d
  from n
)

select * from roll
