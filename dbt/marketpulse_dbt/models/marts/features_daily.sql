with prices as (
  select * from {{ ref('fct_prices_daily') }}
),
news as (
  select * from {{ ref('fct_news_daily') }}
),
earn as (
  select * from {{ ref('fct_earnings') }}
)

select
  p.date,
  p.symbol,
  p.close,
  p.ret_d1,
  p.ret_5d,
  p.vol_20d,
  coalesce(n.articles_1d, 0)  as articles_1d,
  coalesce(n.articles_3d, 0)  as articles_3d,
  e.surprise_pct,
  case
    when lead(p.close) over (partition by p.symbol order by p.date) > p.close
      then 1 else 0
  end as label_up_next_day
from prices p
left join news n  on p.symbol = n.symbol and p.date = n.date
left join earn e  on p.symbol = e.symbol and p.date = e.date
