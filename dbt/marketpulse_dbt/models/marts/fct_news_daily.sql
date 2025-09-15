select
  symbol,
  dt as date,
  coalesce(articles_1d, 0) as articles_1d,
  coalesce(articles_3d, 0) as articles_3d
from {{ ref('int_news_daily') }}
