select
  symbol,
  dt as date,
  open, high, low, close, volume,
  ret_d1, ret_5d, vol_20d
from {{ ref('int_prices_enriched') }}
