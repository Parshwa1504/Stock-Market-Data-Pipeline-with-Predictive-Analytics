select
  symbol,
  report_date as date,
  actual_eps,
  consensus_eps,
  surprise_pct
from {{ ref('int_earnings_clean') }}
