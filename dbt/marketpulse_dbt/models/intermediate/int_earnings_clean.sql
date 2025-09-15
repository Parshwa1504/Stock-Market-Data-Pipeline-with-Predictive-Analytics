with e as (
  select
    symbol,
    cast(report_date as date) as report_date,
    actual_eps,
    consensus_eps,
    case
      when surprise_pct is not null then surprise_pct
      when consensus_eps is not null and consensus_eps != 0
        then (actual_eps - consensus_eps) / consensus_eps * 100
      else null
    end as surprise_pct
  from {{ ref('stg_earnings') }}
)

select * from e
