with type_payout_analysis as (
  select
--     id,
    --     source_id,
        -- SPLIT_PART(source_id, '_', 1) type,
        type
--     reporting_category type,
    --     type,
    max(created) created_max,
    automatic_transfer_id payout_id
  from
    balance_transactions
  where
    created > date '2020-10-10'
    and automatic_transfer_id is not null
  group by
    1,
    3 --  order by
    --  3,2,1
),
payout_analysis as (
  select
    min(created) min_created,
    max(created) max_created,
    automatic_transfer_id payout_id

  from
    balance_transactions
  where
    created > date '2020-09-01'
    and automatic_transfer_id is not null
   group by 3
)
select
--   source_id,
--   id,
  type,
  --    as activity_date,
  created_max raw_date,
  
  date_format(
    created_max at time zone 'America/New_York',
    '%r'
  ) AS trx_date,
  payout_id
from
  type_payout_analysis
order by
  payout_id,
  raw_date,
  type
