-- calendar_days is a temporary table with a row and value for every day between the timestamps
-- To edit the dates for this query, change the dates in sequence() 
with date_selection as (
    select 
      date('2019-12-07') as date_min,
      date('2019-12-13') as date_max
),

charge_and_fee_data as (
  select 
    date(bt.created) as activity_at,
    bt.reporting_category,
    pmd.card_brand,
    pmd.card_funding,
    case when pmd.type = 'card' then 'cnp' else pmd.type end as type,
    sum(bt.amount) / 100.0 as amount
  from balance_transactions bt 
  left join payment_method_details pmd
    on bt.source_id = pmd.charge_id
  where bt.type in('charge', 'application_fee')
    and date(bt.created) >= (select date_min from date_selection)
    and date(bt.created) <= (select date_max from date_selection) 
  group by 1,2,3,4,5
),

refund_data as (
  select 
    date(bt.created) as activity_at,
    bt.reporting_category,
    re.charge_id,
    re.id,
    pmd.card_brand,
    pmd.card_funding,
    case when pmd.type = 'card' then 'cnp' else pmd.type end as type,
    sum(bt.amount) / 100.0 as amount
  from balance_transactions bt 
  left join refunds re
    on bt.source_id = re.id
  left join payment_method_details pmd
    on re.charge_id = pmd.charge_id
  where bt.type in('refund')
    and date(bt.created) >= (select date_min from date_selection)
    and date(bt.created) <= (select date_max from date_selection) 
    and date(re.created) >= (select date_min from date_selection)
    and date(re.created) <= (select date_max from date_selection) 
  group by 1,2,3,4,5,6,7  
)

select * from charge_and_fee_data
union
select 
  activity_at,
  reporting_category,
  card_brand,
  card_funding,
  type,
  sum(amount) as amount
from refund_data 
group by 1,2,3,4,5
order by 1,2,5,4,3
