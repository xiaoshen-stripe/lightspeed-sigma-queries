
-- To edit the dates for this query, change the dates in date_min and date_max 
with date_selection as (
    select 
      date('2019-11-01') as date_min,
      date('2019-11-30') as date_max
), 

icplus_data as (
  select
    date(incurred_at) as activity_at,
    date(attribution_start_time) as attribution_start_time,
    date(attribution_end_time) - interval '1' day as attribution_end_time,
    fee_category,
    card_brand,
    card_funding,
    case when card_present then 'Card Present' else 'CNP' end as type,
    sum(billing_amount) / 100.0 as amount 
  from icplus_fees
  where date(incurred_at) >= (select date_min from date_selection)
    and date(incurred_at) <= (select date_max from date_selection)
  group by 1,2,3,4,5,6,7
),

daily_other_fee_data as (
  select 
    date(substr(description, position('(' in description)+1, 10)) as activity_at,
    null as attribution_start_time,
    null as attribution_end_time,
    description,
    null as card_brand,
    null as card_funding,
    null as type,
    -amount / 100.0 as amount
  from balance_transactions
  where type = 'stripe_fee'
    and (lower(description) like '%radar%' or lower(description) like '%services fee%') 
    and date(substr(description, position('(' in description)+1, 10)) >= (select date_min from date_selection)
    and date(substr(description, position('(' in description)+1, 10)) <= (select date_max from date_selection)
  ),

monthly_other_fee_data as (
  select 
    date(substr(description, position('(' in description)+1, 10)) as activity_at,
    date(substr(description, position('(' in description)+1, 10)) as attribution_start_time,
    date(substr(description, position(')' in description)-10, 10)) as attribution_end_time,
    description,
    null as card_brand,
    null as card_funding,
    null as type,
    -amount / 100.0 as amount
  from balance_transactions
  where type = 'stripe_fee'
    and (lower(description) like '%sigma%' or lower(description) like '%active reader fee%' or lower(description) like '%connect%') 
    and date(substr(description, position('(' in description)+1, 10)) >= (select date_min from date_selection)
    and date(substr(description, position(')' in description)-10, 10)) <= (select date_max from date_selection)
)

select * from icplus_data
union 
select * from daily_other_fee_data
union 
select * from monthly_other_fee_data

order by 1,7,4,6,5