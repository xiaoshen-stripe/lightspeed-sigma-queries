-- calendar_days is a temporary table with a row and value for every day for the past 1 month
with calendar_days as (
  select
    date(day) as day
  from unnest(
    sequence(date('2019-08-01'), date('2019-08-26'), interval '1' day)
  ) as t(day)
),
--
-- daily_balance_transactions is a temporary table that aggregates and pivots different
-- balance_transaction types on a daily basis for each currency
daily_balance_transactions as (
  select
    date(created) as day,
    sum(case when type in ('payment', 'charge') then amount else 0 end) as captured_authorizations,
    sum(case when type in ('payment_refund', 'refund') then amount else 0 end) as refunds,
    sum(case when type in ('application_fee') then amount else 0 end) as application_fees,
    sum(case when type in ('application_fee_refund') then amount else 0 end) as application_fee_refunds,
    count_if(type in ('payment', 'charge')) as captured_authorizations_count,
    count_if(type in ('payment_refund', 'refund')) as refund_count,
    count_if(type in ('application_fee')) as application_fee_count,
    count_if(type in ('application_fee_refund')) as application_fee_refund_count
  from balance_transactions
  where type not like '%transfer%' and type <> 'payout'
  and date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)
  group by 1
)
--
-- Join temporary tables, and format output for reporting display
-- Note: if you have currencies that do not have cents (e.g. JPY), you should not divide by 100.0
select
  calendar_days.day,
  coalesce(captured_authorizations/100.0, 0) as captured_authorizations,
  coalesce(refunds/100.0, 0) as refunds,
  coalesce(application_fees/100.0, 0) as application_fees,
  coalesce(application_fee_refunds/100.0, 0) as application_fee_refunds,
  coalesce(captured_authorizations_count, 0) as captured_authorizations_count,
  coalesce(refund_count, 0) as refund_count,
  coalesce(application_fee_count, 0) as application_fee_count,
  coalesce(application_fee_refund_count, 0) as application_fee_refund_count
from daily_balance_transactions
right join calendar_days
  on calendar_days.day = daily_balance_transactions.day
where
  calendar_days.day >= (select min(daily_balance_transactions.day) from daily_balance_transactions)  -- since the first transaction
order by 1 desc 
