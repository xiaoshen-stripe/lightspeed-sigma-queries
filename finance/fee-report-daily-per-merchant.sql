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
    account,
    sum(case when type in ('payment', 'charge') then amount else 0 end) as captured_authorizations,
    sum(case when type in ('payment_refund', 'refund') then amount else 0 end) as refunds,
    count_if(type in ('payment', 'charge')) as captured_authorizations_count,
    count_if(type in ('payment_refund', 'refund')) as refund_count
  from connected_account_balance_transactions
  where type not like '%transfer%' and type <> 'payout'
  and date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)
  group by 1, 2
),
daily_application_fees as (
  select 
    date(created) as day,
    account_id as account,
    sum(amount) as application_fees,
    count(id) as application_fee_count
  from application_fees
  where date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)  
  group by 1,2
),
daily_application_fee_refunds as (
  select 
    date(r.created) as day,
    f.account_id as account,
    sum(r.amount) as application_fee_refunds,
    count(r.id) as application_fee_refund_count
  from application_fee_refunds r
  join application_fees f
    on f.id = r.fee_id
  where date(r.created) >= (select min(calendar_days.day) from calendar_days)
  and date(r.created) <= (select max(calendar_days.day) from calendar_days)  
  group by 1,2
),
accounts as (
  select 
    account as account,
    day
  from daily_balance_transactions
  union
  select 
    account as account,
    day
  from daily_application_fees
  union
  select 
    account as account,
    day
  from daily_application_fee_refunds
)
--
-- Join temporary tables, and format output for reporting display
-- Note: if you have currencies that do not have cents (e.g. JPY), you should not divide by 100.0
select
  calendar_days.day,
  accounts.account,
  coalesce(daily_balance_transactions.captured_authorizations/100.0, 0) as captured_authorizations,
  coalesce(daily_balance_transactions.refunds/100.0, 0) as refunds,
  coalesce(daily_application_fees.application_fees/100.0, 0) as application_fees,
  coalesce(daily_application_fee_refunds.application_fee_refunds/100.0, 0) as application_fee_refunds,
  coalesce(daily_balance_transactions.captured_authorizations_count, 0) as captured_authorizations_count,
  coalesce(daily_balance_transactions.refund_count, 0) as refund_count,
  coalesce(daily_application_fees.application_fee_count, 0) as application_fee_count,
  coalesce(daily_application_fee_refunds.application_fee_refund_count, 0) as application_fee_refund_count
from calendar_days
full outer join daily_balance_transactions
  on calendar_days.day = daily_balance_transactions.day
full outer join accounts 
  on calendar_days.day = accounts.day
full outer join daily_application_fees
  on accounts.account = daily_application_fees.account and calendar_days.day = daily_application_fees.day
full outer join daily_application_fee_refunds 
  on accounts.account = daily_application_fee_refunds.account and calendar_days.day = daily_application_fee_refunds.day
where
  calendar_days.day >= (select min(daily_balance_transactions.day) from daily_balance_transactions)  -- since the first transaction
order by 1 desc, 2
