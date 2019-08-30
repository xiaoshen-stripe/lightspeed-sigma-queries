-- calendar_days is a temporary table with a row and value for every day for the past 1 month
with calendar_days as (
  select
    date(day) as day
  from unnest(
    sequence(date('2019-08-01'), date('2019-08-26'), interval '1' day)
  ) as t(day)
),
daily_transfer_failures as (
  select
    date(created) as day,
    account,
    sum(case when type = 'transfer_failure' and amount > 0 then amount else 0 end) as transfer_credit_failures,
    sum(case when type = 'transfer_failure' and amount < 0 then amount else 0 end) as transfer_debit_failures,
    sum(case when type = 'payout_failure' and amount > 0 then amount else 0 end) as payout_credit_failures,
    sum(case when type = 'payout_failure' and amount < 0 then amount else 0 end) as payout_debit_failures,
    sum(case when type = 'transfer_failure' and amount > 0 then 1 else 0 end) as transfer_credit_failure_count,
    sum(case when type = 'transfer_failure' and amount < 0 then 1 else 0 end) as transfer_debit_failure_count,
    sum(case when type = 'payout_failure' and amount > 0 then 1 else 0 end) as payout_credit_failure_count,
    sum(case when type = 'payout_failure' and amount < 0 then 1 else 0 end) as payout_debit_failure_count
  from connected_account_balance_transactions
  where date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)
  group by 1, 2 
  having (count_if(type = 'transfer_failure')>0 or count_if(type = 'payout_failure')>0)
  order by 1 desc, 2
)
--
-- Join temporary tables, and format output for reporting display
-- Note: if you have currencies that do not have cents (e.g. JPY), you should not divide by 100.0
select
  calendar_days.day,
  account,
  coalesce(transfer_credit_failures/100.0, 0) as transfer_credit_failures,
  coalesce(transfer_debit_failures/100.0, 0) as transfer_debit_failures,
  coalesce(payout_credit_failures/100.0, 0) as payout_credit_failures,
  coalesce(payout_debit_failures/100.0, 0) as payout_credit_failures,
  coalesce(transfer_credit_failure_count, 0) as transfer_credit_failure_count,
  coalesce(transfer_debit_failure_count, 0) as transfer_credit_failure_count,
  coalesce(payout_credit_failure_count, 0) as payout_credit_failure_count,
  coalesce(payout_debit_failure_count, 0) as payout_debit_failure_count
from daily_transfer_failures
right join calendar_days
  on calendar_days.day = daily_transfer_failures.day
where
  calendar_days.day >= (select min(daily_transfer_failures.day) from daily_transfer_failures)  -- since the first transaction
order by 1 desc, 2
