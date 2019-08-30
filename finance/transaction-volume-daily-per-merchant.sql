-- calendar_days is a temporary table with a row and value for every day for the past 1 month
with calendar_days as (
  select
    date(day) as day
  from unnest(
    sequence(date('2019-08-01'), date('2019-08-26'), interval '1' day)
  ) as t(day)
),
-- day_balance_transactions is a temporary table that aggregates and pivots different
-- balance_transaction types on a daily basis for each currency
day_balance_transactions as (
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
--
-- day_declines is a temporary table that aggregates
-- declines on a daily basis for each currency
day_declines as (
  select
    date(created) as day,
    destination_id as account,
    sum(amount) as declines,
    count(id) as decline_count
  from charges
  where outcome_type = 'issuer_declined'
  and date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)                      
  group by 1,2
),
-- --day_disputes is a temporary table that aggregates
-- the connect account payouts
day_disputes as(
  select
    date(balance_transactions.created) as day,
    balance_transactions.source_id,
    charge_id,
    destination_id as account,
    sum(case when type = 'adjustment' and lower(balance_transactions.description) like 'chargeback withdrawal%' then balance_transactions.amount else 0 end) as disputes,
    sum(case when type = 'adjustment' and lower(balance_transactions.description) like 'chargeback reversal%' then balance_transactions.amount else 0 end) as disputes_won,
    count(distinct case when type = 'adjustment' and lower(balance_transactions.description) like 'chargeback withdrawal%' then balance_transactions.source_id end) as dispute_count,
    count(distinct case when type = 'adjustment' and lower(balance_transactions.description) like 'chargeback reversal%' then balance_transactions.source_id end) as disputes_won_count
  from balance_transactions
  left join disputes on balance_transactions.source_id = disputes.id
  left join charges on disputes.charge_id = charges.id
  where type = 'adjustment'
  and date(balance_transactions.created) >= (select min(calendar_days.day) from calendar_days)
  and date(balance_transactions.created) <= (select max(calendar_days.day) from calendar_days)                      
  group by 1,2,3,4
),
--daily_connect_payouts is a temporary table that aggregates
-- the connect account payouts
day_connect_payouts as (
  select
    date(connected_account_transfers.date) as day,
    account,
    sum(amount) as connect_payouts,
    count(id) as connect_payout_count
  from connected_account_transfers
  where status = 'paid'
  and date(connected_account_transfers.date) >= (select min(calendar_days.day) from calendar_days)
  and date(connected_account_transfers.date) <= (select max(calendar_days.day) from calendar_days)                      
  group by 1,2
),
accounts as (
  select 
    account as account,
    day
  from day_balance_transactions
  union
  select account as account,
    day
  from day_connect_payouts
)
--
-- Join temporary tables, and format output for reporting display
-- Note: if you have currencies that do not have cents (e.g. JPY), you should not divide by 100.0
select
  calendar_days.day,
  accounts.account,
  coalesce(captured_authorizations/100.0, 0) as captured_authorizations,
  coalesce(refunds/100.0, 0) as refunds,
  coalesce(declines/100.0, 0) as declines,
  coalesce(disputes/100.0, 0) as disputes,
  coalesce(disputes_won/100.0, 0) as disputes_won,
  coalesce(connect_payouts/100.0, 0) as connect_payouts,
  coalesce(captured_authorizations_count, 0) as captured_authorizations_count,
  coalesce(refund_count, 0) as refund_count,
  coalesce(decline_count, 0) as decline_count,
  coalesce(dispute_count, 0) as dispute_count,
  coalesce(disputes_won_count, 0) as disputes_won_count,
  coalesce(connect_payout_count, 0) as connect_payout_count
from calendar_days
full outer join accounts 
  on calendar_days.day = accounts.day
full outer join day_balance_transactions
  on calendar_days.day = day_balance_transactions.day
full outer join day_disputes
  on calendar_days.day = day_disputes.day and accounts.account = day_disputes.account
full outer join day_declines
  on calendar_days.day = day_declines.day and accounts.account = day_declines.account
full outer join day_connect_payouts
  on calendar_days.day  = day_connect_payouts.day and accounts.account = day_connect_payouts.account
where
  calendar_days.day >= (select min(day_balance_transactions.day) from day_balance_transactions)  -- since the first transaction
order by 1 desc
