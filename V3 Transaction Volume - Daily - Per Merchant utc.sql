  
-- calendar_days is a temporary table with a row and value for every day between the timestamps
-- To edit the dates for this query, change the dates in sequence() 
-- utc time should be used before 10/6, since that's when global cutoff came into play 
with calendar_days as (
  select
    date(day) as day
  from unnest(
    sequence(date('2019-09-01'), date('2019-09-30'), interval '1' day)
  ) as t(day)
),

day_transfers as (
  select date(created) as activity_date,
  destination_id as account,
  sum(amount) as transfers 
  from transfers 
  where status = 'paid' 
    and source_type = 'card'
    and type = 'stripe_account'
    and date(created) >= (select min(calendar_days.day) from calendar_days)
    and date(created) <= (select max(calendar_days.day) from calendar_days)  
  group by 1, 2
),

-- day_balance_transactions is a temporary table that aggregates and pivots different
-- balance_transaction types on a daily basis for each currency
day_balance_transactions as (
  select
    case when type <> 'payout' then date(created) else date(available_on) end as activity_date,
    currency,
    account,
    sum(case when type in ('payment', 'charge') then amount else 0 end) as total_charges,
    sum(case when type in ('payment_refund', 'refund') then amount else 0 end) as refunds,
    sum(case when type in ('payment_refund', 'refund') and date_diff('day',date(created), date(available_on)) = 0 then amount else 0 end) as refunds_t0,
    sum(case when type in ('payment_refund', 'refund') and date_diff('day',date(created), date(available_on)) = 1 then amount else 0 end) as refunds_t1,
    sum(case when type in ('payment_refund', 'refund') and date_diff('day',date(created), date(available_on)) = 2 then amount else 0 end) as refunds_t2,
    sum(case when type in ('payment_refund', 'refund') and date_diff('day',date(created), date(available_on)) = 3 then amount else 0 end) as refunds_t3,
    sum(case when type in ('payment_refund', 'refund') and date_diff('day',date(created), date(available_on)) = 4 then amount else 0 end) as refunds_t4,
    sum(case when type in ('payment_refund', 'refund') and date_diff('day',date(created), date(available_on)) = 5 then amount else 0 end) as refunds_t5,
    sum(case when type in ('payment', 'charge') then -fee else 0 end) as application_fees,
    sum(case when type in ('adjustment') then amount else 0 end) as adjustments,
    sum(case when type in ('payout') then amount else 0 end) as connect_payouts
  from connected_account_balance_transactions
  where type not like '%transfer%' 
    and case when type <> 'payout'
      then date(created) >= (select min(calendar_days.day) from calendar_days) 
      else date(available_on) >= (select min(calendar_days.day) from calendar_days) end
    and date(created) <= (select max(calendar_days.day) from calendar_days)  
  group by 1, 2, 3
),

--day_fees selects all IC+ fees
day_fees as (
  select 
    date(incurred_at) as activity_date, 
    destination_id as account,
    sum(case when fee_category in ('network_cost') then billing_amount else 0 end) as network_costs,
    sum(case when fee_category in ('stripe_fee') then billing_amount else 0 end) as stripe_fees,
    count(distinct (case when card_present = true then charge_id end)) * 2.0 as stripe_terminal_fees,
    count(distinct (case when card_present = false then charge_id end)) * 1.5 as stripe_radar_fees
  from icplus_fees 
  where date(incurred_at) >= (select min(calendar_days.day) from calendar_days) 
    and date(incurred_at) <= (select max(calendar_days.day) from calendar_days) 
  group by 1, 2
),

day_payouts as (
   select 
     date(bt.created) as activity_date,
     bt.account,
     bt.automatic_transfer_id as payout_id,
     date(po.date) as batch_paid_date,
     po.amount as batched_connect_payout,
     po.status as batch_status
   from connected_account_balance_transactions bt 
   left join connected_account_transfers po
     on bt.automatic_transfer_id = po.id
   where bt.type in ('payment', 'charge')
   and date(bt.created) >= (select min(calendar_days.day) from calendar_days) 
   and date(bt.created) <= (select max(calendar_days.day) from calendar_days) 
   group by 1, 2, 3, 4, 5, 6
),

--connect_account_details is a temporary table that selects
--business information from connect accounts
connect_account_details as (
  select 
      id,
      coalesce(business_name, business_url) as business_name 
    from connected_accounts 
)
--
-- Join temporary tables, and format output for reporting display
-- Note: if you have currencies that do not have cents (e.g. JPY), you should not divide by 100.0
select
  calendar_days.day as activity_date,
  day_balance_transactions.account as destination_id,
  connect_account_details.business_name,
  coalesce(day_balance_transactions.currency, null) as currency,
  coalesce(transfers/100.0, 0) as transfers,
  coalesce(total_charges/100.0, 0) as total_charges,
  coalesce(refunds/100.0, 0) as refunds,
  coalesce(application_fees/100.0, 0) as application_fees,
  coalesce(network_costs/100.0, 0) as network_costs,
  coalesce(stripe_fees/100.0, 0) as stripe_fees,
  coalesce(stripe_terminal_fees/100.0, 0) as stripe_terminal_fees,
  coalesce(stripe_radar_fees/100.0, 0) as stripe_radar_fees,
  coalesce(adjustments/100.0, 0) as adjustments,
  coalesce(connect_payouts/100.0, 0) as connect_payouts_paid,
  coalesce(batched_connect_payout / 100.0, 0) as batched_connect_payout,
  batch_paid_date,
  batch_status,
  coalesce(refunds_t0/100.0, 0) as refunds_t0,
  coalesce(refunds_t1/100.0, 0) as refunds_t1,
  coalesce(refunds_t2/100.0, 0) as refunds_t2,
  coalesce(refunds_t3/100.0, 0) as refunds_t3,
  coalesce(refunds_t4/100.0, 0) as refunds_t4,
  coalesce(refunds_t5/100.0, 0) as refunds_t5
from calendar_days 
left join day_balance_transactions
  on calendar_days.day = day_balance_transactions.activity_date 
left join day_transfers 
  on calendar_days.day = day_transfers.activity_date and day_balance_transactions.account = day_transfers.account
left join day_fees 
  on calendar_days.day = day_fees.activity_date and day_balance_transactions.account = day_fees.account
left join day_payouts 
  on calendar_days.day = day_payouts.activity_date and day_balance_transactions.account = day_payouts.account  
left join connect_account_details 
  on day_balance_transactions.account = connect_account_details.id 
order by destination_id asc, day asc