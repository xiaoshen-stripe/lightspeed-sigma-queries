-- calendar_days is a temporary table with a row and value for every day between the timestamps
with calendar_days as (
  select
    date(day) as day
  from unnest(
    sequence(date('2019-10-01'), date('2019-10-31'), interval '1' day)
  ) as t(day)
),
-- daily_connect_accounts is a temporary table that counts the number of active connect accounts per day
daily_connect_accounts as (
  select 
    date(created) as day,
    count(distinct destination_id) as count_accounts
  from charges 
  where date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)  
  group by 1
),
-- daily_balance_transactions is a temporary table that aggregates and pivots different
-- balance_transaction types on a daily basis for each currency
daily_balance_transactions as (
  select
    date(created) as day,
    sum(case when type in ('payment', 'charge') then amount else 0 end) as captured_authorizations,
    sum(case when type in ('payment_refund', 'refund') then amount else 0 end) as refunds,
    sum(case when type = 'adjustment' and lower(description) like 'chargeback withdrawal%' then amount else 0 end) as disputes,
    sum(case when type = 'adjustment' and lower(description) like 'chargeback reversal%' then amount else 0 end) as disputes_won,
    sum(case when type = 'adjustment' and lower(description) like 'chargeback%' then fee else 0 end) as stripe_dispute_fees,
    sum(case when type in('application_fee', 'application_fee_refund') then amount else 0 end) as ls_payfac_fees,
    sum(case when type = 'network_cost' then -amount else 0 end) as interchange_cash,
    sum(case when type = 'stripe_fee' and lower(description) like 'card payments%' then -amount else 0 end) as stripe_icplus_fees_cash,
    sum(case when type = 'stripe_fee' and lower(description) not like 'card payments%' and lower(description) not like 'sigma%' and lower(description) not like '%active reader fee' and lower(description) not like 'connect%' then -amount else 0 end) as stripe_other_fees_daily_cash,
    sum(case when type = 'stripe_fee' and (lower(description) like 'sigma%' or lower(description) like '%active reader fee' or lower(description) like 'connect%') then -amount else 0 end) as stripe_other_fees_monthly_cash,
    count_if(type in ('payment', 'charge')) as captured_authorizations_count,
    count_if(type in ('payment_refund', 'refund')) as refund_count,
    count(distinct case when type = 'adjustment' and lower(description) like 'chargeback withdrawal%' then source_id end) as dispute_count,
    count(distinct case when type = 'adjustment' and lower(description) like 'chargeback reversal%' then source_id end) as disputes_won_count                   
  from balance_transactions
  where type not like '%transfer%' and type <> 'payout'
  and date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)
  group by 1
),
--
-- daily_declines is a temporary table that aggregates
-- declines on a daily basis for each currency
daily_declines as (
  select
    date(created) as day,
    sum(amount) as declines,
    count(id) as decline_count
  from charges
  where outcome_type = 'issuer_declined'
  and date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)
  group by 1
),
--daily_platform_payouts is a temporary table that aggregates
-- the platform payouts
daily_platform_payouts as (
  select
    date(transfers.date) as day,
    sum(amount) as platform_payouts,
    count(id) as platform_payout_count,
    status
  from transfers
  where status in('paid', 'in_transit', 'pending')
  and type='bank_account'
  and date(created) >= (select min(calendar_days.day) from calendar_days)
  and date(created) <= (select max(calendar_days.day) from calendar_days)
  group by 1, 4
),
--temporary table for incurred fees
daily_payments_incurred_fees as(
  select 
    date(incurred_at) as day,
    sum(case when fee_category = 'network_cost' then billing_amount else 0 end) as daily_interchange,
    sum(case when fee_category = 'stripe_fee' then billing_amount else 0 end) as daily_stripe_icplus_fees
  from icplus_fees 
  where date(incurred_at) >= (select min(calendar_days.day) from calendar_days)
  and date(incurred_at) <= (select max(calendar_days.day) from calendar_days)
  and attribution_start_time is null
  and attribution_end_time is null
  group by 1
),
--temporary table for attributed fees
daily_payments_attributed_fees as(
  select 
    date(day) as day,
    sum(case when date(day) between attribution_start_time and attribution_end_time then (billing_amount / date_diff('day', attribution_start_time, attribution_end_time)) else 0 end) as attr_interchange
  from calendar_days
  cross join icplus_fees 
  where date(attribution_end_time) >= (select min(calendar_days.day) from calendar_days)
    and date(attribution_start_time) <= (select max(calendar_days.day) from calendar_days)
  group by 1
),

--daily_connect_payouts is a temporary table that aggregates
-- the connect account payouts
daily_connect_payouts as (
  select
    date(connected_account_transfers.date) as day,
    sum(amount) as connect_payouts,
    count(id) as connect_payout_count
  from connected_account_transfers
  where status in('paid')
  and type in('bank_account','card')
  and date(connected_account_transfers.date) >= (select min(calendar_days.day) from calendar_days)
  and date(connected_account_transfers.date) < (select max(calendar_days.day) from calendar_days)
  group by 1
)

--
-- Join temporary tables, and format output for reporting display
-- Note: if you have currencies that do not have cents (e.g. JPY), you should not divide by 100.0
select
  calendar_days.day,
  coalesce(count_accounts, 0) as connect_accounts,
  coalesce(captured_authorizations/100.0, 0) as captured_authorizations,
  coalesce(refunds/100.0, 0) as refunds,
  coalesce(declines/100.0, 0) as declines,
  coalesce(disputes/100.0, 0) as disputes,
  coalesce(disputes_won/100.0, 0) as disputes_won,
  coalesce(stripe_dispute_fees/100.0, 0) as stripe_dispute_fees,
  coalesce(ls_payfac_fees/100.0, 0) as ls_payfac_fees,
  coalesce(platform_payouts/100.0, 0) as platform_payouts,
  daily_platform_payouts.status as platform_payout_status,
  coalesce(daily_interchange/100.0, 0) as interchange_rec,
  coalesce(attr_interchange/100.0, 0) as attr_interchange_rec,
  coalesce(daily_stripe_icplus_fees/100.0, 0) as stripe_icplus_fees_rec,
  coalesce(interchange_cash/100.0, 0) as interchange_cash,
  coalesce(stripe_icplus_fees_cash/100.0, 0) as stripe_icplus_fees_cash,
  coalesce(stripe_other_fees_daily_cash/100.0, 0) as stripe_other_fees_daily_cash,
  coalesce(stripe_other_fees_monthly_cash/100.0, 0) as stripe_other_fees_monthly_cash,
  coalesce(connect_payouts/100.0, 0) as connect_payouts,
  coalesce(captured_authorizations_count, 0) as captured_authorizations_count,
  coalesce(refund_count, 0) as refund_count,
  coalesce(decline_count, 0) as decline_count,
  coalesce(dispute_count, 0) as dispute_count,
  coalesce(disputes_won_count, 0) as disputes_won_count,
  coalesce(platform_payout_count, 0) as platform_payout_count,
  coalesce(connect_payout_count, 0) as connect_payout_count
from calendar_days 
left join daily_connect_accounts 
  on calendar_days.day = daily_connect_accounts.day
left join daily_balance_transactions
  on calendar_days.day = daily_balance_transactions.day
left join daily_declines
  on calendar_days.day = daily_declines.day 
left join daily_platform_payouts
  on calendar_days.day = daily_platform_payouts.day
left join daily_payments_incurred_fees 
  on calendar_days.day = daily_payments_incurred_fees.day
left join daily_payments_attributed_fees 
  on calendar_days.day = daily_payments_attributed_fees.day
left join daily_connect_payouts
  on calendar_days.day = daily_connect_payouts.day 
where
  calendar_days.day >= (select min(daily_balance_transactions.day) from daily_balance_transactions)  -- since the first transaction
order by 1 desc
