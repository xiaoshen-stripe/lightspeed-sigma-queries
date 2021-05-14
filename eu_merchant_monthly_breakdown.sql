-- Merchant Monthly Breakdown
-- This query breaks down the monthly volume and fees for each connected account
-- Change the months on the commented lines below to edit the date range for this query
-- date range is inclusive of start and end month
with date_range as (
  select
    date_trunc('month', month) as "month"
  from unnest(
    sequence(
      (date_parse('2021-01' -- change the YYYY-MM starting month here
         ,'%Y-%m')  + interval '01' day), 
      (date_parse('2021-05' -- change the YYYY-MM ending month here
         ,'%Y-%m') + interval '30' day), 
      interval '1' month)
  -- (we needed to give the dates a day, padded sometime in the middle of the month, the days will be truncated off the date later)
  ) as t(month)
),
rate_schedule as (
select 
   1.5 as radar_fee_per_charge_rate 
--    based on contract 
),
-- gets the icplus and stripe fees, aggregated by account and by month, and estimates the terminal and radar fees
monthly_card_icplus_fees as (
  select 
    destination_id as account,
    date_format(incurred_at, '%Y-%m') as "month",
    'card' as payment_method_type,
    sum(case when fee_category in ('network_cost') then billing_amount else 0 end) as network_costs,
    sum(case when fee_category in ('stripe_fee') then billing_amount else 0 end) as stripe_fees,
    -- count(distinct (case when card_present = true then charge_id end)) * 2.0 as stripe_terminal_fees,
    count(distinct (case when card_present = false then charge_id end)) * (select radar_fee_per_charge_rate from rate_schedule) as stripe_radar_fees
    -- card by default 
  from icplus_fees
  where 
    date_trunc('month', incurred_at) in (select month from date_range)
  group by 1, 2, 3
  order by 1, 2, 3
),





monthly_lpm_stripe_fees as (
select
  ch.destination_id as account,
  date_format(ch.created, '%Y-%m') as "month",
  ch.payment_method_type,
  0 as network_costs,
  sum(bt.fee) as stripe_fees,
  0 as stripe_radar_fees
from
  charges ch
  left join balance_transactions bt on ch.balance_transaction_id = bt.id
where
  payment_method_type not in ('card')
  and ch.created > date '2021-01-01'
group by
  1,
  2,
  3
),

monthly_icplus_fees as (
  select
    account,
    month,
    sum(network_costs) as network_costs,
    sum(stripe_fees) as stripe_fees,
    sum(stripe_radar_fees) as stripe_radar_fees
  from
    (
      select
        *
      from
        monthly_card_icplus_fees
      union all
      select
        *
      from
        monthly_lpm_stripe_fees
    )
  group by
    1,
    2
),
          
-- gets the platform earnings (application fees) per merchant per month
-- (we're querying the connected_account tables separately here, 
-- because the application_fees weren't available in the non-connected tables)
monthly_lspd_fees as (
  select
    charges.account as account,
    date_format(charges.created, '%Y-%m') as month,
    charges.currency,
    sum(coalesce(fees.amount, 0)) as sum_application_fees
  from
    connected_account_charges as charges
  left join connected_account_balance_transaction_fee_details as fees 
    on charges.balance_transaction_id = fees.balance_transaction_id
  where
    date_trunc('month', charges.created) in (select month from date_range)
    and charges.captured
  group by 1, 2, 3
  order by 1, 2, 3
),
        
-- gets the monthly volume per merchant and per month, broken down by method type 
monthly_gmv as (
  select
    -- pivot columns
    charges.destination_id as account,
    date_format(charges.created, '%Y-%m') as month,
    charges.currency,
    -- volume sums
    sum(charges.amount) as total_sum_amount,
    sum(case when charges.payment_method_type = 'card' then charges.amount else 0 end) as sum_amount_when_card,
    sum(case when charges.payment_method_type = 'bancontact' then charges.amount else 0 end) as sum_amount_when_bancontact,

    sum(case when charges.payment_method_type = 'ideal' then charges.amount else 0 end) as sum_amount_when_ideal,
    -- number of transactions
    count(charges.id) as total_count_charges,
    sum(case when charges.payment_method_type = 'card' then 1 else 0 end) as count_when_card,
    sum(case when charges.payment_method_type = 'bancontact' then 1 else 0 end) as count_when_bancontact,
    sum(case when charges.payment_method_type = 'ideal' then 1 else 0 end) as count_when_ideal
    
  from
    charges -- using charges instead of connected_account_charges because we need the `payment_method_type` column
  where
    date_trunc('month', created) in (select month from date_range)
    and charges.captured
  group by 1, 2, 3
  order by 1, 2, 3
)

-- join all the aggregations together on the account, month and currency
-- selecting all the columns for the final result
-- all the above subquery money units are in cents, so here we divide by 100.0
-- In the future, if there are currencies in lightspeed sigma data
-- that do not have cents, add a case/when/else to not divide by 100.0 for those currencies
select 
  -- pivot columns
  monthly_gmv.account,
  date_format(date_range.month, '%Y-%m') as month,
  monthly_gmv.currency as currency,
  -- metadata
  connected_accounts.business_name,
--   metadata.value as business_type, -- this is null for most of the rows... `connected_accounts_metadata` table is missing rows
  -- volume
  monthly_gmv.total_sum_amount / 100.0 as total_volume,
  monthly_gmv.sum_amount_when_card / 100.0 as card_volume,
  monthly_gmv.sum_amount_when_bancontact / 100.0 as bancontact_volume,
  monthly_gmv.sum_amount_when_ideal / 100.0 as ideal_volume,
  -- number of transactions
  monthly_gmv.total_count_charges as total_num_txns,
--   monthly_gmv.count_when_card_present as CP_num_txns,
  monthly_gmv.count_when_card as card_num_txns,
  monthly_gmv.count_when_bancontact as bancontact_num_txns,
  monthly_gmv.count_when_ideal as ideal_num_txns,

  -- platform earnings
  monthly_lspd_fees.sum_application_fees / 100.0 as lspd_fees,
  -- icplus/stripe fees
  round(coalesce(monthly_icplus_fees.network_costs, 0) / 100.0, 3) as icplus_fees, -- round to the nearest cent  
  round(coalesce(monthly_icplus_fees.stripe_fees, 0) / 100.0, 3) as stripe_fees, -- round to the nearest cent
--   coalesce(monthly_icplus_fees.stripe_terminal_fees, 0) / 100.0 as terminal_fees,
  coalesce(monthly_icplus_fees.stripe_radar_fees, 0) / 100.0 as radar_fees
from monthly_gmv 
inner join date_range on monthly_gmv.month = date_format(date_range.month, '%Y-%m')
join monthly_icplus_fees 
  on monthly_gmv.account = monthly_icplus_fees.account 
    and monthly_icplus_fees.month = monthly_gmv.month
left join connected_accounts on monthly_gmv.account = connected_accounts.id
-- left join connected_accounts_metadata metadata on monthly_gmv.account = metadata.account_id
left join monthly_lspd_fees 
  on monthly_gmv.account = monthly_lspd_fees.account 
    and monthly_gmv.month = monthly_lspd_fees.month 
    and monthly_lspd_fees.currency = monthly_gmv.currency
order by 1, 2