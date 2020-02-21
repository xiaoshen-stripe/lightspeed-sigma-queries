-- Line items query: the first few selects build a table of "line items", which are the base unit of the accrual
-- reports and make the subsequent queries easier to understand.

-- QUERY PARTS:
-- 1. Build an activity_excluding_icplus table
--    - Each row is a BT
--    - Excludes all BTs for IC+ fees (handled separately)
--    - Includes the relevant columns of information
-- 2. Decompose the activity_excluding_icplus_fees table into tables representing gross and fee components
--    - Any BTs from above with a gross component get included in a gross_line_items table
--    - Any BTs from above with a fee component get include in a fee_line_items_excluding_icplus table
-- 3. Build fee_line_items_from_icplus table from IC+ data
--    - Each row is a separate fee
-- 4. Combine line items components to create line_items table
--    - Stitches together the line items components created in parts 2 and 3
--    - Adds in auto-payouts data
--    - Ready to be queried for the report-specific query!

-- QUERY PART 1
-- Part 1a: Gather all non-fee activity
-- Part 1b: Gather all non-IC+ fee activity
-- (Part 1a and 1b are done separately for query optimization. In order to reduce the join size, we filter by created date which can be done for non-fees)
-- Part 1c: Union these two results together
-- Part 1d: Join to charges and coalesce to get activity_excluding_icplus_fees table

-- date selection is extracted out as input 
with date_selection as (
select
  -- (timestamp '2019-11-01 00:00:00') as date_min,
  -- (timestamp '2019-12-01 00:00:00') as date_max
  date_add('month', -1, (date_trunc('month', data_load_time))) as date_min,
  date_trunc('month', data_load_time) as date_max
),
-- ksr is interested in the breakdown of different european markets. So only eur related items are of interest
currency_selection as (
select
  'usd' as currency_selected
),
non_fee_activity_partial as (
  select
    bts.id as balance_transaction_id,
    bts.created as balance_transaction_created_at,
    bts.reporting_category as balance_transaction_reporting_category,
    bts.currency as currency,
    bts.amount as gross,
    bts.fee as fee,
    -- Store these amounts in refund-specific field; we'll join to charges and coalesce in part 1d
    refunds.currency as customer_facing_currency_from_refund,
    -1 * refunds.amount as customer_facing_amount_from_refund,
    refunds.id as refund_id,
    disputes.id as dispute_id,
    -- If we don't join on icplus_fees below (in the is_connect_template case) we need to cast this null so the schema checker can tell the column type
    cast(null as varchar) as fee_id,
    transfers.destination_id as destination_id_from_transfer,
    bts.automatic_transfer_id as automatic_payout_id,
    case
      when reporting_category = 'charge' then bts.source_id
      else coalesce(refunds.charge_id, disputes.charge_id)
    end as charge_id,
    case
      when bts.reporting_category in ('charge', 'partial_capture_reversal') then 'charge_captured'
      else bts.reporting_category
    end as event_type,
    -- fee activity time handled in a separate subquery
    null as activity_start_time,
    null as activity_end_time,
    bts.description as balance_transaction_description
  from balance_transactions as bts

  -- Note that for BTs with reporting_category={refund, charge_failure},
  -- source_id points to a refund object, so the following left join will match in both cases.
  left join refunds as refunds
    on bts.source_id = refunds.id
  -- Similarly, for BTs with reporting_category={dispute, dispute_reversal}, source_id
  -- points to a dispute object, so the following left join will match in both cases.
  left join disputes as disputes
    on bts.source_id = disputes.id
  -- And for BTs with reporting_category={transfer, transfer_reversal}, source_id
  -- points to a transfer object, so the following left join will match in both cases.
  left join transfers as transfers
    on (bts.source_id = transfers.id and transfers.type = 'stripe_account')
  where bts.reporting_category not in ('payout', 'connect_reserved_funds', 'fee', 'network_cost')
    and bts.created >= (select date_min from date_selection)
    and bts.created < (select date_max from date_selection)
    and bts.currency = (select currency_selected from currency_selection)
),
-- Part 1b: Gather all non IC+ fees along with their attribution dates
fee_activity_excluding_icplus_partial as (
  select
    bts.id as balance_transaction_id,
    bts.created as balance_transaction_created_at,
    bts.reporting_category as balance_transaction_reporting_category,
    bts.currency as currency,
    bts.amount as gross,
    bts.fee as fee,
    -- leave non fee fields empty
    null as customer_facing_currency_from_refund,
    null as customer_facing_amount_from_refund,
    null as refund_id,
    null as dispute_id,
    -- If we don't join on icplus_fees below (in the is_connect_template case) we need to cast this null so the schema checker can tell the column type
    cast(null as varchar) as fee_id,
    null as destination_id_from_transfer,
    bts.automatic_transfer_id as automatic_payout_id,
    null as charge_id,
    bts.reporting_category as event_type,
    case
      when bts.reporting_category = 'fee'
        and bts.created >= timestamp '2018-04-01'
        -- Note that this regexp (used here and in the next case statement)
        -- should match the regexp used PostBilledDescriptionCheckSuite
        and regexp_like(bts.description, '^[\w\(\)\+ ]+ \(\d{4}-\d{2}-\d{2}( - \d{4}-\d{2}-\d{2})?\): .+$')
        then cast(regexp_extract(bts.description, '\((\d{4}-\d{2}-\d{2})', 1) as timestamp)
      else null
    end as activity_start_time,
    case
      when bts.reporting_category = 'fee'
        and bts.created >= timestamp '2018-04-01'
        and regexp_like(bts.description, '^[\w\(\)\+ ]+ \(\d{4}-\d{2}-\d{2}( - \d{4}-\d{2}-\d{2})?\): .+$')
        then cast(regexp_extract(bts.description, '(\d{4}-\d{2}-\d{2})\)', 1) as timestamp) + interval '1' day
      else null
    end as activity_end_time,
    bts.description as balance_transaction_description
  from balance_transactions as bts
  where bts.reporting_category = 'fee'
    and bts.currency = (select currency_selected from currency_selection)
    -- Removes BTs for IC+ fees, since those are included by a later query on the icplus_fees table
    and not regexp_like(bts.description, '^(Card payments|IC\+|Interchange plus)')
),
-- Part 1c: Partial creation of activity_excluding_icplus_fees (completed in next query)
activity_excluding_icplus_partial as (
  select * from non_fee_activity_partial
  UNION ALL
  select * from fee_activity_excluding_icplus_partial
),
-- Part 1d: Complete the activity_excluding_icplus_fees table by joining on charges and adding in a few more fields
-- as well as determining the proper activity_at, activity_interval_type, etc.
activity_excluding_icplus as (
  select
    activity.balance_transaction_id,
    balance_transaction_created_at,
    balance_transaction_reporting_category,
    coalesce(activity_end_time - interval '1' second, balance_transaction_created_at) as activity_at,
    activity.currency,
    gross,
    fee,
    automatic_payout_id,
    activity.charge_id,
    refund_id,
    activity.dispute_id,
    fee_id,
    coalesce(charges.destination_id, destination_id_from_transfer) as destination_id,
    charges.customer_id,
    customers.email as customer_email,
    customers.description as customer_description,
    -- Regarding the following two case statements: we need to pull the customer_facing_{amount, currency} from
    -- the appropriate joined object. In particular, we need to be careful because for refunds and disputes we
    -- have joined in both the refund/dispute _and_ the upstream charge. So we'll make our logic precise by switching
    -- on the reporting category (rather than just using a coalesce, which we had previously and led to bugs):
    -- * For refunds and charge failures, use values from the joined in refund object
    -- * For charges, use value from the joined-in charge object
    -- * Sadly for disputes there is no API object with the customer-facing amounts/currencies
    case
      when balance_transaction_reporting_category in ('refund', 'charge_failure') then customer_facing_currency_from_refund
      when balance_transaction_reporting_category = 'charge' then charges.currency
      else null
    end as customer_facing_currency,
    case
      when balance_transaction_reporting_category in ('refund', 'charge_failure') then customer_facing_amount_from_refund
      when balance_transaction_reporting_category = 'charge' then charges.amount
      else null
    end as customer_facing_amount,
    event_type,
    charges.invoice_id,
    invoices.subscription_id,
    charges.card_brand,
    charges.card_funding,
    charges.card_country,
    case
      when activity_end_time is not null then 'range'
      else 'instant'
    end as activity_interval_type,
    activity_start_time,
    activity_end_time,
    balance_transaction_description
  from activity_excluding_icplus_partial as activity
  left join charges
    on activity.charge_id = charges.id
    left join customers
    on customers.id = charges.customer_id
    left join invoices
    on invoices.id = charges.invoice_id
  where coalesce(activity_end_time - interval '1' second, balance_transaction_created_at) >= (select date_min from date_selection)
    and coalesce(activity_end_time - interval '1' second, balance_transaction_created_at) < (select date_max from date_selection)
),

-- QUERY PART 2
-- The next two subqueries create separate rows for gross and fee portions of the non-IC+ activity
gross_line_items as (
  select
    balance_transaction_id,
    balance_transaction_created_at,
    balance_transaction_reporting_category,
    case
      when balance_transaction_reporting_category in ('fee', 'network_cost') then 'other_fee'
      else 'gross'
    end as balance_transaction_component,
    activity_at,
    currency,
    gross as amount,
    charge_id,
    refund_id,
    dispute_id,
    invoice_id,
    subscription_id,
    fee_id,
    destination_id,
    customer_id,
    customer_email,
    customer_description,
    automatic_payout_id,
    customer_facing_currency,
    customer_facing_amount,
    event_type,
    card_brand,
    card_funding,
    card_country,
    activity_interval_type,
    activity_start_time,
    activity_end_time,
    balance_transaction_description
  from activity_excluding_icplus
  where gross != 0
),
fee_line_items_excluding_icplus as (
  select
    balance_transaction_id,
    balance_transaction_created_at,
    balance_transaction_reporting_category,
    case
      when balance_transaction_reporting_category in ('fee', 'network_cost') then 'tax'
      when balance_transaction_reporting_category in ('dispute', 'dispute_reversal') then 'dispute_fee'
      when balance_transaction_reporting_category in ('transfer', 'transfer_reversal') then balance_transaction_reporting_category || '_fee'
      when balance_transaction_reporting_category = 'other_adjustment' then 'other_fee'
      else 'payments_fee'
    end as balance_transaction_component,
    activity_at,
    currency,
    -1 * fee as amount,
    charge_id,
    refund_id,
    dispute_id,
    invoice_id,
    subscription_id,
    fee_id,
    destination_id,
    customer_id,
    customer_email,
    customer_description,
    automatic_payout_id,
    null as customer_facing_currency,
    null as customer_facing_amount,
    event_type,
    card_brand,
    card_funding,
    card_country,
    activity_interval_type,
    activity_start_time,
    activity_end_time,
    balance_transaction_description
  from activity_excluding_icplus
  where fee != 0
),

-- QUERY PART 3
-- Pull line items from itemized IC+ table based on accrual date
  icplus_bts as (
    select
      id,
      created,
      reporting_category,
      automatic_transfer_id,
      description
    from balance_transactions bts
    where bts.reporting_category in ('fee', 'network_cost')
      and regexp_like(bts.description, '^(Card payments|IC\+|Interchange plus)')
  ),
  fee_line_items_from_icplus as (
    select
      icplus_fees.balance_transaction_id,
      bts.created as balance_transaction_created_at,
      bts.reporting_category as balance_transaction_reporting_category,
      icplus_fees.fee_name as balance_transaction_component,
      coalesce(icplus_fees.incurred_at, icplus_fees.attribution_end_time - interval '1' second) as activity_at,
      icplus_fees.billing_currency as currency,
      -1 * icplus_fees.subtotal_amount * coalesce(icplus_fees.fx_rate, 1) as amount,
      icplus_fees.charge_id,
      icplus_fees.refund_id,
      icplus_fees.dispute_id,
      canonical_charge.invoice_id,
      canonical_invoice.subscription_id,
      icplus_fees.id as fee_id,
      icplus_fees.destination_id,
      icplus_fees.customer_id,
      canonical_customer.email as customer_email,
      canonical_customer.description as customer_description,
      bts.automatic_transfer_id as automatic_payout_id,
      null as customer_facing_currency,
      null as customer_facing_amount,
      coalesce(icplus_fees.event_type, 'aggregate card scheme') as event_type,
      canonical_charge.card_brand,
      canonical_charge.card_funding,
      canonical_charge.card_country,
      case
        when icplus_fees.attribution_end_time is not null then 'range'
        else 'instant'
      end as activity_interval_type,
      icplus_fees.attribution_start_time as activity_start_time,
      icplus_fees.attribution_end_time as activity_end_time,
      bts.description as balance_transaction_description
    from icplus_fees
    left join icplus_bts as bts
      on icplus_fees.balance_transaction_id = bts.id
      left join charges as canonical_charge
      on canonical_charge.id = icplus_fees.charge_id
      left join customers as canonical_customer
      on canonical_customer.id = canonical_charge.customer_id
      left join invoices as canonical_invoice
      on canonical_invoice.id = canonical_charge.invoice_id
    where coalesce(icplus_fees.incurred_at, icplus_fees.attribution_end_time - interval '1' second) >= (select date_min from date_selection)
      and coalesce(icplus_fees.incurred_at, icplus_fees.attribution_end_time - interval '1' second) < (select date_max from date_selection)
      and icplus_fees.subtotal_amount != 0
      and icplus_fees.billing_currency = (select currency_selected from currency_selection)

  ),
  tax_line_items_from_icplus as (
    select
      icplus_fees.balance_transaction_id,
      bts.created as balance_transaction_created_at,
      bts.reporting_category as balance_transaction_reporting_category,
      'tax' as balance_transaction_component,
      coalesce(icplus_fees.incurred_at, icplus_fees.attribution_end_time - interval '1' second) as activity_at,
      icplus_fees.billing_currency as currency,
      -1 * icplus_fees.tax_amount * coalesce(icplus_fees.fx_rate, 1) as amount,
      icplus_fees.charge_id,
      icplus_fees.refund_id,
      icplus_fees.dispute_id,
      canonical_charge.invoice_id,
      canonical_invoice.subscription_id,
      icplus_fees.id as fee_id,
      icplus_fees.destination_id,
      icplus_fees.customer_id,
      canonical_customer.email as customer_email,
      canonical_customer.description as customer_description,
      bts.automatic_transfer_id as automatic_payout_id,
      null as customer_facing_currency,
      null as customer_facing_amount,
      coalesce(icplus_fees.event_type, 'aggregate card scheme') as event_type,
      canonical_charge.card_brand,
      canonical_charge.card_funding,
      canonical_charge.card_country,
      case
        when icplus_fees.attribution_end_time is not null then 'range'
        else 'instant'
      end as activity_interval_type,
      icplus_fees.attribution_start_time as activity_start_time,
      icplus_fees.attribution_end_time as activity_end_time,
      bts.description as balance_transaction_description
    from icplus_fees
    left join icplus_bts as bts
      on icplus_fees.balance_transaction_id = bts.id
      left join charges as canonical_charge
      on canonical_charge.id = icplus_fees.charge_id
      left join customers as canonical_customer
      on canonical_customer.id = canonical_charge.customer_id
      left join invoices as canonical_invoice
      on canonical_invoice.id = canonical_charge.invoice_id
    where coalesce(icplus_fees.incurred_at, icplus_fees.attribution_end_time - interval '1' second) >= (select date_min from date_selection)
      and coalesce(icplus_fees.incurred_at, icplus_fees.attribution_end_time - interval '1' second) < (select date_max from date_selection)
      and icplus_fees.tax_amount != 0
      and icplus_fees.billing_currency = (select currency_selected from currency_selection)
  ),

-- QUERY PART 4
-- Combine line items components then add auto_payouts data
line_items_partial as (
  select * from gross_line_items
  union
  select * from fee_line_items_excluding_icplus
    union
    select * from fee_line_items_from_icplus
    union
    select * from tax_line_items_from_icplus
),
automatic_payouts as (
  select
    payouts.id,
    bts.available_on as effective_at
  from balance_transactions as bts
  join transfers as payouts
    on bts.source_id = payouts.id
  where bts.reporting_category = 'payout'
    and payouts.automatic
),
line_items as (
  select
    balance_transaction_id,
    balance_transaction_created_at,
    balance_transaction_reporting_category,
    balance_transaction_component,
    activity_at,
    currency,
    amount,
    charge_id,
    refund_id,
    dispute_id,
    invoice_id,
    subscription_id,
    fee_id,
    destination_id,
    customer_id,
    customer_email,
    customer_description,
    automatic_payout_id,
    automatic_payouts.effective_at as automatic_payout_effective_at,
    event_type,
    card_brand,
    card_funding,
    card_country,
    customer_facing_currency,
    customer_facing_amount,
    activity_interval_type,
    date_trunc('day', coalesce(activity_start_time, activity_at)) as activity_start_date,
    date_trunc('day', coalesce(activity_end_time - interval '1' second, activity_at)) as activity_end_date,
    balance_transaction_description
  from line_items_partial
  left join automatic_payouts
    on automatic_payouts.id = line_items_partial.automatic_payout_id
),
charge_application_fee_bts as (
    select
       balance_transaction_id
    from line_items 
    where 
       balance_transaction_component = 'payments_fee'
       and balance_transaction_reporting_category = 'charge' 
       -- only focus on the charges
)

-- Query the resulting line_items table to format them for human CSV consumption in the activity itemized report
select
  line_items.balance_transaction_id,
  date_format(balance_transaction_created_at, '%Y-%m-%d %T') as balance_transaction_created_at,
  balance_transaction_reporting_category,
  balance_transaction_component,
  date_format(activity_at, '%Y-%m-%d %T') as activity_at,
  currency,
  decimalize_amount(currency, amount, 6) as amount,
  charge_id,
  refund_id,
  dispute_id,
  invoice_id,
  subscription_id,
  fee_id,
  destination_id,
  customer_id,
  customer_email,
  customer_description,
  automatic_payout_id,
  date_format(automatic_payout_effective_at, '%Y-%m-%d %T') as automatic_payout_effective_at,
  event_type,
  card_brand,
  card_funding,
  card_country,
  customer_facing_currency,
  decimalize_amount(customer_facing_currency, customer_facing_amount) as customer_facing_amount,
  activity_interval_type,
  date_format(activity_start_date, '%Y-%m-%d') as activity_start_date,
  date_format(activity_end_date, '%Y-%m-%d') as activity_end_date,
  balance_transaction_description, 
  case
     when charge_id not like 'py_%' then 'charges'
     when charge_id like 'py_%' and line_items.balance_transaction_id in (select * from charge_application_fee_bts) then 'chargeback_fees'
     else 'unlinked_refunds'
  end as charge_type
from line_items
where line_items.balance_transaction_id is not null
and balance_transaction_reporting_category = 'charge' 
-- only focus on the charges
order by
  balance_transaction_created_at,
  balance_transaction_id,
  case
    when balance_transaction_component = 'gross' then 1
    else 2
  end,
  balance_transaction_component