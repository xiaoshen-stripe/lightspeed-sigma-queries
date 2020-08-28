with subscription_charges as (
  select
    subscriptions.id subscription_id,
    charges.*
  from
    subscriptions
    left join invoices on subscriptions.id = invoices.subscription_id
    left join charges on invoices.charge_id = charges.id
),
charges_metadata_filtered as (
  select
    *
  from
    charges_metadata
  where
    key = 'product_id'
    and value in('resto', 'retail')
)
select
  date_format(date_trunc('day', i.incurred_at), '%Y-%m-%d') date,
  round(billing_amount / -100.00, 10) as billed,
  subscription_id,
  coalesce(
    if(subscription_id is not null, 'subscription'),
    c.value
  ) business_type,
  m.description charge_description,
  coalesce(
    i.charge_id,
    m.id,
    i.refund_id,
    i.dispute_id,
    i.request_id
  ) activity_id,
  i.fee_name,
  i.fee_category
from
  icplus_fees i
  left join charges m on m.id = i.charge_id
  left join charges_metadata_filtered c on i.charge_id = c.charge_id
  left join subscription_charges sc on sc.id = i.charge_id
where
  i.incurred_at > date('2020-07-01')
--   and subscription_id is not null

