select
  id bt_id,
  type bt_type,
  reporting_category,
  net amount,
  currency,
  created,
  available_on,
  source_id linked_model_jd,
  description,
  status
from
  balance_transactions bts
where
  automatic_transfer_id is null
  and date(bts.created at time zone 'America/New_York') >= date_add(
    'month',
    -1,
    (date_trunc('month', current_date))
  )
  and date(bts.created at time zone 'America/New_York') < date_add(
    'month',
    0,
    (date_trunc('month', current_date))
  )