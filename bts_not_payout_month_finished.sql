select
  id bt_id,
  type bt_type,
  reporting_category,
  net amount,
  currency,
  created,
  available_on,
  source_id linked_model_jd,
  description
from
  balance_transactions bts
where
  automatic_transfer_id is null
  and bts.created >= date_add(
    'month',
    -1,
    (date_trunc('month', current_date))
  )
  and bts.created < date_add(
    'month',
    0,
    (date_trunc('month', current_date))
  )
