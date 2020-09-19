select
  date_trunc('day', created),
  round(sum(amount)) as charge_total,
  round(sum(amount * 0.5 * 0.01)) as fee_total
from
  charges
where
  invoice_id is not null
group by
  1
