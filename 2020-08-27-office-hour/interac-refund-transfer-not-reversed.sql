select
  refunds.id refunds_id,
  refunds.amount refunds_amount,
  refunds.created refund_created,
  refunds.status refund_status,
  charges.destination_id merchant_account,
  connected_accounts.business_name,
  connected_accounts.business_url,
  transfers.id transfer_id
from
  refunds
  left join charges on refunds.charge_id = charges.id
  left join transfers on charges.transfer_id = transfers.id
  left join transfer_reversals on transfers.id = transfer_reversals.transfer_id
  left join connected_accounts on charges.destination_id = connected_accounts.id
where
  charges.payment_method_type = 'interac_present'
  and charges.captured
  and refunds.status = 'succeeded'
  and transfer_reversals.id is NULL
  and refunds.created > date '2020-01-01'
order by
  merchant_account,
  business_name,
  refund_created DESC

