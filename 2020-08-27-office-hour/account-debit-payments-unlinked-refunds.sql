-- unreferenced refund account debit payments
with account_debit_payments as (
  select
    *
  from(
      VALUES
        'py_1HKut5FfuIutquDb2LBRMrsi',
        'py_1HKuA1FfuIutquDbLDYZFXM5',
        'py_1HKu7jFfuIutquDblxFE1Nkf'
    ) as t(payment_id)
)
select
  refunds.id refund_id,
  refunds.created refund_created,
  refunds.amount/100.0 refund_amount,
  refunds.balance_transaction_id refund_bt_id,
  refunds.status,
  connected_account_transfers.id merchant_account_debit_transfer_id,
  destination_payment_id account_debit_payment_id,
  connected_account_transfers.account merchant_account_id
from
  connected_account_transfers
  left join connected_account_transfers_metadata on connected_account_transfers.id = connected_account_transfers_metadata.transfer_id
  left join (
    select
      *
    from
      refunds_metadata
    where
      key = 'parent_transfer'
  ) refunds_metadata on connected_account_transfers.id = refunds_metadata.value
  left join (
    select
      *
    from
      refunds
    where
      charge_id is null
  ) refunds on refunds.id = refunds_metadata.refund_id
where
  connected_account_transfers.destination_id = 'acct_1Ez19vFfuIutquDb' -- US platform account ID
  and not connected_account_transfers.automatic
  and not connected_account_transfers.reversed
  and connected_account_transfers_metadata.key = 'transaction_type'
  and connected_account_transfers_metadata.value = 'unreferenced-refund'
  and destination_payment_id in (
    select
      *
    from
      account_debit_payments
  )

