select
  refunds.id refund_id,
  refunds.created,
  refunds.amount refund_amount,
  refunds.balance_transaction_id refund_bt,
  connected_account_transfers.id transfer_id,
  connected_account_transfers.destination_payment_id payment_id,
  connected_account_transfers.account merchant_account,
  payment_methods.id payment_method_id,
  payment_methods.card_brand,
  payment_methods.card_country,
  payment_methods.card_exp_month,
  payment_methods.card_exp_year,
  payment_methods.card_fingerprint,
  payment_methods.card_funding,
  payment_methods.card_iin card_bin_number,
  payment_methods.card_last4,
  payment_methods.card_three_d_secure_supported
from
  (
    select
      *
    from
      refunds
    where
      charge_id is null
      and status = 'succeeded'
  ) refunds
  left join (
    select
      *
    from
      refunds_metadata
    where
      key in ('parent_transfer')
  ) refunds_metadata on refunds.id = refunds_metadata.refund_id
  left join connected_account_transfers on refunds_metadata.value = connected_account_transfers.id
  left join (
    select
      *
    from
      connected_account_transfers_metadata
    where
      key in ('final_destination')
  ) connected_account_transfers_metadata on connected_account_transfers.id = connected_account_transfers_metadata.transfer_id
  left join payment_methods on payment_methods.id = connected_account_transfers_metadata.value
where
  connected_account_transfers.destination_id = 'acct_1Ez19vFfuIutquDb' -- US platform account ID
  and not connected_account_transfers.automatic
  and not connected_account_transfers.reversed
  and refunds.created > date '2020-09-01'
