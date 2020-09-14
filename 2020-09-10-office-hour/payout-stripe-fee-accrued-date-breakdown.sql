select
  date(transfers.date) as payout_date,
  transfers.id as payout_id,
  transfers.amount / 100.0 as payout_amount,
  transfers.status as payout_status,
  balance_transactions.currency,
  balance_transactions.type as balance_transaction_type,
  case
    when balance_transactions.reporting_category = 'fee'
    and regexp_like(
      balance_transactions.description,
      '^[\w\(\)\+ ]+ \(\d{4}-\d{2}-\d{2}( - \d{4}-\d{2}-\d{2})?\): .+$'
    ) then cast(
      regexp_extract(
        balance_transactions.description,
        '\((\d{4}-\d{2}-\d{2})',
        1
      ) as timestamp
    )
    else null
  end as fee_activity_start_time,
  sum(balance_transactions.amount / 100.0) as balance_transaction_amount,
  sum(balance_transactions.net / 100.0) as balance_transaction_net
from
  (
    select
      *
    from
      transfers
    where
      id = 'po_1HRH8YFfuIutquDbm5YbJazs' --     modify po to payout of interest
      and transfers.type = 'bank_account'
  ) transfers
  left join (
    select
      *
    from
      balance_transactions
    where
      type not in ('payout')
  ) balance_transactions on balance_transactions.automatic_transfer_id = transfers.id
  left join charges on charges.id = balance_transactions.source_id
  left join disputes on disputes.id = balance_transactions.source_id
  left join refunds on refunds.id = balance_transactions.source_id
group by
  1,
  2,
  3,
  4,
  5,
  6,
  7
