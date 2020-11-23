with earliest_date as (
  select
    date '2020-11-01' as date_after 
),
financing_paydown_bts as (
  select
    account merchant,
    created date,
    round(amount / 100.00, 2) paydown_amount,
    description,
    currency
  from
    connected_account_balance_transactions
  where
    account = 'acct_1GGv67CmU1jXmt7F' --   change to merchant account of interest
    and type in ('financing_paydown')
    and created > (
      select
        date_after
      from
        earliest_date
    )
),
sales_bt as (
  select
    cabt.account merchant,
    cac.id payment_id,
    cabt.created,
    cabt.source_id,
    cabt.currency,
    round(af.amount / -100.00, 2) application_fee_amount,
    round(cabt.amount / 100.00, 2) sales_amount
  from
    connected_account_balance_transactions cabt
    left join connected_account_charges cac on cabt.id = cac.balance_transaction_id
    left join application_fees af on cac.application_fee_id = af.id
  where
    cabt.account = 'acct_1GGv67CmU1jXmt7F'
    and cabt.type in ('payment')
    and cabt.reporting_category = 'charge'
    and cabt.created > (
      select
        date_after
      from
        earliest_date
    )
)
select
  sales_bt.merchant,
  sales_bt.payment_id,
  sales_bt.created,
  sales_bt.sales_amount,
  sales_bt.application_fee_amount,
  financing_paydown_bts.paydown_amount,
  financing_paydown_bts.description paydown_description
from
  sales_bt
  left join financing_paydown_bts on financing_paydown_bts.description like concat('%', sales_bt.source_id, '%')
order by
  sales_bt.created DESC
