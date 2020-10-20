SELECT
  m.business_name AS merchant_name,
  c.destination_id AS account_id,
  d.id AS case_id,
  date_format(
    c.created AT TIME ZONE 'AMERICA/NEW_YORK',
    '%Y-%m-%d'
  ) AS trx_date,
  date_format(
    d.created AT TIME ZONE 'AMERICA/NEW_YORK',
    '%Y-%m-%d'
  ) AS report_date,
  date_format(
    bt.created AT TIME ZONE 'AMERICA/NEW_YORK',
    '%Y-%m-%d'
  ) AS funds_withdrawn_date,
  d.evidence_details_due_by AS respond_by,
  d.is_charge_refundable,
  CAST(d.amount AS DECIMAL(18, 2)) / 100 AS amount,
  CASE
    WHEN c.payment_method_type = 'card' THEN 'CNP'
    ELSE 'CP'
  END AS cp_or_cnp,
  CASE
    WHEN d.status = 'warning_needs_response' THEN 'inquiry'
    WHEN d.status = 'needs_response' THEN 'chargeback'
  END AS TYPE,
  CASE
    d.reason
    WHEN 'fraudulent' THEN 'Fraud'
    WHEN 'duplicate' THEN 'Duplicate processing'
    WHEN 'credit_not_processed' THEN 'Credit not processed'
    WHEN 'product_not_received' THEN 'Merchandise/Services not Received'
    WHEN 'product_unacceptable' THEN 'Not As Described/Defective'
    WHEN 'subscription_canceled' THEN 'Cancelled Merchandise/Services'
    ELSE d.reason
  END AS reason_code,
  n.card_last4 AS last_4digits_card,
  a.value AS order_id
FROM
  disputes d
  LEFT JOIN balance_transactions bt ON d.id = bt.source_id
  LEFT JOIN charges c ON d.id = c.dispute_id
  LEFT JOIN connected_accounts m ON c.destination_id = m.id
  LEFT JOIN payment_method_details n ON c.id = n.charge_id
  LEFT JOIN (
    SELECT
      *
    FROM
      charges_metadata
    WHERE
      KEY = 'order_id'
  ) a ON c.id = a.charge_id
WHERE
  d.status IN (
    'warning_needs_response',
    'needs_response'
  )
  AND d.id = 'du_1HFcpmFfuIutquDbj7e21YAA' 
--   AND d.created >= DATE_ADD('day', -30, DATE(current_date))
ORDER BY
  d.created DESC
