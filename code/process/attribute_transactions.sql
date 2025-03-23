INSERT INTO attributed_transactions
SELECT
    t.transaction_id,
    t.account_id,
    a.account_name,
    t.transaction_type,
    t.amount,
    t.currency,
    t.transaction_status,
    t.datetime_occured,
    LEAST(
        CASE WHEN t.amount > 1000 THEN 30 ELSE 0 END +
        CASE WHEN t.transaction_type = 'Withdrawal' THEN 20 ELSE 0 END +
        CASE WHEN t.transaction_status = 'Failed' THEN 25 ELSE 0 END,
        100
    ) AS risk_score
FROM transactions AS t
LEFT JOIN accounts AS a ON t.account_id = a.account_id;