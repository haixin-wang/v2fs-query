WITH double_entry_book AS (
    SELECT addresses AS address, btc_inputs.type, -btc_inputs.value AS value
    FROM btc_inputs
    INNER JOIN btc_transactions ON btc_transactions.hash = btc_inputs.transaction_hash
    UNION ALL
    SELECT addresses AS address, btc_outputs.type, btc_outputs.value AS value
    FROM btc_outputs
    INNER JOIN btc_transactions ON btc_transactions.hash = btc_outputs.transaction_hash
)
SELECT address, type, sum(value) AS balance
FROM double_entry_book
GROUP BY address, type
ORDER BY balance DESC
LIMIT 1000;
