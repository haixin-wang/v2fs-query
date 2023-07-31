WITH double_entry_book AS (
    SELECT to_address AS address, value AS value, eth_blocks.timestamp AS block_timestamp
    FROM eth_traces
    INNER JOIN eth_blocks ON eth_blocks.number = eth_traces.block_number
    WHERE to_address IS NOT NULL
    AND status = 1
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    UNION ALL 
    SELECT from_address AS address, -value AS value, eth_blocks.timestamp AS block_timestamp
    FROM eth_traces
    INNER JOIN eth_blocks ON eth_blocks.number = eth_traces.block_number
    WHERE from_address IS NOT NULL
    AND status = 1
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    UNION ALL
    SELECT
        miner AS address,
        SUM(CAST(eth_receipts.gas_used AS real) * CAST((effective_gas_price - COALESCE(base_fee_per_gas, 0)) AS real)) AS value,
        block_timestamp
    FROM eth_transactions
    JOIN eth_blocks ON eth_blocks.number = eth_transactions.block_number
    JOIN eth_receipts ON eth_transactions.hash = eth_receipts.transaction_hash
    GROUP BY eth_blocks.miner, block_timestamp
    UNION ALL
    SELECT
        from_address AS address,
        -(CAST(eth_receipts.gas_used AS real) * CAST(effective_gas_price AS real)) AS value,
        block_timestamp
    FROM eth_transactions
    JOIN eth_receipts ON eth_transactions.hash = eth_receipts.transaction_hash
),
double_entry_book_grouped_by_date AS (
    SELECT address, SUM(value) AS balance_increment, DATE(block_timestamp, 'unixepoch') AS date
    FROM double_entry_book
    GROUP BY address, date
),
daily_balances_with_gaps AS (
    SELECT address, date, SUM(balance_increment) OVER (PARTITION BY address ORDER BY date) AS balance,
    LEAD(date, 1, '2023-05-18') over (PARTITION BY address ORDER BY date) AS next_date
    FROM double_entry_book_grouped_by_date
),
calendar(date) AS (
    VALUES('2023-05-11')
    UNION ALL
    SELECT date(date, '+1 day')
    FROM calendar
    WHERE date < '2023-05-18'
),
daily_balances AS (
    SELECT address, calendar.date, balance
    FROM daily_balances_with_gaps
    JOIN calendar ON daily_balances_with_gaps.date <= calendar.date AND calendar.date < daily_balances_with_gaps.next_date
)
SELECT date, COUNT(*) AS address_count
FROM daily_balances
WHERE balance > 0
GROUP BY date;