WITH double_entry_book AS (
    SELECT to_address AS address, value AS value
    FROM eth_traces
    WHERE to_address IS NOT NULL
    AND status = 1
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    UNION ALL
    SELECT from_address AS address, -value AS value
    FROM eth_traces
    WHERE from_address IS NOT NULL
    AND status = 1
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    UNION ALL
    SELECT
        miner AS address,
        SUM(CAST(eth_receipts.gas_used AS real) * CAST((effective_gas_price - COALESCE(base_fee_per_gas, 0)) AS real)) AS value
    FROM eth_transactions
    JOIN eth_blocks ON eth_blocks.number = eth_transactions.block_number
    JOIN eth_receipts ON eth_transactions.hash = eth_receipts.transaction_hash
    GROUP BY eth_blocks.number, eth_blocks.miner
    UNION ALL
    SELECT
        from_address AS address,
        -(CAST(eth_receipts.gas_used AS real) * CAST(effective_gas_price AS real)) AS value
    FROM eth_transactions
    JOIN eth_receipts ON eth_transactions.hash = eth_receipts.transaction_hash
)
SELECT address, SUM(value) AS balance
FROM double_entry_book
GROUP BY address
ORDER BY balance DESC
LIMIT 1000;