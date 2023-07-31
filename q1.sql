WITH eth_throughput AS (
    SELECT "ethereum" AS chain, count(*) / (24 * 60 * 60.0 / count(*) OVER (PARTITION BY DATE(DATETIME(block_timestamp, "unixepoch")))) as throughput, block_timestamp AS time
    FROM eth_transactions
    GROUP BY eth_transactions.block_number, eth_transactions.block_timestamp
    ORDER BY throughput DESC
    LIMIT 1
),
btc_throughput AS (
    SELECT "bitcoin" AS chain, count(*) / (24 * 60 * 60.0 / count(*) OVER (PARTITION BY DATE(DATETIME(block_timestamp, "unixepoch")))) as throughput, block_timestamp AS time
    FROM btc_transactions
    GROUP BY btc_transactions.block_number, btc_transactions.block_timestamp
    ORDER BY throughput DESC
    LIMIT 1
)
SELECT * FROM eth_throughput
UNION ALL
SELECT * FROM btc_throughput
ORDER BY throughput DESC;
