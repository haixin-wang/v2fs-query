WITH all_transactions AS (
    SELECT hash AS transaction_hash, block_number, block_timestamp, from_address, to_address
    FROM eth_transactions
),
addresses_with_high_fanout AS (
    SELECT from_address AS address
    FROM all_transactions
    GROUP BY from_address
    HAVING COUNT(*) > 50
),
transactions_0_hops AS (
    SELECT
        0 AS hops,
        transactions.from_address,
        transactions.to_address,
        transactions.block_timestamp,
        transactions.from_address || '->' || transactions.to_address AS path
    FROM all_transactions AS transactions
    WHERE transactions.from_address = '0xae2fc483527b8ef99eb5d9b44875f005ba1fae13'
),
transactions_1_hops AS (
    SELECT 
        1 AS hops,
        transactions.from_address,
        transactions.to_address,
        transactions.block_timestamp,
        path || '->' || transactions.to_address AS path
    FROM all_transactions AS transactions
    INNER JOIN transactions_0_hops ON transactions_0_hops.to_address = transactions.from_address
    AND transactions_0_hops.block_timestamp <= transactions.block_timestamp
    LEFT JOIN addresses_with_high_fanout
    ON addresses_with_high_fanout.address = transactions.from_address
    WHERE addresses_with_high_fanout.address IS NULL
),
transactions_2_hops AS (
    SELECT 
        2 AS hops,
        transactions.from_address,
        transactions.to_address,
        transactions.block_timestamp,
        path || '->' || transactions.to_address AS path
    FROM all_transactions AS transactions
    INNER JOIN transactions_1_hops ON transactions_1_hops.to_address = transactions.from_address
    AND transactions_1_hops.block_timestamp <= transactions.block_timestamp
    LEFT JOIN addresses_with_high_fanout
    ON addresses_with_high_fanout.address = transactions.from_address
    WHERE addresses_with_high_fanout.address IS NULL
),
transactions_3_hops AS (
    SELECT 
        3 AS hops,
        transactions.from_address,
        transactions.to_address,
        transactions.block_timestamp,
        path || '->' || transactions.to_address AS path
    FROM all_transactions AS transactions
    INNER JOIN transactions_2_hops ON transactions_2_hops.to_address = transactions.from_address
    AND transactions_2_hops.block_timestamp <= transactions.block_timestamp
    LEFT JOIN addresses_with_high_fanout
    ON addresses_with_high_fanout.address = transactions.from_address
    WHERE addresses_with_high_fanout.address IS NULL
),
transactions_all_hops AS (
    SELECT * FROM transactions_0_hops WHERE to_address = "0x364d523e171767b7e0f730dafe3e36f93a0f66db"
    UNION ALL
    SELECT * FROM transactions_1_hops WHERE to_address = "0x364d523e171767b7e0f730dafe3e36f93a0f66db"
    UNION ALL
    SELECT * FROM transactions_2_hops WHERE to_address = "0x364d523e171767b7e0f730dafe3e36f93a0f66db"
    UNION ALL
    SELECT * FROM transactions_3_hops WHERE to_address = "0x364d523e171767b7e0f730dafe3e36f93a0f66db"
)
SELECT
    hops,
    path
FROM transactions_all_hops
ORDER BY hops ASC
LIMIT 100;