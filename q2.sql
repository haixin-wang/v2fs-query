WITH ether_emitted_by_date AS (
    SELECT DATE(timestamp, 'unixepoch') AS date, SUM(value) AS value
    FROM eth_traces JOIN eth_blocks ON eth_traces.block_number = eth_blocks.number
    WHERE trace_type IN ("genesis", "reward")
    GROUP BY date
)
SELECT date, SUM(value) OVER (ORDER BY date) / 1000000000000000000.0 AS supply
FROM ether_emitted_by_date;