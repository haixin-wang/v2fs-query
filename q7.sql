WITH double_entry_book AS (
    SELECT btc_outputs.addresses AS address, value, block_timestamp
    FROM btc_transactions INNER JOIN btc_outputs ON btc_transactions.hash = btc_outputs.transaction_hash
    UNION ALL
    SELECT btc_inputs.addresses AS address, -value AS value, block_timestamp
    FROM btc_transactions INNER JOIN btc_inputs ON btc_transactions.hash = btc_inputs.transaction_hash
),
double_entry_book_by_date AS (
    SELECT DATE(block_timestamp, "unixepoch") AS date, address, SUM(value * 0.00000001) AS value
    FROM double_entry_book
    GROUP BY address, date
),
daily_balances_with_gaps AS (
    SELECT 
        address, 
        date, 
        SUM(value) OVER (PARTITION BY address ORDER BY date) AS balance,
        LEAD(date, 1, '2023-05-18') OVER (PARTITION BY address ORDER BY date) AS next_date
        FROM double_entry_book_by_date
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
    WHERE balance > 1
),
address_counts AS (
    SELECT
        date,
        count(*) AS address_count
    FROM
        daily_balances
    GROUP BY date
),
daily_balances_sampled AS (
    SELECT address, daily_balances.date, balance
    FROM daily_balances
    JOIN address_counts ON daily_balances.date = address_counts.date
),
ranked_daily_balances AS (
    SELECT 
        date,
        balance,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY balance DESC) AS rank
    FROM daily_balances_sampled
)
SELECT 
    date,
    1 - 2 * SUM((balance * (rank - 1) + balance / 2)) / COUNT(*) / SUM(balance) AS gini
FROM ranked_daily_balances
GROUP BY date
HAVING SUM(balance) > 0
ORDER BY date ASC;
