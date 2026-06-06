-- ============================================================
-- anomaly_detection.sql
-- Detect unusual transaction patterns in the Silver zone
-- ============================================================

-- 1. Transactions with abnormally high amounts (> mean + 3 std deviations)
WITH stats AS (
    SELECT
        category,
        AVG(total_amount)    AS mean_amount,
        STDDEV(total_amount) AS std_amount
    FROM "datalake_silver"."transactions"
    WHERE year = '2026' AND month = '05'
    GROUP BY category
)
SELECT
    t.transaction_id,
    t.store_id,
    t.category,
    t.total_amount,
    ROUND(s.mean_amount, 2)                                     AS category_mean,
    ROUND((t.total_amount - s.mean_amount) / s.std_amount, 2)  AS z_score
FROM "datalake_silver"."transactions" t
JOIN stats s USING (category)
WHERE year = '2026'
  AND month = '05'
  AND t.total_amount > (s.mean_amount + 3 * s.std_amount)
ORDER BY z_score DESC
LIMIT 50;


-- 2. Stores with sudden drop in transaction volume (> 50% below daily average)
WITH daily_store_counts AS (
    SELECT
        store_id,
        day,
        COUNT(*) AS daily_transactions
    FROM "datalake_silver"."transactions"
    WHERE year = '2026' AND month = '05'
    GROUP BY store_id, day
),
store_avg AS (
    SELECT
        store_id,
        AVG(daily_transactions) AS avg_daily_transactions
    FROM daily_store_counts
    GROUP BY store_id
)
SELECT
    dsc.store_id,
    dsc.day,
    dsc.daily_transactions,
    ROUND(sa.avg_daily_transactions, 1) AS store_avg,
    ROUND(dsc.daily_transactions * 100.0 / sa.avg_daily_transactions, 1) AS pct_of_avg
FROM daily_store_counts dsc
JOIN store_avg sa USING (store_id)
WHERE dsc.daily_transactions < sa.avg_daily_transactions * 0.5
ORDER BY pct_of_avg ASC;


-- 3. Unusual return rate spikes by store
SELECT
    store_id,
    day,
    COUNT(*)                                           AS total_transactions,
    SUM(CASE WHEN is_return = true THEN 1 ELSE 0 END) AS returns,
    ROUND(
        SUM(CASE WHEN is_return = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                  AS return_rate_pct
FROM "datalake_silver"."transactions"
WHERE year  = '2026'
  AND month = '05'
GROUP BY store_id, day
HAVING return_rate_pct > 10          -- flag stores with >10% return rate
ORDER BY return_rate_pct DESC;


-- 4. Hourly transaction volume to detect off-hours activity
SELECT
    HOUR(timestamp)        AS hour_of_day,
    COUNT(*)               AS transaction_count,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM "datalake_silver"."transactions"
WHERE year  = '2026'
  AND month = '05'
  AND day   = '16'
GROUP BY HOUR(timestamp)
ORDER BY hour_of_day;
