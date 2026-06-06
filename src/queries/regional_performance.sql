-- ============================================================
-- regional_performance.sql
-- Analyse sales performance across store regions
-- ============================================================

-- 1. Monthly revenue and store activity by region
SELECT
    region,
    SUM(total_revenue)       AS monthly_revenue,
    SUM(transaction_count)   AS monthly_transactions,
    MAX(active_stores)       AS active_stores,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM "datalake_gold"."revenue_by_region"
WHERE year  = '2026'
  AND month = '05'
GROUP BY region
ORDER BY monthly_revenue DESC;


-- 2. Daily revenue trend per region (heatmap source)
SELECT
    day,
    region,
    total_revenue,
    transaction_count
FROM "datalake_gold"."revenue_by_region"
WHERE year  = '2026'
  AND month = '05'
ORDER BY day, region;


-- 3. Best performing region per day
SELECT
    day,
    region,
    total_revenue
FROM (
    SELECT
        day,
        region,
        total_revenue,
        RANK() OVER (PARTITION BY day ORDER BY total_revenue DESC) AS rnk
    FROM "datalake_gold"."revenue_by_region"
    WHERE year  = '2026'
      AND month = '05'
)
WHERE rnk = 1
ORDER BY day;


-- 4. Region revenue vs national average
WITH daily_avg AS (
    SELECT
        year, month, day,
        AVG(total_revenue) AS national_avg
    FROM "datalake_gold"."revenue_by_region"
    WHERE year = '2026' AND month = '05'
    GROUP BY year, month, day
)
SELECT
    r.day,
    r.region,
    r.total_revenue,
    ROUND(da.national_avg, 2)                                    AS national_avg,
    ROUND((r.total_revenue - da.national_avg) / da.national_avg * 100, 2) AS vs_avg_pct
FROM "datalake_gold"."revenue_by_region" r
JOIN daily_avg da USING (year, month, day)
WHERE r.year  = '2026'
  AND r.month = '05'
ORDER BY r.day, vs_avg_pct DESC;
