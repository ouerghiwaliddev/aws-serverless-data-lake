-- ============================================================
-- revenue_by_product.sql
-- Query the Gold zone: daily revenue breakdown by product category
-- ⚡ Cost tip: always filter on partition columns (year/month/day)
-- ============================================================

-- 1. Total revenue per category for the current month
SELECT
    category,
    SUM(total_revenue)      AS monthly_revenue,
    SUM(units_sold)         AS monthly_units,
    SUM(transaction_count)  AS monthly_transactions,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM "datalake_gold"."revenue_by_product"
WHERE year  = '2026'
  AND month = '05'
GROUP BY category
ORDER BY monthly_revenue DESC;


-- 2. Daily revenue trend for Electronics (last 30 days)
SELECT
    year,
    month,
    day,
    total_revenue,
    units_sold,
    transaction_count
FROM "datalake_gold"."revenue_by_product"
WHERE year     = '2026'
  AND month    = '05'
  AND category = 'Electronics'
ORDER BY year, month, day;


-- 3. Revenue share (%) per category on a specific day
SELECT
    category,
    total_revenue,
    ROUND(
        total_revenue * 100.0 / SUM(total_revenue) OVER (), 2
    ) AS revenue_share_pct
FROM "datalake_gold"."revenue_by_product"
WHERE year  = '2026'
  AND month = '05'
  AND day   = '16'
ORDER BY revenue_share_pct DESC;


-- 4. Month-over-month growth per category
WITH current_month AS (
    SELECT category, SUM(total_revenue) AS revenue
    FROM "datalake_gold"."revenue_by_product"
    WHERE year = '2026' AND month = '05'
    GROUP BY category
),
previous_month AS (
    SELECT category, SUM(total_revenue) AS revenue
    FROM "datalake_gold"."revenue_by_product"
    WHERE year = '2026' AND month = '04'
    GROUP BY category
)
SELECT
    c.category,
    c.revenue                                         AS current_revenue,
    p.revenue                                         AS previous_revenue,
    ROUND((c.revenue - p.revenue) / p.revenue * 100, 2) AS growth_pct
FROM current_month c
LEFT JOIN previous_month p USING (category)
ORDER BY growth_pct DESC;
