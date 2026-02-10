#!/bin/bash
# Diagnostic script to analyze multi-year returns data
# Run with: sudo ./scripts/diagnose_multi_year_returns.sh > logs/diagnose_output.log 2>&1

echo "=== MULTI-YEAR RETURNS DIAGNOSTIC ==="
echo "Date: $(date)"
echo ""

echo "=== 1. Check date range in fact table ==="
sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "
SELECT
    MIN(d.date) as min_date,
    MAX(d.date) as max_date,
    COUNT(*) as total_rows,
    COUNT(DISTINCT s.ticker) as unique_stocks
FROM analytics.fact_daily_market f
JOIN analytics.dim_date d ON f.date_id = d.date_id
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id;
"

echo ""
echo "=== 2. Check data availability by year ==="
sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "
SELECT
    d.year,
    COUNT(*) as rows,
    COUNT(DISTINCT s.ticker) as stocks
FROM analytics.fact_daily_market f
JOIN analytics.dim_date d ON f.date_id = d.date_id
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
GROUP BY d.year
ORDER BY d.year;
"

echo ""
echo "=== 3. Check first and last date per stock ==="
sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "
SELECT
    s.ticker,
    MIN(d.date) as first_date,
    MAX(d.date) as last_date,
    (MAX(d.date) - MIN(d.date)) / 365.25 as years_of_data
FROM analytics.fact_daily_market f
JOIN analytics.dim_date d ON f.date_id = d.date_id
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
GROUP BY s.ticker
ORDER BY s.ticker;
"

echo ""
echo "=== 4. Test multi-year returns query (simplified) ==="
sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "
WITH date_bounds AS (
    SELECT
        MAX(d.date) as max_date,
        MAX(d.date) - INTERVAL '1 year' as date_1y,
        MAX(d.date) - INTERVAL '3 years' as date_3y,
        MAX(d.date) - INTERVAL '5 years' as date_5y,
        MAX(d.date) - INTERVAL '10 years' as date_10y
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_date d ON f.date_id = d.date_id
)
SELECT * FROM date_bounds;
"

echo ""
echo "=== 5. Check if prices exist for historical dates (PETR4.SA) ==="
sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "
WITH date_bounds AS (
    SELECT
        MAX(d.date) as max_date,
        MAX(d.date) - INTERVAL '1 year' as date_1y,
        MAX(d.date) - INTERVAL '3 years' as date_3y,
        MAX(d.date) - INTERVAL '5 years' as date_5y,
        MAX(d.date) - INTERVAL '10 years' as date_10y
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_date d ON f.date_id = d.date_id
)
SELECT
    'Current' as period, db.max_date as target_date, d.date, f.close_price
FROM analytics.fact_daily_market f
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
JOIN analytics.dim_date d ON f.date_id = d.date_id
CROSS JOIN date_bounds db
WHERE s.ticker = 'PETR4.SA' AND d.date = db.max_date

UNION ALL

SELECT
    '1Y Ago' as period, db.date_1y as target_date, d.date, f.close_price
FROM analytics.fact_daily_market f
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
JOIN analytics.dim_date d ON f.date_id = d.date_id
CROSS JOIN date_bounds db
WHERE s.ticker = 'PETR4.SA' AND d.date <= db.date_1y
ORDER BY d.date DESC
LIMIT 1;
"

echo ""
echo "=== 6. Sample of raw.stocks data ==="
sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "
SELECT ticker, MIN(date), MAX(date), COUNT(*) as rows
FROM raw.stocks
GROUP BY ticker
ORDER BY ticker
LIMIT 20;
"

echo ""
echo "=== 7. Check if DISTINCT ON works (test query) ==="
sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "
WITH date_bounds AS (
    SELECT MAX(d.date) - INTERVAL '1 year' as date_1y
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_date d ON f.date_id = d.date_id
)
SELECT DISTINCT ON (s.stock_id)
    s.ticker, d.date, f.close_price
FROM analytics.fact_daily_market f
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
JOIN analytics.dim_date d ON f.date_id = d.date_id
CROSS JOIN date_bounds db
WHERE d.date <= db.date_1y
ORDER BY s.stock_id, d.date DESC
LIMIT 5;
"

echo ""
echo "=== 8. Full multi-year returns query result ==="
sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "
WITH date_bounds AS (
    SELECT
        MAX(d.date) as max_date,
        MAX(d.date) - INTERVAL '1 year' as date_1y,
        MAX(d.date) - INTERVAL '3 years' as date_3y,
        MAX(d.date) - INTERVAL '5 years' as date_5y,
        MAX(d.date) - INTERVAL '10 years' as date_10y
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_date d ON f.date_id = d.date_id
),
latest_prices AS (
    SELECT
        s.stock_id,
        s.ticker,
        f.close_price as current_price
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
    JOIN analytics.dim_date d ON f.date_id = d.date_id
    CROSS JOIN date_bounds db
    WHERE d.date = db.max_date
),
price_1y AS (
    SELECT DISTINCT ON (s.stock_id)
        s.stock_id,
        f.close_price
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
    JOIN analytics.dim_date d ON f.date_id = d.date_id
    CROSS JOIN date_bounds db
    WHERE d.date <= db.date_1y
    ORDER BY s.stock_id, d.date DESC
),
price_3y AS (
    SELECT DISTINCT ON (s.stock_id)
        s.stock_id,
        f.close_price
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
    JOIN analytics.dim_date d ON f.date_id = d.date_id
    CROSS JOIN date_bounds db
    WHERE d.date <= db.date_3y
    ORDER BY s.stock_id, d.date DESC
),
price_5y AS (
    SELECT DISTINCT ON (s.stock_id)
        s.stock_id,
        f.close_price
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
    JOIN analytics.dim_date d ON f.date_id = d.date_id
    CROSS JOIN date_bounds db
    WHERE d.date <= db.date_5y
    ORDER BY s.stock_id, d.date DESC
),
price_10y AS (
    SELECT DISTINCT ON (s.stock_id)
        s.stock_id,
        f.close_price
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
    JOIN analytics.dim_date d ON f.date_id = d.date_id
    CROSS JOIN date_bounds db
    WHERE d.date <= db.date_10y
    ORDER BY s.stock_id, d.date DESC
)
SELECT
    lp.ticker,
    lp.current_price,
    p1.close_price as price_1y_ago,
    p3.close_price as price_3y_ago,
    p5.close_price as price_5y_ago,
    p10.close_price as price_10y_ago,
    ROUND(((lp.current_price - p1.close_price) / NULLIF(p1.close_price, 0) * 100)::numeric, 2) as return_1y,
    ROUND(((lp.current_price - p3.close_price) / NULLIF(p3.close_price, 0) * 100)::numeric, 2) as return_3y,
    ROUND(((lp.current_price - p5.close_price) / NULLIF(p5.close_price, 0) * 100)::numeric, 2) as return_5y,
    ROUND(((lp.current_price - p10.close_price) / NULLIF(p10.close_price, 0) * 100)::numeric, 2) as return_10y
FROM latest_prices lp
LEFT JOIN price_1y p1 ON lp.stock_id = p1.stock_id
LEFT JOIN price_3y p3 ON lp.stock_id = p3.stock_id
LEFT JOIN price_5y p5 ON lp.stock_id = p5.stock_id
LEFT JOIN price_10y p10 ON lp.stock_id = p10.stock_id
ORDER BY lp.ticker;
"

echo ""
echo "=== DIAGNOSTIC COMPLETE ==="
