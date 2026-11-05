-- Example analytical queries for DuckDB (run with: duckdb data/warehouse/datatrust.duckdb < sql/analytical_queries.sql
-- or through the dashboard's Data Explorer). Tables come from the models in dbt_project/models.

-- 1. Busiest days with a running share of total volume (CTE + window functions)
WITH daily AS (
    SELECT pickup_date, trips, fare_revenue FROM gold.daily_trip_metrics
)
SELECT pickup_date, trips,
       RANK() OVER (ORDER BY trips DESC) AS volume_rank,
       ROUND(100.0 * SUM(trips) OVER (ORDER BY pickup_date) / SUM(trips) OVER (), 2) AS cumulative_pct
FROM daily
ORDER BY volume_rank
LIMIT 10;

-- 2. Weekday vs weekend revenue (date/time function + conditional expression)
SELECT CASE WHEN isodow(pickup_date) >= 6 THEN 'weekend' ELSE 'weekday' END AS day_type,
       COUNT(*) AS days, SUM(trips) AS trips, ROUND(AVG(fare_revenue), 2) AS avg_daily_fare_revenue
FROM gold.daily_trip_metrics
GROUP BY 1;

-- 3. Top 10 pickup locations and their share of trips
SELECT pu_location_id, trips, ROUND(pct_of_trips, 2) AS pct_of_trips,
       ROUND(avg_fare, 2) AS avg_fare
FROM gold.location_metrics
ORDER BY trips DESC
LIMIT 10;

-- 4. Rush-hour profile: busiest hour for each ISO weekday (window function)
SELECT pickup_dow, pickup_hour, trips
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY pickup_dow ORDER BY trips DESC) AS rn
    FROM gold.hourly_trip_metrics
)
WHERE rn = 1
ORDER BY pickup_dow;

-- 5. Month-over-month fare revenue change (date_trunc + LAG)
WITH monthly AS (
    SELECT date_trunc('month', pickup_date) AS month, SUM(fare_revenue) AS fare_revenue
    FROM gold.daily_trip_metrics GROUP BY 1
)
SELECT month, ROUND(fare_revenue, 2) AS fare_revenue,
       ROUND(100.0 * (fare_revenue - LAG(fare_revenue) OVER (ORDER BY month))
             / NULLIF(LAG(fare_revenue) OVER (ORDER BY month), 0), 2) AS pct_change
FROM monthly ORDER BY month;

-- 6. Tip percentage by payment type (card only is meaningful)
SELECT payment_type_name, trips, ROUND(share_pct, 2) AS share_pct, ROUND(tip_pct, 2) AS tip_pct
FROM gold.payment_type_metrics ORDER BY trips DESC;

-- 7. Data quality: rules that failed in the latest run
SELECT stage, rule_name, severity, latest_failed, ROUND(latest_failure_pct, 3) AS failure_pct, latest_status
FROM gold.data_quality_summary
WHERE latest_failed > 0
ORDER BY latest_failure_pct DESC;

-- 8. Pipeline health: slowest runs and rejection rate
SELECT run_id, status, duration_seconds, records_read, ROUND(rejection_pct, 2) AS rejection_pct
FROM gold.pipeline_run_summary
ORDER BY duration_seconds DESC NULLS LAST
LIMIT 10;

-- 9. Partition pruning demo: only the 2023-01-15 files are read thanks to hive partitioning
SELECT COUNT(*) AS trips
FROM read_parquet('data/silver/trips/*/*.parquet', hive_partitioning = true)
WHERE pickup_date = '2023-01-15';
