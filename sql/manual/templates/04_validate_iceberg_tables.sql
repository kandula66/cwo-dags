-- Run in Hue/Impala after loading Iceberg tables.
-- Replace __BUSINESS_DATE__, __ANALYTICS_DATABASE__, and __STAGING_DATABASE__ before running.

SELECT
    'vehicle_health_enriched' AS table_name,
    '__BUSINESS_DATE__' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM __ANALYTICS_DATABASE__.vehicle_health_enriched
WHERE business_date = '__BUSINESS_DATE__';

SELECT
    'daily_vehicle_health_summary' AS table_name,
    '__BUSINESS_DATE__' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM __ANALYTICS_DATABASE__.daily_vehicle_health_summary
WHERE business_date = '__BUSINESS_DATE__';

SELECT
    'service_kpi_summary' AS table_name,
    '__BUSINESS_DATE__' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM __ANALYTICS_DATABASE__.service_kpi_summary
WHERE business_date = '__BUSINESS_DATE__';

SELECT
    'data_quality_report' AS table_name,
    '__BUSINESS_DATE__' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM __ANALYTICS_DATABASE__.data_quality_report
WHERE business_date = '__BUSINESS_DATE__';

SELECT
    dataset_name,
    metric_name,
    metric_value,
    status,
    business_date,
    pipeline_run_id,
    reported_at
FROM __ANALYTICS_DATABASE__.data_quality_report
WHERE business_date = '__BUSINESS_DATE__'
ORDER BY reported_at DESC;
