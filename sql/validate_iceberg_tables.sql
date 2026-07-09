{% set business_date = dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) %}
{% set gold_db = params.gold_database %}
{% set staging_db = params.staging_database %}

SELECT
    'vehicle_health_enriched' AS table_name,
    '{{ business_date }}' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM {{ gold_db }}.vehicle_health_enriched
WHERE business_date = '{{ business_date }}';

SELECT
    'daily_vehicle_health_summary' AS table_name,
    '{{ business_date }}' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM {{ gold_db }}.daily_vehicle_health_summary
WHERE business_date = '{{ business_date }}';

SELECT
    'service_kpi_summary' AS table_name,
    '{{ business_date }}' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM {{ gold_db }}.service_kpi_summary
WHERE business_date = '{{ business_date }}';

SELECT
    'data_quality_report' AS table_name,
    '{{ business_date }}' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM {{ gold_db }}.data_quality_report
WHERE business_date = '{{ business_date }}';

SELECT
    'vehicle_health_enriched' AS table_name,
    staging.row_count AS staging_row_count,
    iceberg.row_count AS iceberg_row_count,
    iceberg.row_count - staging.row_count AS row_count_delta
FROM
    (
        SELECT COUNT(*) AS row_count
        FROM {{ staging_db }}.vehicle_health_enriched_parquet
        WHERE business_date = '{{ business_date }}'
    ) staging,
    (
        SELECT COUNT(*) AS row_count
        FROM {{ gold_db }}.vehicle_health_enriched
        WHERE business_date = '{{ business_date }}'
    ) iceberg;

SELECT
    'daily_vehicle_health_summary' AS table_name,
    staging.row_count AS staging_row_count,
    iceberg.row_count AS iceberg_row_count,
    iceberg.row_count - staging.row_count AS row_count_delta
FROM
    (
        SELECT COUNT(*) AS row_count
        FROM {{ staging_db }}.daily_vehicle_health_summary_parquet
        WHERE business_date = '{{ business_date }}'
    ) staging,
    (
        SELECT COUNT(*) AS row_count
        FROM {{ gold_db }}.daily_vehicle_health_summary
        WHERE business_date = '{{ business_date }}'
    ) iceberg;

SELECT
    'service_kpi_summary' AS table_name,
    staging.row_count AS staging_row_count,
    iceberg.row_count AS iceberg_row_count,
    iceberg.row_count - staging.row_count AS row_count_delta
FROM
    (
        SELECT COUNT(*) AS row_count
        FROM {{ staging_db }}.service_kpi_summary_parquet
        WHERE business_date = '{{ business_date }}'
    ) staging,
    (
        SELECT COUNT(*) AS row_count
        FROM {{ gold_db }}.service_kpi_summary
        WHERE business_date = '{{ business_date }}'
    ) iceberg;

SELECT
    'data_quality_report' AS table_name,
    staging.row_count AS staging_row_count,
    iceberg.row_count AS iceberg_row_count,
    iceberg.row_count - staging.row_count AS row_count_delta
FROM
    (
        SELECT COUNT(*) AS row_count
        FROM {{ staging_db }}.data_quality_report_parquet
        WHERE business_date = '{{ business_date }}'
    ) staging,
    (
        SELECT COUNT(*) AS row_count
        FROM {{ gold_db }}.data_quality_report
        WHERE business_date = '{{ business_date }}'
    ) iceberg;

SELECT
    dataset_name,
    metric_name,
    metric_value,
    status,
    business_date,
    pipeline_run_id,
    reported_at
FROM {{ gold_db }}.data_quality_report
WHERE business_date = '{{ business_date }}'
ORDER BY reported_at DESC;
