-- ============================================================================
-- 01_create_iceberg_tables.sql
-- ============================================================================

-- Run in Hue/Impala. Replace porsche_vehicle_health_analytics before running.

CREATE DATABASE IF NOT EXISTS porsche_vehicle_health_analytics;

CREATE TABLE IF NOT EXISTS porsche_vehicle_health_analytics.vehicle_health_enriched (
    source_file STRING,
    vin STRING,
    event_timestamp TIMESTAMP,
    event_date DATE,
    odometer_km DOUBLE,
    battery_voltage DOUBLE,
    engine_temp_c DOUBLE,
    oil_pressure_kpa DOUBLE,
    tire_pressure_fl DOUBLE,
    tire_pressure_fr DOUBLE,
    tire_pressure_rl DOUBLE,
    tire_pressure_rr DOUBLE,
    dtc_code STRING,
    severity STRING,
    country STRING,
    business_date STRING,
    source_zip STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP,
    model_year INT,
    powertrain STRING,
    production_plant STRING,
    warranty_start_date DATE,
    customer_region STRING,
    dealer_id STRING,
    service_type STRING,
    service_status STRING,
    warranty_flag BOOLEAN,
    service_duration_hours DOUBLE,
    dealer_name STRING,
    region STRING,
    dealer_tier STRING,
    is_critical_event BOOLEAN,
    is_warning_event BOOLEAN,
    model STRING
)
PARTITIONED BY SPEC (business_date)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS porsche_vehicle_health_analytics.daily_vehicle_health_summary (
    business_date STRING,
    event_date DATE,
    country STRING,
    customer_region STRING,
    model_year INT,
    powertrain STRING,
    severity STRING,
    telemetry_event_count BIGINT,
    affected_vehicle_count BIGINT,
    critical_dtc_count BIGINT,
    avg_odometer_km DOUBLE,
    max_odometer_km DOUBLE,
    avg_battery_voltage DOUBLE,
    avg_engine_temp_c DOUBLE,
    repeated_warning_vehicle_count BIGINT,
    model STRING
)
PARTITIONED BY SPEC (business_date)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS porsche_vehicle_health_analytics.service_kpi_summary (
    business_date STRING,
    dealer_id STRING,
    dealer_name STRING,
    country STRING,
    dealer_tier STRING,
    model STRING,
    model_year INT,
    powertrain STRING,
    service_type STRING,
    service_event_count BIGINT,
    avg_service_duration_hours DOUBLE,
    avg_labor_hours DOUBLE,
    total_parts_cost DECIMAL(38,2),
    warranty_service_rate DOUBLE,
    region STRING
)
PARTITIONED BY SPEC (business_date)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS porsche_vehicle_health_analytics.data_quality_report (
    dataset_name STRING,
    metric_name STRING,
    metric_value BIGINT,
    status STRING,
    business_date STRING,
    pipeline_run_id STRING,
    reported_at TIMESTAMP
)
PARTITIONED BY SPEC (business_date)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');


-- ============================================================================
-- 02_create_parquet_staging_tables.sql
-- ============================================================================

-- Run in Hue/Impala only after the Spark Parquet output exists.
-- Replace 2026-07-09, s3a://porscheenv-buk-3b2308dc/data/porsche/curated/vehicle-health, and porsche_vehicle_health_analytics_staging before running.

CREATE DATABASE IF NOT EXISTS porsche_vehicle_health_analytics_staging;

DROP TABLE IF EXISTS porsche_vehicle_health_analytics_staging.vehicle_health_enriched_parquet;
CREATE EXTERNAL TABLE porsche_vehicle_health_analytics_staging.vehicle_health_enriched_parquet (
    source_file STRING,
    vin STRING,
    event_timestamp TIMESTAMP,
    event_date DATE,
    odometer_km DOUBLE,
    battery_voltage DOUBLE,
    engine_temp_c DOUBLE,
    oil_pressure_kpa DOUBLE,
    tire_pressure_fl DOUBLE,
    tire_pressure_fr DOUBLE,
    tire_pressure_rl DOUBLE,
    tire_pressure_rr DOUBLE,
    dtc_code STRING,
    severity STRING,
    country STRING,
    business_date STRING,
    source_zip STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP,
    model_year INT,
    powertrain STRING,
    production_plant STRING,
    warranty_start_date DATE,
    customer_region STRING,
    dealer_id STRING,
    service_type STRING,
    service_status STRING,
    warranty_flag BOOLEAN,
    service_duration_hours DOUBLE,
    dealer_name STRING,
    region STRING,
    dealer_tier STRING,
    is_critical_event BOOLEAN,
    is_warning_event BOOLEAN
)
PARTITIONED BY (model STRING)
STORED AS PARQUET
LOCATION 's3a://porscheenv-buk-3b2308dc/data/porsche/curated/vehicle-health/business_date=2026-07-09/vehicle_health_enriched';

ALTER TABLE porsche_vehicle_health_analytics_staging.vehicle_health_enriched_parquet RECOVER PARTITIONS;

DROP TABLE IF EXISTS porsche_vehicle_health_analytics_staging.daily_vehicle_health_summary_parquet;
CREATE EXTERNAL TABLE porsche_vehicle_health_analytics_staging.daily_vehicle_health_summary_parquet (
    business_date STRING,
    event_date DATE,
    country STRING,
    customer_region STRING,
    model_year INT,
    powertrain STRING,
    severity STRING,
    telemetry_event_count BIGINT,
    affected_vehicle_count BIGINT,
    critical_dtc_count BIGINT,
    avg_odometer_km DOUBLE,
    max_odometer_km DOUBLE,
    avg_battery_voltage DOUBLE,
    avg_engine_temp_c DOUBLE,
    repeated_warning_vehicle_count BIGINT
)
PARTITIONED BY (model STRING)
STORED AS PARQUET
LOCATION 's3a://porscheenv-buk-3b2308dc/data/porsche/curated/vehicle-health/business_date=2026-07-09/daily_vehicle_health_summary';

ALTER TABLE porsche_vehicle_health_analytics_staging.daily_vehicle_health_summary_parquet RECOVER PARTITIONS;

DROP TABLE IF EXISTS porsche_vehicle_health_analytics_staging.service_kpi_summary_parquet;
CREATE EXTERNAL TABLE porsche_vehicle_health_analytics_staging.service_kpi_summary_parquet (
    business_date STRING,
    dealer_id STRING,
    dealer_name STRING,
    country STRING,
    dealer_tier STRING,
    model STRING,
    model_year INT,
    powertrain STRING,
    service_type STRING,
    service_event_count BIGINT,
    avg_service_duration_hours DOUBLE,
    avg_labor_hours DOUBLE,
    total_parts_cost DECIMAL(38,2),
    warranty_service_rate DOUBLE
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 's3a://porscheenv-buk-3b2308dc/data/porsche/curated/vehicle-health/business_date=2026-07-09/service_kpi_summary';

ALTER TABLE porsche_vehicle_health_analytics_staging.service_kpi_summary_parquet RECOVER PARTITIONS;

DROP TABLE IF EXISTS porsche_vehicle_health_analytics_staging.data_quality_report_parquet;
CREATE EXTERNAL TABLE porsche_vehicle_health_analytics_staging.data_quality_report_parquet (
    dataset_name STRING,
    metric_name STRING,
    metric_value BIGINT,
    status STRING,
    business_date STRING,
    pipeline_run_id STRING,
    reported_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3a://porscheenv-buk-3b2308dc/data/porsche/curated/vehicle-health/business_date=2026-07-09/data_quality_report';


-- ============================================================================
-- 03_load_iceberg_tables.sql
-- ============================================================================

-- Run in Hue/Impala after creating staging tables.
-- Replace 2026-07-09, porsche_vehicle_health_analytics, and porsche_vehicle_health_analytics_staging before running.

DELETE FROM porsche_vehicle_health_analytics.vehicle_health_enriched
WHERE business_date = '2026-07-09';

INSERT INTO porsche_vehicle_health_analytics.vehicle_health_enriched (
    source_file,
    vin,
    event_timestamp,
    event_date,
    odometer_km,
    battery_voltage,
    engine_temp_c,
    oil_pressure_kpa,
    tire_pressure_fl,
    tire_pressure_fr,
    tire_pressure_rl,
    tire_pressure_rr,
    dtc_code,
    severity,
    country,
    business_date,
    source_zip,
    pipeline_run_id,
    ingested_at,
    model_year,
    powertrain,
    production_plant,
    warranty_start_date,
    customer_region,
    dealer_id,
    service_type,
    service_status,
    warranty_flag,
    service_duration_hours,
    dealer_name,
    region,
    dealer_tier,
    is_critical_event,
    is_warning_event,
    model
)
SELECT
    source_file,
    vin,
    event_timestamp,
    event_date,
    odometer_km,
    battery_voltage,
    engine_temp_c,
    oil_pressure_kpa,
    tire_pressure_fl,
    tire_pressure_fr,
    tire_pressure_rl,
    tire_pressure_rr,
    dtc_code,
    severity,
    country,
    business_date,
    source_zip,
    pipeline_run_id,
    ingested_at,
    model_year,
    powertrain,
    production_plant,
    warranty_start_date,
    customer_region,
    dealer_id,
    service_type,
    service_status,
    warranty_flag,
    service_duration_hours,
    dealer_name,
    region,
    dealer_tier,
    is_critical_event,
    is_warning_event,
    model
FROM porsche_vehicle_health_analytics_staging.vehicle_health_enriched_parquet
WHERE business_date = '2026-07-09';

DELETE FROM porsche_vehicle_health_analytics.daily_vehicle_health_summary
WHERE business_date = '2026-07-09';

INSERT INTO porsche_vehicle_health_analytics.daily_vehicle_health_summary (
    business_date,
    event_date,
    country,
    customer_region,
    model_year,
    powertrain,
    severity,
    telemetry_event_count,
    affected_vehicle_count,
    critical_dtc_count,
    avg_odometer_km,
    max_odometer_km,
    avg_battery_voltage,
    avg_engine_temp_c,
    repeated_warning_vehicle_count,
    model
)
SELECT
    business_date,
    event_date,
    country,
    customer_region,
    model_year,
    powertrain,
    severity,
    telemetry_event_count,
    affected_vehicle_count,
    critical_dtc_count,
    avg_odometer_km,
    max_odometer_km,
    avg_battery_voltage,
    avg_engine_temp_c,
    repeated_warning_vehicle_count,
    model
FROM porsche_vehicle_health_analytics_staging.daily_vehicle_health_summary_parquet
WHERE business_date = '2026-07-09';

DELETE FROM porsche_vehicle_health_analytics.service_kpi_summary
WHERE business_date = '2026-07-09';

INSERT INTO porsche_vehicle_health_analytics.service_kpi_summary (
    business_date,
    dealer_id,
    dealer_name,
    country,
    dealer_tier,
    model,
    model_year,
    powertrain,
    service_type,
    service_event_count,
    avg_service_duration_hours,
    avg_labor_hours,
    total_parts_cost,
    warranty_service_rate,
    region
)
SELECT
    business_date,
    dealer_id,
    dealer_name,
    country,
    dealer_tier,
    model,
    model_year,
    powertrain,
    service_type,
    service_event_count,
    avg_service_duration_hours,
    avg_labor_hours,
    total_parts_cost,
    warranty_service_rate,
    region
FROM porsche_vehicle_health_analytics_staging.service_kpi_summary_parquet
WHERE business_date = '2026-07-09';

DELETE FROM porsche_vehicle_health_analytics.data_quality_report
WHERE business_date = '2026-07-09';

INSERT INTO porsche_vehicle_health_analytics.data_quality_report (
    dataset_name,
    metric_name,
    metric_value,
    status,
    business_date,
    pipeline_run_id,
    reported_at
)
SELECT
    dataset_name,
    metric_name,
    metric_value,
    status,
    business_date,
    pipeline_run_id,
    reported_at
FROM porsche_vehicle_health_analytics_staging.data_quality_report_parquet
WHERE business_date = '2026-07-09';


-- ============================================================================
-- 04_validate_iceberg_tables.sql
-- ============================================================================

-- Run in Hue/Impala after loading Iceberg tables.
-- Replace 2026-07-09, porsche_vehicle_health_analytics, and porsche_vehicle_health_analytics_staging before running.

SELECT
    'vehicle_health_enriched' AS table_name,
    '2026-07-09' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM porsche_vehicle_health_analytics.vehicle_health_enriched
WHERE business_date = '2026-07-09';

SELECT
    'daily_vehicle_health_summary' AS table_name,
    '2026-07-09' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM porsche_vehicle_health_analytics.daily_vehicle_health_summary
WHERE business_date = '2026-07-09';

SELECT
    'service_kpi_summary' AS table_name,
    '2026-07-09' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM porsche_vehicle_health_analytics.service_kpi_summary
WHERE business_date = '2026-07-09';

SELECT
    'data_quality_report' AS table_name,
    '2026-07-09' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM porsche_vehicle_health_analytics.data_quality_report
WHERE business_date = '2026-07-09';

SELECT
    dataset_name,
    metric_name,
    metric_value,
    status,
    business_date,
    pipeline_run_id,
    reported_at
FROM porsche_vehicle_health_analytics.data_quality_report
WHERE business_date = '2026-07-09'
ORDER BY reported_at DESC;

