-- Run in Hue/Impala only after the Spark Parquet output exists.
-- Replace __BUSINESS_DATE__, __CURATED_S3_ROOT__, and __STAGING_DATABASE__ before running.

CREATE DATABASE IF NOT EXISTS __STAGING_DATABASE__;

DROP TABLE IF EXISTS __STAGING_DATABASE__.vehicle_health_enriched_parquet;
CREATE EXTERNAL TABLE __STAGING_DATABASE__.vehicle_health_enriched_parquet (
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
LOCATION '__CURATED_S3_ROOT__/business_date=__BUSINESS_DATE__/vehicle_health_enriched';

ALTER TABLE __STAGING_DATABASE__.vehicle_health_enriched_parquet RECOVER PARTITIONS;

DROP TABLE IF EXISTS __STAGING_DATABASE__.daily_vehicle_health_summary_parquet;
CREATE EXTERNAL TABLE __STAGING_DATABASE__.daily_vehicle_health_summary_parquet (
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
LOCATION '__CURATED_S3_ROOT__/business_date=__BUSINESS_DATE__/daily_vehicle_health_summary';

ALTER TABLE __STAGING_DATABASE__.daily_vehicle_health_summary_parquet RECOVER PARTITIONS;

DROP TABLE IF EXISTS __STAGING_DATABASE__.service_kpi_summary_parquet;
CREATE EXTERNAL TABLE __STAGING_DATABASE__.service_kpi_summary_parquet (
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
LOCATION '__CURATED_S3_ROOT__/business_date=__BUSINESS_DATE__/service_kpi_summary';

ALTER TABLE __STAGING_DATABASE__.service_kpi_summary_parquet RECOVER PARTITIONS;

DROP TABLE IF EXISTS __STAGING_DATABASE__.data_quality_report_parquet;
CREATE EXTERNAL TABLE __STAGING_DATABASE__.data_quality_report_parquet (
    dataset_name STRING,
    metric_name STRING,
    metric_value BIGINT,
    status STRING,
    business_date STRING,
    pipeline_run_id STRING,
    reported_at TIMESTAMP
)
STORED AS PARQUET
LOCATION '__CURATED_S3_ROOT__/business_date=__BUSINESS_DATE__/data_quality_report';
