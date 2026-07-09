-- Run in Hue/Impala. Replace __ANALYTICS_DATABASE__ before running.

CREATE DATABASE IF NOT EXISTS __ANALYTICS_DATABASE__;

CREATE TABLE IF NOT EXISTS __ANALYTICS_DATABASE__.vehicle_health_enriched (
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

CREATE TABLE IF NOT EXISTS __ANALYTICS_DATABASE__.daily_vehicle_health_summary (
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

CREATE TABLE IF NOT EXISTS __ANALYTICS_DATABASE__.service_kpi_summary (
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

CREATE TABLE IF NOT EXISTS __ANALYTICS_DATABASE__.data_quality_report (
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
