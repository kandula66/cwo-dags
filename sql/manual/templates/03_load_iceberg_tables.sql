-- Run in Hue/Impala after creating staging tables.
-- Replace __BUSINESS_DATE__, __ANALYTICS_DATABASE__, and __STAGING_DATABASE__ before running.

DELETE FROM __ANALYTICS_DATABASE__.vehicle_health_enriched
WHERE business_date = '__BUSINESS_DATE__';

INSERT INTO __ANALYTICS_DATABASE__.vehicle_health_enriched (
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
FROM __STAGING_DATABASE__.vehicle_health_enriched_parquet
WHERE business_date = '__BUSINESS_DATE__';

DELETE FROM __ANALYTICS_DATABASE__.daily_vehicle_health_summary
WHERE business_date = '__BUSINESS_DATE__';

INSERT INTO __ANALYTICS_DATABASE__.daily_vehicle_health_summary (
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
FROM __STAGING_DATABASE__.daily_vehicle_health_summary_parquet
WHERE business_date = '__BUSINESS_DATE__';

DELETE FROM __ANALYTICS_DATABASE__.service_kpi_summary
WHERE business_date = '__BUSINESS_DATE__';

INSERT INTO __ANALYTICS_DATABASE__.service_kpi_summary (
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
FROM __STAGING_DATABASE__.service_kpi_summary_parquet
WHERE business_date = '__BUSINESS_DATE__';

DELETE FROM __ANALYTICS_DATABASE__.data_quality_report
WHERE business_date = '__BUSINESS_DATE__';

INSERT INTO __ANALYTICS_DATABASE__.data_quality_report (
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
FROM __STAGING_DATABASE__.data_quality_report_parquet
WHERE business_date = '__BUSINESS_DATE__';
