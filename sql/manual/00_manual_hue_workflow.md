# Manual Hue Workflow

Use these files only for manual Impala/Hue testing.

For manual testing, open only the generated file:

```text
rendered/<current-date>/00_run_all_in_hue.sql
```

The `templates/` folder is only used by `render_manual_sql.py`; do not paste those files into Hue directly.

Recommended: render one SQL file for today, then paste/run that file in Hue:

```bash
python3 iceberg_analytics/tools/render_manual_sql.py
```

By default this uses the current UTC date and writes one file here:

```text
iceberg_analytics/sql/manual/rendered/<current-date>/00_run_all_in_hue.sql
```

Only pass `--business-date` when deliberately testing a historical date.

The raw files under `templates/` contain these placeholders:

- `__BUSINESS_DATE__`
- `__CURATED_S3_ROOT__`, for example `s3a://porscheenv-buk-3b2308dc/data/porsche/curated/vehicle-health`
- `__ANALYTICS_DATABASE__`, for example `porsche_vehicle_health_analytics`
- `__STAGING_DATABASE__`, for example `porsche_vehicle_health_analytics_staging`

Simple run order:

1. Run the existing CDE Spark job for the current date so Parquet exists.
2. Run `python3 iceberg_analytics/tools/render_manual_sql.py`.
3. Open `00_run_all_in_hue.sql` in Hue.
4. Run it in the Impala editor.

The normal production path is the Airflow DAG in `iceberg_analytics/dags/`; this manual path is only to prove the SQL in Hue first.
