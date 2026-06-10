"""Simple CDP Public Cloud ETL DAG.

This DAG expects a raw sales CSV to already exist in the configured data lake
path. It runs a CDE Spark job and then uses the Airflow SQL operator to run
Data Hub Impala SQL over the curated Parquet output.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

sales_summary_sql = """
CREATE DATABASE IF NOT EXISTS {{ params.impala_database }};

CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.impala_database }}.clean_sales (
    order_id STRING,
    customer_id STRING,
    product_name STRING,
    quantity INT,
    unit_price DOUBLE,
    total_amount DOUBLE
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION '{{ params.clean_sales_path }}';

INVALIDATE METADATA {{ params.impala_database }}.clean_sales;

ALTER TABLE {{ params.impala_database }}.clean_sales RECOVER PARTITIONS;

DROP VIEW IF EXISTS {{ params.impala_database }}.sales_revenue_by_order_date;

CREATE VIEW {{ params.impala_database }}.sales_revenue_by_order_date AS
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(quantity) AS units_sold,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM {{ params.impala_database }}.clean_sales
GROUP BY order_date;
"""


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="simple_cdp_sales_etl",
    description="Simple CDP ETL using CDE and SQL operators.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    params={
        "raw_sales_path": Param(
            "s3a://sstsandbox-buk-9f833afb/data/raw/",
            type="string",
            description="S3/data lake folder containing the raw sales CSV file.",
        ),
        "clean_sales_path": Param(
            "s3a://sstsandbox-buk-9f833afb/data/curated/sales_clean",
            type="string",
            description="S3/data lake path where Spark writes clean Parquet output.",
        ),
        "impala_database": Param(
            "retail_demo",
            type="string",
            description="Data Hub Impala database used by the SQL step.",
        ),
        "cde_job_name": Param(
            "simple-cdp-sales-etl",
            type="string",
            description="Existing CDE Spark job that runs spark/jobs/clean_sales.py.",
        ),
    },
    tags=["cdp", "cde", "spark", "impala", "example"],
) as dag:
    run_cde_spark_transform = CdeRunJobOperator(
        task_id="run_cde_spark_transform",
        connection_id="sstcwocde",
        job_name="{{ params.cde_job_name }}",
        variables={
            "raw_sales_path": "{{ params.raw_sales_path }}",
            "clean_sales_path": "{{ params.clean_sales_path }}",
        },
        wait=True,
    )

    run_impala_sql = SQLExecuteQueryOperator(
        task_id="run_datahub_impala_sql",
        conn_id="datahub_impala",
        sql=sales_summary_sql,
        split_statements=True,
        return_last=False,
    )

    run_cde_spark_transform >> run_impala_sql
