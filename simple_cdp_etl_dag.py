"""Simple CDP Public Cloud ETL DAG.

This DAG expects a raw sales CSV to already exist in the configured data lake
path. It uses PythonOperator tasks to run a Spark cleanup job in CDE and run
Data Hub Impala SQL over the curated Parquet output.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql" / "impala"
IMPALA_SQL = SQL_DIR / "sales_summary.sql"


def as_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "y"}


def run_cde_spark_job(*, conn_id: str) -> None:
    """Run the existing CDE Spark job using the CDE CLI."""
    context = get_current_context()
    params = context["params"]
    airflow_conn = BaseHook.get_connection(conn_id)
    extra = airflow_conn.extra_dejson

    command = [
        extra.get("cde_binary", "cde"),
        "job",
        "run",
        "--name",
        params["cde_job_name"],
        "--wait",
        "--variable",
        f"raw_sales_path={params['raw_sales_path']}",
        "--variable",
        f"clean_sales_path={params['clean_sales_path']}",
    ]

    env = os.environ.copy()
    if airflow_conn.host:
        env["CDE_VCLUSTER_ENDPOINT"] = airflow_conn.host
    if airflow_conn.login:
        env["CDE_ACCESS_KEY_ID"] = airflow_conn.login
    if airflow_conn.password:
        env["CDE_ACCESS_KEY_SECRET"] = airflow_conn.password
    if extra.get("cdp_endpoint"):
        env["CDE_CDP_ENDPOINT"] = extra["cdp_endpoint"]
    if extra.get("config_profile"):
        env["CDE_CONFIG_PROFILE"] = extra["config_profile"]

    result = subprocess.run(command, capture_output=True, check=False, text=True, env=env)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise AirflowException(f"CDE Spark job failed with exit code {result.returncode}.")


def run_datahub_impala_sql(
    *,
    sql_file: str,
    impala_database: str,
    clean_sales_path: str,
    conn_id: str,
) -> None:
    """Render and run the Impala SQL file using impala-shell."""
    with open(sql_file, encoding="utf-8") as file:
        rendered_sql = (
            file.read()
            .replace("{{ params.impala_database }}", impala_database)
            .replace("{{ params.clean_sales_path }}", clean_sales_path)
        )

    airflow_conn = BaseHook.get_connection(conn_id)
    extra = airflow_conn.extra_dejson

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as temp_sql:
        temp_sql.write(rendered_sql)
        temp_sql_path = temp_sql.name

    command = [
        extra.get("impala_shell_binary", "impala-shell"),
        "-i",
        f"{airflow_conn.host}:{airflow_conn.port or 443}",
        "-f",
        temp_sql_path,
    ]

    if airflow_conn.login:
        command.extend(["-u", airflow_conn.login])
    if as_bool(extra.get("use_ssl"), True):
        command.append("--ssl")
    if as_bool(extra.get("use_http_transport"), True):
        command.extend(["--protocol", "hs2-http"])
    if extra.get("http_path"):
        command.extend(["--http_path", extra["http_path"]])
    if as_bool(extra.get("ldap"), True):
        command.append("-l")

    command.extend(extra.get("shell_args", []))

    env = os.environ.copy()
    if airflow_conn.password:
        env["IMPALA_PASSWORD"] = airflow_conn.password
        command.extend(["--ldap_password_cmd", 'printf %s "$IMPALA_PASSWORD"'])

    try:
        result = subprocess.run(command, capture_output=True, check=False, text=True, env=env)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode != 0:
            raise AirflowException(
                f"impala-shell failed with exit code {result.returncode}."
            )
    finally:
        Path(temp_sql_path).unlink(missing_ok=True)


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="simple_cdp_sales_etl",
    description="Simple CDP ETL using PythonOperator, CDE Spark, and Data Hub Impala.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    template_searchpath=[str(SQL_DIR)],
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
            "simple-cdp-sales-cleanup",
            type="string",
            description="Existing CDE Spark job that runs spark/jobs/clean_sales.py.",
        ),
        "cde_conn_id": Param(
            "sstcwocde",
            type="string",
            description="Airflow connection ID for the CDE Virtual Cluster.",
        ),
    },
    tags=["cdp", "cde", "spark", "impala", "example"],
) as dag:
    run_cde_spark_transform = PythonOperator(
        task_id="run_cde_spark_transform",
        python_callable=run_cde_spark_job,
        op_kwargs={
            "conn_id": "{{ params.cde_conn_id }}",
        },
    )

    run_impala_sql = PythonOperator(
        task_id="run_datahub_impala_sql",
        python_callable=run_datahub_impala_sql,
        op_kwargs={
            "sql_file": str(IMPALA_SQL),
            "impala_database": "{{ params.impala_database }}",
            "clean_sales_path": "{{ params.clean_sales_path }}",
            "conn_id": "datahub_impala",
        },
    )

    run_cde_spark_transform >> run_impala_sql
