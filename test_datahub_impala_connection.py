"""Small DAG to test the Data Hub Impala Airflow connection."""

from __future__ import annotations

import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


def as_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "y"}


def test_impala_connection(conn_id: str = "datahub_impala") -> None:
    """Run SELECT 1 through impala-shell using an Airflow Impala connection."""
    airflow_conn = BaseHook.get_connection(conn_id)
    extra = airflow_conn.extra_dejson

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as temp_sql:
        temp_sql.write("SELECT 1 AS connection_test;\n")
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
                f"impala-shell connection test failed with exit code {result.returncode}."
            )
    finally:
        Path(temp_sql_path).unlink(missing_ok=True)


with DAG(
    dag_id="test_datahub_impala_connection",
    description="Test Data Hub Impala connectivity with SELECT 1.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["datahub", "impala", "test"],
) as dag:
    PythonOperator(
        task_id="select_1",
        python_callable=test_impala_connection,
    )
