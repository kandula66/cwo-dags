from datetime import datetime
from airflow import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

with DAG(
    dag_id="airflow_test_cde_dag",
    schedule=None,   # manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cde", "test"],
) as dag:

    run_airflow_test_job = CdeRunJobOperator(
        task_id="run_airflow_test",
        job_name="airflow_test",
        connection_id="sstcwocde",
        wait_for_completion=True
    )
