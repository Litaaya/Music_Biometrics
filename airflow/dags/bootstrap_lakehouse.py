from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "QuanDo",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "01_bootstrap_lakehouse",
    default_args=default_args,
    description="Initialize Lakehouse base data and dbt staging models",
    schedule_interval="@once",
    catchup=False,
    tags=["bootstrap", "lakehouse"],
) as dag:

    run_bootstrap_loader = BashOperator(
        task_id="run_bootstrap_loader",
        bash_command='cd /opt/airflow/dbt_project && export PYTHONPATH=. && python src/loader/main.py',
    )

    dbt_debug_check = BashOperator(
        task_id="dbt_debug_check",
        bash_command="cd /opt/airflow/dbt_project/transform/dbt_lakehouse && dbt debug --profiles-dir . || true",
    )

    dbt_build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command="cd /opt/airflow/dbt_project/transform/dbt_lakehouse && dbt build --select staging --profiles-dir . --no-partial-parse",
    )

    run_bootstrap_loader >> dbt_debug_check >> dbt_build_staging