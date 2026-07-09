from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "QuanDo",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "03_intraday_biometric_pipeline",
    default_args=default_args,
    description="Micro-batch ELT for near real-time stress dashboard",
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=['realtime', 'intraday']
) as dag:

    execute_near_realtime_dbt = BashOperator(
        task_id="execute_realtime_dbt",
        bash_command=(
            "cd /opt/airflow/dbt_project/transform/dbt_lakehouse && dbt build --select gold_realtime_user_stress --profiles-dir . --no-partial-parse"
        ),
        execution_timeout=timedelta(minutes=5),
    )