from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "QuanDo",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "02_daily_biometric_pipeline",
    default_args=default_args,
    description="Daily ELT pipeline for biometric Silver and Gold models",
    schedule="5 0 * * *",
    catchup=False,
    tags=['production', 'daily', 'marts']
) as dag:

    log_pipeline_start = BashOperator(
        task_id="log_pipeline_start",
        bash_command='echo "[AUDIT LOG] Starting daily biometric pipeline at $(date)"',
    )

    execute_dbt_build = BashOperator(
        task_id="execute_dbt_build",
        bash_command="cd /opt/airflow/dbt_project/transform/dbt_lakehouse && dbt build",
    )

    log_pipeline_success = BashOperator(
        task_id="log_pipeline_success",
        bash_command='echo "[AUDIT LOG] Pipeline completed successfully. Gold tables are ready for Power BI."',
    )

    log_pipeline_start >> execute_dbt_build >> log_pipeline_success