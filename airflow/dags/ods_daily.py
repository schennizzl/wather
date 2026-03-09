from __future__ import annotations

import datetime as dt

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt_steam"
MOSCOW_TZ = pendulum.timezone("Europe/Moscow")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


with DAG(
    dag_id="steam_ods_daily",
    start_date=pendulum.datetime(2024, 1, 1, tz=MOSCOW_TZ),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    run_ods_models = BashOperator(
        task_id="run_ods_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "export TRINO_PASSWORD=\"$(cat /run/secrets/airflow_admin_password)\" && "
            "dbt run --profiles-dir . --select path:models/ods"
        ),
    )

    test_ods_models = BashOperator(
        task_id="test_ods_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "export TRINO_PASSWORD=\"$(cat /run/secrets/airflow_admin_password)\" && "
            "dbt test --profiles-dir . --select path:models/ods"
        ),
    )

    run_ods_models >> test_ods_models
