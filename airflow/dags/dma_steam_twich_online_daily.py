from __future__ import annotations

import datetime as dt

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

PROJECT_DIR = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt_steam"
MOSCOW_TZ = pendulum.timezone("Europe/Moscow")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


def ods_logical_date(current_logical_date: dt.datetime, **_: object) -> dt.datetime:
    current_moscow = pendulum.instance(current_logical_date).in_timezone(MOSCOW_TZ)
    target_day = current_moscow.date()

    # Manual runs after 03:00 Moscow should wait for that day's scheduled ODS run.
    if current_moscow.time() < dt.time(hour=3):
        target_day -= dt.timedelta(days=1)

    return pendulum.datetime(
        target_day.year,
        target_day.month,
        target_day.day,
        3,
        0,
        0,
        tz=MOSCOW_TZ,
    )


with DAG(
    dag_id="steam_dma_twich_online_daily",
    start_date=pendulum.datetime(2024, 1, 1, tz=MOSCOW_TZ),
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    wait_for_ods = ExternalTaskSensor(
        task_id="wait_for_ods",
        external_dag_id="steam_ods_daily",
        external_task_id="test_ods_models",
        execution_date_fn=ods_logical_date,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 6,
    )

    run_dma_model = BashOperator(
        task_id="run_dma_steam_twich_online",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "export TRINO_PASSWORD=\"$(cat /run/secrets/airflow_admin_password)\" && "
            "dbt run --profiles-dir . --select dma_steam_twich_online"
        ),
    )

    test_dma_model = BashOperator(
        task_id="test_dma_steam_twich_online",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "export TRINO_PASSWORD=\"$(cat /run/secrets/airflow_admin_password)\" && "
            "dbt test --profiles-dir . --select dma_steam_twich_online"
        ),
    )

    wait_for_ods >> run_dma_model >> test_dma_model
