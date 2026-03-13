from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from trino.auth import BasicAuthentication
from trino.dbapi import connect

PROJECT_DIR = "/opt/airflow/project"
DATA_DIR = f"{PROJECT_DIR}/data"
FETCH_SCRIPT = f"{PROJECT_DIR}/scripts/fetch_game_online.py"
GAMES_FILE = f"{PROJECT_DIR}/games.txt"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt_steam"
S3_BUCKET = "raw"
S3_PREFIX = "steam/raw/game_online/dt={{ ds }}/hour={{ execution_date.strftime('%H') }}"
FILE_NAME = "game_online_{{ ts_nodash }}.ndjson"
MOSCOW_TZ = pendulum.timezone("Europe/Moscow")


def _trino_connect():
    user = os.getenv("TRINO_USER", "ilya")
    password = os.getenv("TRINO_PASSWORD", "")
    password_file = os.getenv("TRINO_PASSWORD_FILE", "")
    if password_file:
        password = Path(password_file).read_text(encoding="utf-8").strip()
    if not password:
        raise RuntimeError("TRINO password is not configured")
    connect_timeout = float(os.getenv("TRINO_CONNECT_TIMEOUT", "3"))
    request_timeout = float(os.getenv("TRINO_REQUEST_TIMEOUT", "20"))
    max_attempts = int(os.getenv("TRINO_MAX_ATTEMPTS", "2"))
    return connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8443")),
        user=user,
        auth=BasicAuthentication(user, password),
        catalog="hive",
        schema="raw",
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "https"),
        verify=os.getenv("TRINO_VERIFY", "false").lower() == "true",
        request_timeout=(connect_timeout, request_timeout),
        max_attempts=max_attempts,
    )


def sync_landing_game_online_partitions() -> None:
    conn = _trino_connect()
    cur = conn.cursor()
    cur.execute("CALL system.sync_partition_metadata('raw', 'game_online_files', 'ADD')")
    cur.fetchall()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=2),
}


with DAG(
    dag_id="steam_game_online_every_20m",
    start_date=pendulum.datetime(2024, 1, 1, tz=MOSCOW_TZ),
    schedule_interval="*/20 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    ensure_dirs = BashOperator(
        task_id="ensure_dirs",
        bash_command=f"mkdir -p {DATA_DIR}",
    )

    fetch_online = BashOperator(
        task_id="fetch_online",
        env={"HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": ""},
        bash_command=(
            f"python {FETCH_SCRIPT} --games-file {GAMES_FILE} "
            f"--output {DATA_DIR}/{FILE_NAME} "
            f"--meta-source-file s3://{S3_BUCKET}/{S3_PREFIX}/{FILE_NAME} "
            "--meta-ingested-at '{{ ts }}' "
            "--meta-dt '{{ ds }}' "
            "--meta-hour '{{ execution_date.strftime(\"%H\") }}'"
        ),
    )

    upload_minio = BashOperator(
        task_id="upload_minio",
        bash_command=(
            f"test -s {DATA_DIR}/{FILE_NAME} && "
            "(aws s3 mb s3://raw --endpoint-url http://minio:9000 || true) && "
            "aws s3 cp "
            f"{DATA_DIR}/{FILE_NAME} "
            f"s3://{S3_BUCKET}/{S3_PREFIX}/{FILE_NAME} "
            "--endpoint-url http://minio:9000 --no-progress"
        ),
    )

    sync_landing_partitions = PythonOperator(
        task_id="sync_landing_partitions",
        python_callable=sync_landing_game_online_partitions,
    )

    load_stg_trino = BashOperator(
        task_id="load_stg_trino",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "export TRINO_PASSWORD=\"$(cat /run/secrets/airflow_admin_password)\" && "
            "dbt run --profiles-dir . --select stg_steam_game_online"
        ),
    )

    test_dbt_models = BashOperator(
        task_id="test_dbt_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "export TRINO_PASSWORD=\"$(cat /run/secrets/airflow_admin_password)\" && "
            "dbt test --profiles-dir . "
            "--select stg_steam_game_online"
        ),
    )

    ensure_dirs >> fetch_online >> upload_minio >> sync_landing_partitions >> load_stg_trino >> test_dbt_models
