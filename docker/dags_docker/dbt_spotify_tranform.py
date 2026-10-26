from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Rutas DENTRO del contenedor del scheduler (no de tu Mac)
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"   # ejecutable dentro del venv que creó el Dockerfile
DBT_DIR = "/opt/airflow/dbt"                  # destino del volumen ./docker_dbt

DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,              # dónde está profiles.yml
    "DBT_TARGET_PATH": "/tmp/dbt_target",     # artefactos fuera del repo
    "DBT_LOG_PATH": "/tmp/dbt_logs",          # logs fuera del repo
}


def dbt_task(task_id: str, comando: str) -> BashOperator:

    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} {comando}",
        env=DBT_ENV,
        append_env=True,
    )


with DAG(
    dag_id="dbt_spotify_transform",
    schedule=None,
    start_date=datetime(2026, 9, 9),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["dbt", "spotify"],
    default_args={
        "owner": "axel",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
) as dag:

    run_staging = dbt_task("dbt_run_staging", "run --select path:models/staging")
    test_staging = dbt_task("dbt_test_staging", "test --select path:models/staging")
    run_intermediate = dbt_task("dbt_run_intermediate", "run --select path:models/intermediate")
    test_intermediate = dbt_task("dbt_test_intermediate", "test --select path:models/intermediate")

    run_staging >> test_staging >> run_intermediate >> test_intermediate