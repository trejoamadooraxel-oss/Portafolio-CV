import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.decorators import task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.models import DagRun, XCom
from airflow.utils.session import provide_session



def verificar_parametros(flag_name, **kwargs):
    return kwargs['params'].get(flag_name) is True


def notificar_error_pipeline(context):
    import requests

    task_id = context['task_instance'].task_id
    run_id = context['task_instance'].run_id
    error_detectado = context['task_instance'].error

    mensaje = (
        f"*¡EL PIPELINE DE SPOTIFY SE HA CAÍDO!*"
        f"• *Tarea afectada:* `{task_id}`\n"
        f"• *ID de Ejecución:* `{run_id}`\n"
        f"• *Error de Python:* `{error_detectado}`\n"
        f"• _Por favor, revisa los contenedores de Docker cuanto antes._"
    )

    logging.error(f"ERROR: FATAL ERROR DURANTE EL ETL DE SPOTIFY{mensaje}")




default_args = {
    'owner': 'axel',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=5),
    'on_failure_callback': notificar_error_pipeline,
}

'''
NOTE:
    Para configurar la configuracion de la tarea:
    con schedule:,
        schedule="@hourly" Se ejecuta automáticamente cada hora.
        schedule="@daily" Se ejecuta diario a la media noche
        schedule="@weekly" Se ejecuta automaticamente a la semana

        Conf Personalizada.

         ┌───────────── Minuto (0 - 59)
         │ ┌─────────── Hora (0 - 23)
         │ │ ┌───────── Día del mes (1 - 31)
         │ │ │ ┌─────── Mes del año (1 - 12)
         │ │ │ │ ┌───── Día de la semana (0 - 6 o 1 - 7) (1 = Lunes)
         │ │ │ │ │
         * * * * *

        schedule="* * * * *"
        schedule="0 8 1 1 *"         

'''
with DAG(
        dag_id="spotify_etl_process_main",
        default_args=default_args,
        schedule="@daily",
        start_date=datetime(2026, 9, 9),
        catchup=False,
        tags=["spotify", "etl"],
        params={
            "nombre_artista": Param(default="Bad Bunny", type="string",
                                    description="Nombre del artista a buscar en Spotify", ),
            "proceso_artista": Param(default=True, type="boolean",
                                     description="Realiza el proceso para la tabla artista crear, extraer y subir inf."),
            "proceso_album": Param(default=True, type="boolean",
                                   description="Realiza el proceso para la tabla album crear, extraer y subir inf."),
            "proceso_canciones": Param(default=True, type="boolean",
                                       description="Realiza el proceso para la tabla canciones crear, extraer y subir inf."),
            "proceso_mas_informacion": Param(default=True, type="boolean",
                                             description="Realiza el proceso para la tabla mas_inf crear, extraer y subir inf."),
            "proceso_historico": Param(default=True, type="boolean",
                                       description="Realiza el proceso de subir informacion historica.")
        },
) as dag:
    condicion_proceso_artista = ShortCircuitOperator(
        task_id="condicion_proceso_artista",
        python_callable=verificar_parametros,
        op_kwargs={"flag_name": "proceso_artista"},
    )

    condicion_proceso_album = ShortCircuitOperator(
        task_id="condicion_proceso_album",
        python_callable=verificar_parametros,
        op_kwargs={"flag_name": "proceso_album"},
    )

    condicion_proceso_canciones = ShortCircuitOperator(
        task_id="condicion_proceso_canciones",
        python_callable=verificar_parametros,
        op_kwargs={"flag_name": "proceso_canciones"},
    )

    condicion_mas_inf = ShortCircuitOperator(
        task_id="condicion_mas_informacion",
        python_callable=verificar_parametros,
        op_kwargs={"flag_name": "proceso_mas_informacion"},
    )

    condicion_proceso_historico = ShortCircuitOperator(
        task_id="condicion_proceso_historico",
        python_callable=verificar_parametros,
        op_kwargs={"flag_name": "proceso_historico"},
    )

    start_process = BashOperator(
        task_id="inicializador",
        bash_command='echo "Iniciando pipeline de Spotify..."',
    )

    esperar_5_seg = BashOperator(
        task_id="tiempo_espera_5s",
        bash_command="sleep 5",
    )

    disparar_dbt = TriggerDagRunOperator(
        task_id="disparar_dbt_spotify",
        trigger_dag_id="dbt_spotify_transform",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    disparar_etl_artista = TriggerDagRunOperator(
        task_id="etl_artista_spotify",
        trigger_dag_id="spotify_etl_process_artist",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
        conf={"artista_a_procesar": "{{ params.nombre_artista }}"},
    )


    @task
    @provide_session
    def preparar_configuracion_album(nombre_artista, session=None, **kwargs):
        from airflow.models import DagRun

        # 1. Buscamos las ejecuciones del DAG de artistas
        last_artist_runs = DagRun.find(dag_id='spotify_etl_process_artist', session=session)
        id_spotify = None

        if last_artist_runs:
            # Obtenemos la corrida más reciente (la última en el historial)
            target_run = last_artist_runs[-1]

            # 2. Consultamos usando la API nativa de Airflow que deserializa el string automáticamente
            id_spotify = XCom.get_one(
                dag_id='spotify_etl_process_artist',
                task_id='artista.subir_inf_postgres',  # ID completo real en base de datos
                key='return_value',
                run_id=target_run.run_id,
                session=session
            )

        logging.info(f"--- ID RECUPERADO DE LA BD DE AIRFLOW: {id_spotify} ---")

        return {
            "artista_id_spotify": str(id_spotify) if id_spotify else None,
            "artista_nombre": nombre_artista
        }

    config_dinamica = preparar_configuracion_album(nombre_artista="{{ params.nombre_artista }}")

    disparar_etl_album = TriggerDagRunOperator(
        task_id="etl_album_spotify",
        trigger_dag_id="spotify_etl_process_album",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
        conf=config_dinamica,
    )

    start_process >> condicion_proceso_artista >> disparar_etl_artista >> config_dinamica >> disparar_etl_album





