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

#Verify Conect Big Querry
import Spotify_api.Conexiones.etl_conect_big_querry as gcp

#Postgres
from Spotify_api.Models import Sync_Track
from Spotify_api.Esquemas.esquemas import TrackSchema

#Esquemas
import Spotify_api.Esquemas.etl_esquemas_big_querry as esquemas

#Extraer Inf
import Spotify_api.Conexiones.conection_api as spotify_api
import Spotify_api.Spotify_Api.extract_inf_api as extract_inf

#Tratamiento y Transformacion
from Spotify_api.Transforms.pipeline_transform import DataPipeline


def verify_conection_spotify():
    try:
        sp = spotify_api.conection_spotify()
        sp.search(q="test", type="artist", limit=1)
        logging.info(f"OK. La conexion con Spotify API a sido Exitosa.")
        return True
    except Exception as e:
        logging.error(f"ERROR. La conexion con Spotify API a falaldo: {e}.")
        return False

def verificar_conexion_bigquery(**context):
    return gcp.verificar_conexion_bigquery()

with DAG(
    dag_id="spotify_etl_process_canciones",
    schedule=None,
    start_date=datetime(2026, 9, 9),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["spotify", "process", "canciones","Axel T", "26-OCT-26"],
    default_args={
        "owner": '05:05',
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
) as dag:

    start_process = BashOperator(
        task_id="Inicializador_Canciones",
        bash_command='echo "Iniciando Procesos de Extraccion:"',
    )

    time_sleep_10 = BashOperator(
        task_id="tiempo_espera_10s",
        bash_command="sleep 10",
    )

    verify_spotify_api_conect = PythonSensor(
        task_id="verify_spotify_api_conect",
        python_callable=verify_conection_spotify,
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    verify_big_querry_conect = PythonOperator(
        task_id="verify_big_querry_conect",
        python_callable=verificar_conexion_bigquery
    )

    @task
    def extraer_tacks(list_albums):

        #nombre_artista = 'Don Omar'#kwargs["params"]["nombre_artista"]
        sp = spotify_api.conection_spotify()
        dicc_track = extract_inf.list_tracks(sp, list_albums)
        logging.info(f"OK. Canciones extraidas: {dicc_track}")

        return dicc_track

    @task
    def create_tabla_canciones():
        try:
            Sync_Track.create_table()
            logging.info(f"OK. Se creo la Tabla.")
        except Exception as e:
            logging.error(f"ERROR. No se creo ni se encontro la tabla : {e}")


    @task
    def tratamiento_de_datos(list_dic, tabla):
        pipeline = DataPipeline()
        df_limpio = pipeline._transformar_datos(list_dic)
        #registros_postgres = pipeline._Postgres_data(df_limpio, tabla)
        df_big_query = pipeline._Big_Query_data(df_limpio, tabla)
        return df_big_query


    @task
    def subir_inf_postgres(lista_registros, project_id):

        logging.info(f'El Proyecto ID es: {project_id}')

        if lista_registros:
            # PostgresSQL
            Sync_Track.insert_to_table(lista_registros)
            logging.info(f"OK. Se ingreso la informacion en la tabla")

        else:
            mensaje = f"ERROR. La lista de registros para Postgres esta vacia: {lista_registros}."
            logging.error(mensaje)
            raise ValueError(mensaje)


    @task
    def subir_inf_big_query(lista_registros, project_id, tabla, dataset):

        logging.info(f'El Proyecto ID es: {project_id}')

        try:
            # BigQurry
            cliente_gcp = gcp.get_bq_client(project_id)
            gcp.cargar_dataframe(cliente_gcp, lista_registros, dataset, tabla, esquemas.SCHEMAS_BQ[tabla])
            logging.info(f"OK. Se ingreso la informacion en la tabla {tabla} de Big Querry")

        except Exception as e:
            mensaje = f"ERROR. Error al subir informacion a Big query: {str(e)}."
            logging.error(mensaje)
            raise ValueError(mensaje)


    @task_group(group_id="canciones")
    def procesar_canciones(cliente_project_id,tabla, dataset ):

        lista_albums = [{'id_spotify_artist': '3rWZHrfrsPBxVy692yAIxF', 'num_canciones': 10, 'url': {'spotify': 'https://open.spotify.com/album/6x9IdAz7Br6NnlK7BBJubL'}, 'id_spotify': '6x9IdAz7Br6NnlK7BBJubL', 'nombre_album': 'The Thread', 'tipo_album': 'album', 'fecha_precisa': 'day', 'fecha_lanzamiento': '2026-07-24', 'id_artista': 161}, {'id_spotify_artist': '3rWZHrfrsPBxVy692yAIxF', 'num_canciones': 12, 'url': {'spotify': 'https://open.spotify.com/album/055CZXcGINxmZKxwQ9hroP'}, 'id_spotify': '055CZXcGINxmZKxwQ9hroP', 'nombre_album': 'petal rock black', 'tipo_album': 'album', 'fecha_precisa': 'day', 'fecha_lanzamiento': '2026-02-17', 'id_artista': 161}]

        create_tabla_canciones()
        return_dicc_tracks = extraer_tacks(lista_albums)
        retorno_datos = tratamiento_de_datos(return_dicc_tracks, tabla)
        #subir_inf_postgres(retorno_datos,cliente_project_id)
        subir_inf_big_query(retorno_datos, cliente_project_id, tabla, dataset)
        return return_dicc_tracks


    xcom_canciones = procesar_canciones(verify_big_querry_conect.output,'track','Spotify_dataset')

    start_process >> verify_spotify_api_conect >> verify_big_querry_conect >> xcom_canciones
