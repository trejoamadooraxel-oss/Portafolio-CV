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
from Spotify_api.Models import Sync_Artist


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
    dag_id="spotify_etl_process_artist",
    schedule=None,
    start_date=datetime(2026, 9, 9),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["spotify", "process", "artist","Axel T", "25-OCT-26"],
    default_args={
        "owner": '02:55',
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
) as dag:

    start_process = BashOperator(
        task_id="Inicializador_Artista",
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

    @task(multiple_outputs=True)
    def extraer_artista(**kwargs):

        dag_run_conf = kwargs.get("dag_run").conf if kwargs.get("dag_run") else {}
        nombre_artista = dag_run_conf.get("artista_a_procesar", "Artista Por Defecto")
        sp = spotify_api.conection_spotify()
        id_spotify, artistas = extract_inf.identificador_artistas(sp, nombre_artista)
        logging.info(f"OK. Artista extraído: {artistas}")

        return {
            "id_spotify": id_spotify,
            "datos_artista": artistas
        }

    @task
    def create_tabla_artista():
        try:
            Sync_Artist.create_table()
            logging.info(f"OK. Se creo la Tabla")
        except Exception as e:
            logging.error(f"ERROR. No se creo ni se encontro la tabla : {e}")

    @task
    def tratamiento_de_datos(list_dic,tabla):
        pipeline = DataPipeline()
        df_limpio = pipeline._transformar_datos(list_dic)
        registros_postgres = pipeline._Postgres_data(df_limpio, tabla)
        #df_big_query = pipeline._Big_Query_data(df_limpio, tabla)
        return registros_postgres

    @task(task_id="subir_inf_postgres")
    def subir_inf_postgres(lista_registros, project_id, id_artista ):

        logging.info(f'El Proyecto ID es: {project_id}')

        if lista_registros:
            #PostgresSQL
            Sync_Artist.insert_to_table(lista_registros)
            logging.info(f"OK. Se ingreso la informacion en la tabla")

        else:
            mensaje = f"ERROR. La lista de registros para Postgres esta vacia: {lista_registros}."
            logging.error(mensaje)
            raise ValueError(mensaje)

        return id_artista

    @task
    def subir_inf_big_query(lista_registros, project_id,tabla, dataset ):

        logging.info(f'El Proyecto ID es: {project_id}')

        try:
            #BigQurry
            cliente_gcp = gcp.get_bq_client(project_id)
            gcp.cargar_dataframe(cliente_gcp, lista_registros, dataset, tabla, esquemas.SCHEMAS_BQ[tabla])
            logging.info(f"OK. Se ingreso la informacion en la tabla {tabla} de Big Querry")

        except Exception as e:
            mensaje = f"ERROR. Error al subir informacion a Big query: {str(e)}."
            logging.error(mensaje)
            raise ValueError(mensaje)


    @task_group(group_id="artista")
    def procesar_artista(cliente_project_id,tabla, dataset ):
        create_tabla_artista()
        resultado_artista_xcom = extraer_artista()
        retorno_datos = tratamiento_de_datos(resultado_artista_xcom["datos_artista"], tabla)
        id_artists = subir_inf_postgres(retorno_datos,cliente_project_id, resultado_artista_xcom["id_spotify"])
        #subir_inf_big_query(retorno_datos, cliente_project_id, tabla, dataset)


    xcom_artista = procesar_artista(verify_big_querry_conect.output,'artista','Spotify_dataset')

    start_process >> verify_spotify_api_conect >> verify_big_querry_conect >> xcom_artista
