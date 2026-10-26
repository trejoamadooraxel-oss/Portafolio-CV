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
from Spotify_api.Models import Sync_Album
from Spotify_api.Esquemas.esquemas import AlbumSchema

#Esquemas
import Spotify_api.Esquemas.etl_esquemas_big_querry as esquemas

#Extraer Inf
import Spotify_api.Conexiones.conection_api as spotify_api
import Spotify_api.Spotify_Api.extract_inf_api as extract_inf


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
    dag_id="spotify_etl_process_album",
    schedule=None,
    start_date=datetime(2026, 9, 9),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["spotify", "process", "album","Axel T", "26-OCT-26"],
    default_args={
        "owner": '05:05',
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
) as dag:

    start_process = BashOperator(
        task_id="Inicializador_Album",
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
    def extraer_album(id_artist,**kwargs):

        nombre_artista = 'Don Omar'#kwargs["params"]["nombre_artista"]
        sp = spotify_api.conection_spotify()
        dic_albunes = extract_inf.list_albums(sp, id_artist, nombre_artista)
        logging.info(f"OK. Albunes extraídos: {dic_albunes}")

        return dic_albunes

    @task
    def create_tabla_album():
        try:
            Sync_Album.create_table()
            logging.info(f"OK. Se creo la Tabla.")
        except Exception as e:
            logging.error(f"ERROR. No se creo ni se encontro la tabla : {e}")


    @task
    def subir_inf_artista(list_dic, project_id,tabla, dataset ):

        logging.info(f'El Proyecto ID es: {project_id}')
        lista_registros = []
        for dic in list_dic:
            try:
                registro_validado = AlbumSchema(**dic)
                lista_registros.append(registro_validado.model_dump())
            except Exception as e:
                logging.warning(f"Warning. No se pudo validar la informacion de {dic}, {e}")

        if lista_registros:
            #PostgresSQL
            Sync_Album.insert_to_table(lista_registros)
            logging.info(f"OK. Se ingreso la informacion en la tabla")

            #BigQurry
            cliente_gcp = gcp.get_bq_client(project_id)
            gcp.cargar_dataframe(cliente_gcp, lista_registros, dataset, tabla, esquemas.SCHEMAS_BQ[tabla])
            logging.info(f"OK. Se ingreso la informacion en la tabla {tabla} de Big Querry")

        else:
            mensaje = f"ERROR. La lista de registros esta vacia: {lista_registros}."
            logging.error(mensaje)
            raise ValueError(mensaje)

    @task_group(group_id="artista")
    def procesar_artista(cliente_project_id,tabla, dataset ):
        create_tabla_album()
        return_dicc_albums = extraer_album(id_artist='33ScadVnbm2X8kkUqOkC6Z')
        #return_dicc_albums = extraer_album(id_artist=id_spotify)
        subir_inf_artista(return_dicc_albums,cliente_project_id,tabla, dataset)
        return return_dicc_albums


    xcom_albums = procesar_artista(verify_big_querry_conect.output,'album','Spotify_dataset')

    start_process >> verify_spotify_api_conect >> verify_big_querry_conect >> xcom_albums
