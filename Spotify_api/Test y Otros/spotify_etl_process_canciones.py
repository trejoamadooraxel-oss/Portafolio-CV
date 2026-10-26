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
    def subir_inf_artista(list_dic, project_id,tabla, dataset ):

        logging.info(f'El Proyecto ID es: {project_id}')
        lista_registros = []
        for dic in list_dic:
            try:
                registro_validado = TrackSchema(**dic)
                lista_registros.append(registro_validado.model_dump())
            except Exception as e:
                logging.warning(f"Warning. No se pudo validar la informacion de {dic}, {e}")

        if lista_registros:
            print(lista_registros)
            #BigQurry
            cliente_gcp = gcp.get_bq_client(project_id)
            gcp.cargar_dataframe(cliente_gcp, lista_registros, dataset, tabla, esquemas.SCHEMAS_BQ[tabla])
            logging.info(f"OK. Se ingreso la informacion en la tabla {tabla} de Big Querry")

            lista_canciones_limpia = [
                {k: v for k, v in cancion.items() if k != 'id_album_spotify'}
                for cancion in lista_registros
            ]

            #PostgresSQL
            Sync_Track.insert_to_table(lista_canciones_limpia)
            logging.info(f"OK. Se ingreso la informacion en la tabla")

        else:
            mensaje = f"ERROR. La lista de registros esta vacia: {lista_registros}."
            logging.error(mensaje)
            raise ValueError(mensaje)

    @task_group(group_id="artista")
    def procesar_canciones(cliente_project_id,tabla, dataset ):

        lista_albums = [
                        {'nombre_album': 'DeBÍ TiRAR MáS FOToS', 'id_spotify': '5K79FLRUCSysQnVESLcTdb', 'num_canciones': 17, 'id_artista': 37},
                        {'nombre_album': 'nadie sabe lo que va a pasar mañana', 'id_spotify': '4FftCsAcXXD1nFO9RFUNFO', 'num_canciones': 22, 'id_artista': 37},
                        {'nombre_album': 'Un Verano Sin Ti', 'id_spotify': '3RQQmkQEvNCY4prGKE6oc5', 'num_canciones': 23, 'id_artista': 37},
                        {'nombre_album': 'EL ÚLTIMO TOUR DEL MUNDO', 'id_spotify': '2d9BCZeAAhiZWPpbX9aPCW', 'num_canciones': 16, 'id_artista': 37},
                        {'nombre_album': 'LAS QUE NO IBAN A SALIR', 'id_spotify': '4gvQO5mEuhbMSrLIuwXkmz', 'num_canciones': 10, 'id_artista': 37},
                        {'nombre_album': 'YHLQMDLG', 'id_spotify': '5lJqux7orBlA1QzyiBGti1', 'num_canciones': 20, 'id_artista': 37},
                        {'nombre_album': 'OASIS', 'id_spotify': '6ylFfzx32ICw4L1A7YWNLN', 'num_canciones': 8, 'id_artista': 37},
                        {'nombre_album': 'X 100PRE', 'id_spotify': '7CjJb2mikwAWA1V6kewFBF', 'num_canciones': 15, 'id_artista': 37}
                    ]
        create_tabla_canciones()
        return_dicc_tracks = extraer_tacks(lista_albums)
        subir_inf_artista(return_dicc_tracks,cliente_project_id,tabla, dataset)
        return return_dicc_tracks


    xcom_canciones = procesar_canciones(verify_big_querry_conect.output,'track','Spotify_dataset')

    start_process >> verify_spotify_api_conect >> verify_big_querry_conect >> xcom_canciones
