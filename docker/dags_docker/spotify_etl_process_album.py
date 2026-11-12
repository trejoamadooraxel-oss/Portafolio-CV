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
    def extraer_album(nombre_artista ,id_artist):

        logging.info(f"OK. NOMBRE ARTISTA: {nombre_artista}")
        logging.info(f"OK. ID_ARTISTA: {id_artist}")

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
    def tratamiento_de_datos(list_dic, tabla):
        pipeline = DataPipeline()
        df_limpio = pipeline._transformar_datos(list_dic)
        registros_postgres = pipeline._Postgres_data(df_limpio, tabla)
        #df_big_query = pipeline._Big_Query_data(df_limpio, tabla)
        return registros_postgres

    @task
    def subir_inf_postgres(lista_registros):

        if lista_registros:
            # PostgresSQL
            Sync_Album.insert_to_table(lista_registros)
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


    @task(multiple_outputs=True)
    def obtener_parametros_orquestador(**kwargs):
        dag_run_conf = kwargs.get("dag_run").conf if kwargs.get("dag_run") else {}


        return {
            "nombre": dag_run_conf.get("artista_nombre", "Artista Por Defecto"),
            # Asegura que la clave coincida con el conf del orquestador
            "id_spotify": dag_run_conf.get("artista_id_spotify")
        }


    @task()
    def retorno_de_variables_canciones(list_dicc):
        return [registro['id_spotify'] for registro in list_dicc]

    '''
    @task_group(group_id="album")
    def procesar_artista(cliente_project_id,tabla, dataset):


        create_tabla_album()

        return_dicc_albums = extraer_album(id_artist=params_artista["id_spotify"],
                                            nombre_artista=params_artista["nombre"]
        )

        retorno_datos = tratamiento_de_datos(return_dicc_albums, tabla)
        subir_informacion = subir_inf_postgres(retorno_datos,cliente_project_id)
        #subir_inf_big_query(retorno_datos, cliente_project_id, tabla, dataset)
        return return_dicc_albums


    #xcom_albums = procesar_artista(verify_big_querry_conect.output,'album','Spotify_dataset')
    '''

    params_artista = obtener_parametros_orquestador()
    return_dicc_albums = extraer_album(id_artist=params_artista["id_spotify"],
                                       nombre_artista=params_artista["nombre"])
    retorno_datos = tratamiento_de_datos(return_dicc_albums, 'album')
    subir_info_postgres = subir_inf_postgres(retorno_datos)
    #subir_info_bq = subir_inf_big_query(retorno_datos, verify_big_querry_conect.output,'album', 'Spotify_dataset')
    varaibles_canciones =  retorno_de_variables_canciones(retorno_datos)



    #Flujos
    start_process >> verify_spotify_api_conect  >> create_tabla_album() >> params_artista >> return_dicc_albums >> retorno_datos
    retorno_datos >> subir_info_postgres
    retorno_datos >> varaibles_canciones

    #retorno_datos >> verify_big_querry_conect >> subir_info_bq




    #

