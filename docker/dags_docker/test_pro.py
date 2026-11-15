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
from airflow.sensors.time_delta import TimeDeltaSensor

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

#nuevas
#Postgres
from Spotify_api.Models import Sync_Artist, Sync_Album, Sync_Queries, Sync_Track, Sync_Inf

#Esquemas
import Spotify_api.Esquemas.etl_esquemas_big_querry as esquemas

#Verify Conect Big Querry
import Spotify_api.Conexiones.etl_conect_big_querry as gcp

#Extraer Inf
import Spotify_api.Conexiones.conection_api as spotify_api
import Spotify_api.Spotify_Api.extract_inf_api as extract_inf

#Tratamiento y Transformacion
from Spotify_api.Transforms.pipeline_transform import DataPipeline
import Spotify_api.Conexiones.conection_playwright as con_playwright
from Spotify_api.Playwright.scrapp_spotify import extract_artist_inf

#Scrapping - Playwright


disparar_dbt = TriggerDagRunOperator(
    task_id="disparar_dbt_spotify",
    trigger_dag_id="dbt_spotify_transform",
    wait_for_completion=True,
    poke_interval=20,
    allowed_states=["success"],
    failed_states=["failed"],
)


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


def verify_conection_spotify():
    try:
        sp = spotify_api.conection_spotify()
        sp.search(q="test", type="artist", limit=1)
        logging.info(f"OK. La conexion con Spotify API a sido Exitosa")
        return True
    except Exception as e:
        logging.error(f"ERROR. La conexion con Spotify API a falaldo: {e}")
        return False


def verify_conection_playwright():
    conection_p = None
    try:
        import Spotify_api.Conexiones.conection_playwright as con_playwright
        conection_p = con_playwright.Conection_playwright(headless=True)
        page = conection_p.get_page()
        page.goto('https://www.google.com/', timeout=15000)
        logging.info(f"OK. La conexion con Playwright ha sido Exitosa")
        return True
    except Exception as e:
        logging.error(f"ERROR. La conexion con Playwright ha falaldo: {e}")
        return False
    finally:
        if conection_p:
            conection_p.close_browser()
            conection_p.close_conection_p()


def verificar_conexion_bigquery():
    return gcp.verificar_conexion_bigquery()

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
with (DAG(
        dag_id="spotify_etl_test",
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
            "proceso_dbt": Param(default=True, type="boolean",
                                       description="Realiza el proceso de Crear las vistas.")
        },
) as dag):
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

    condicion_proceso_dbt = ShortCircuitOperator(
        task_id="condicion_proceso_historico",
        python_callable=verificar_parametros,
        op_kwargs={"flag_name": "proceso_dbt"},
    )

    start_process = BashOperator(
        task_id="inicializador",
        bash_command='echo "Iniciando pipeline de Spotify..."',
    )

    esperar_un_momento = BashOperator(
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

    verify_playwright_conect = PythonSensor(
        task_id="verify_playwright_conect",
        python_callable=verify_conection_playwright,
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    verify_big_querry_conect = PythonOperator(
        task_id="verify_big_querry_conect",
        python_callable=verificar_conexion_bigquery
    )


    @task
    def create_tabla(tabla):
        try:
            if tabla == 'artista':
                Sync_Artist.create_table()

            if tabla == 'album':
                Sync_Album.create_table()

            if tabla == 'track':
                Sync_Track.create_table()

            if tabla == 'mas_inf':
                Sync_Inf.create_table()

            logging.info(f"OK. Se creo la tabla: {tabla} para PostgreSQL")

        except Exception as e:

            logging.error(f"ERROR. No se creo ni se encontro la tabla : {e}")


    @task(multiple_outputs=True)
    def extraer_informacion(tabla, id_artist: str = None, ids_albunes: list = None, **kwargs):

        nombre_artista = kwargs["params"]["nombre_artista"]
        sp = spotify_api.conection_spotify()

        if tabla == 'artista':
            id_spotify, artistas = extract_inf.identificador_artistas(sp, nombre_artista)
            logging.info(f"OK. Informacion de {tabla} extraído: {artistas}")

            return {
                "id_spotify": id_spotify,
                "datos_artista": artistas
            }

        if tabla == 'album':
            dic_albunes = extract_inf.list_albums(sp, id_artist, nombre_artista)
            logging.info(f"OK. Informacion de {tabla} extraído: {dic_albunes}")

            return {
                "nombre_artista": nombre_artista,
                "dic_albunes": dic_albunes
            }

        if tabla == 'track':
            dicc_track = extract_inf.list_tracks(sp, ids_albunes)
            logging.info(f"OK. Informacion de {tabla} extraído: {dicc_track}")
            return {
                "nombre_artista": nombre_artista,
                "dicc_canciones": dicc_track
            }

        if tabla == 'mas_inf':

            conection_p = None
            list_info = None
            try:
                list_artist = [nombre_artista]
                logging.info(f'{list_artist}')
                conection_p = con_playwright.Conection_playwright(headless=True)
                logging.info(f"OK. Iniciando proceso de extraccion mediante Playwrite")
                list_info = extract_artist_inf(conection_p, 'https://open.spotify.com/', list_artist, Sync_Artist)

            except Exception as e:
                logging.error(f"ERROR. La conexion con Playwright ha falaldo: {e}")

            finally:
                if conection_p:
                    conection_p.close_browser()
                    conection_p.close_conection_p()
            return {
                "nombre_artista": nombre_artista,
                "dicc_mas_inf": list_info
            }


    @task
    def tratamiento_de_datos(tabla, list_dic):
        pipeline = DataPipeline()
        df_limpio = pipeline._transformar_datos(list_dic)
        registros_postgres = pipeline._Postgres_data(tabla, df_limpio)
        df_big_query = pipeline._Big_Query_data(df_limpio, tabla)
        return {
            'registros_postgres': registros_postgres,
            'df_big_query':df_big_query
            }

    @task
    def subir_inf_postgres(tabla, lista_registros, dicc_id_spotify):

        logging.info(f'Lista de registros: {lista_registros["registros_postgres"]}')

        registros_by_postgres = lista_registros["registros_postgres"]
        id_spotify = None

        if lista_registros:
            if tabla == 'artista':
                Sync_Artist.insert_to_table(registros_by_postgres)
                id_spotify = dicc_id_spotify

            if tabla == 'album':
                Sync_Album.insert_to_table(registros_by_postgres)
                id_spotify = [registro['id_spotify'] for registro in dicc_id_spotify]

            if tabla == 'track':
                Sync_Track.insert_to_table(registros_by_postgres)
                id_spotify = [registro['id_spotify'] for registro in dicc_id_spotify]

            if tabla == 'mas_inf':
                Sync_Inf.insert_to_table(registros_by_postgres)

            logging.info(f"OK. Se ingreso la informacion en la {tabla}")

        else:
            mensaje = f"ERROR. La lista de registros para Postgres esta vacia: {registros_by_postgres}."
            logging.error(mensaje)
            raise ValueError(mensaje)

        return id_spotify

    @task
    def subir_inf_big_query(tabla, lista_registros, project_id, dataset):

        logging.info(f'El Proyecto ID es: {project_id}')
        logging.info(f'data frame: {lista_registros["df_big_query"]}')

        df_big_query = lista_registros["df_big_query"]

        try:
            # BigQurry
            cliente_gcp = gcp.get_bq_client(project_id)
            gcp.cargar_dataframe(cliente_gcp, df_big_query, dataset, tabla, esquemas.SCHEMAS_BQ[tabla])
            logging.info(f"OK. Se ingreso la informacion en la tabla {tabla} de Big Querry")

        except Exception as e:
            mensaje = f"ERROR. Error al subir informacion a Big query: {str(e)}."
            logging.error(mensaje)
            raise ValueError(mensaje)



    @task_group(group_id='Proceso_Artista')
    def proceso_artista(tabla='artista'):
        create_table = create_tabla(tabla)
        dicc_result = extraer_informacion(tabla)
        informacion_tabla = tratamiento_de_datos(tabla, dicc_result['datos_artista'])
        ids = subir_inf_postgres(tabla, informacion_tabla, dicc_result['id_spotify'])
        up_bq = subir_inf_big_query(tabla, informacion_tabla, verify_big_querry_conect.output,'Spotify_dataset')

        create_table >> dicc_result >> informacion_tabla

        informacion_tabla >> ids
        informacion_tabla >> up_bq

        return ids


    @task_group(group_id='Proceso_Album')
    def proceso_album(id_artist, tabla='album'):

        create_table = create_tabla(tabla)
        dicc_result = extraer_informacion(tabla, id_artist)
        informacion_tabla = tratamiento_de_datos(tabla, dicc_result['dic_albunes'])
        ids = subir_inf_postgres(tabla, informacion_tabla, dicc_result['dic_albunes'])
        up_bq = subir_inf_big_query(tabla, informacion_tabla, verify_big_querry_conect.output, 'Spotify_dataset')

        create_table >> dicc_result >> informacion_tabla

        informacion_tabla >> ids
        informacion_tabla >> up_bq


        return ids


    @task_group(group_id='Proceso_Canciones')
    def proceso_canciones(ids_albunes, tabla='track'):
        create_table = create_tabla(tabla)
        dicc_result = extraer_informacion(tabla, ids_albunes=ids_albunes)
        informacion_tabla = tratamiento_de_datos(tabla, dicc_result['dicc_canciones'])
        ids = subir_inf_postgres(tabla, informacion_tabla,dicc_result['dicc_canciones'])
        up_bq = subir_inf_big_query(tabla, informacion_tabla, verify_big_querry_conect.output, 'Spotify_dataset')

        create_table >> dicc_result >> informacion_tabla

        informacion_tabla >> ids
        informacion_tabla >> up_bq

        return ids


    @task_group(group_id='Proceso_Mas_Info')
    def proceso_mas_inf(id_artist, tabla='mas_inf'):
        create_table = create_tabla(tabla)
        dicc_result = extraer_informacion(tabla, id_artist)
        informacion_tabla = tratamiento_de_datos(tabla, dicc_result['dicc_mas_inf'])
        ids = subir_inf_postgres(tabla, informacion_tabla, dicc_result['dicc_mas_inf'])
        up_bq = subir_inf_big_query(tabla, informacion_tabla, verify_big_querry_conect.output, 'Spotify_dataset')

        create_table >> dicc_result >> informacion_tabla

        informacion_tabla >> ids
        informacion_tabla >> up_bq

        return ids


    '''@task_group(group_id='Creacion_tablas')
    def ccreate_tables():
        create_tabla('artista')
        create_tabla('album')
        create_tabla('track')

    create_tablas = ccreate_tables()
    '''

    xcom_artista = proceso_artista()
    xcom_album = proceso_album(xcom_artista)
    xcom_canciones = proceso_canciones(xcom_album)
    xcom_mas_inf = proceso_mas_inf(xcom_artista)

    start_process >> verify_spotify_api_conect >> verify_big_querry_conect

    verify_big_querry_conect \
    >> condicion_proceso_artista >> xcom_artista >> esperar_un_momento \
    >> condicion_proceso_album >> xcom_album \
    >> condicion_proceso_canciones >> xcom_canciones \
    >> condicion_mas_inf >> xcom_mas_inf \
    >> disparar_dbt

