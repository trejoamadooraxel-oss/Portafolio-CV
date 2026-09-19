
import os
from datetime import datetime, timedelta
from multiprocessing.forkserver import connect_to_new_process

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.decorators import task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator

import logging

from wirerope.wire import descriptor_bind



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

def verify_conection_spotify():
    try:
        import Spotify_api.Conexiones.conection_api as spotify_api
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


def verificar_conexion_bigquery(**context):
    """
    Reproduce, paso a paso, la misma verificación que se hizo
    manualmente dentro del contenedor:
      1. Instanciar el cliente
      2. Confirmar el project_id
      3. Listar datasets existentes (llamada real a la API)
    """
    from google.cloud import bigquery

    logging.info(f"OK. Instanciando cliente de BigQuery...")
    client = bigquery.Client(project=os.environ['GCP_PROJECT_ID'])  # toma el proyecto de GOOGLE_APPLICATION_CREDENTIALS / ADC
    logging.info(f"OK. Cliente conectado al proyecto: {client.project}")

    logging.info(f"OK. Listando datasets existentes...")
    datasets = list(client.list_datasets())

    if datasets:
        logging.info(f"OK. Se encontraron {len(datasets)} dataset(s):")
        for ds in datasets:
            logging.info(f"OK.  - {ds.dataset_id}")
    else:
        logging.info(f"OK. Conexión exitosa. Aún no existen datasets en este proyecto.")

    logging.info(f"OK. Verificación de conexión a BigQuery completada sin errores.")

    # Deja el project_id disponible en XCom por si otra tarea lo necesita después
    return client.project


with DAG(
    dag_id="spotify_etl_pro",
    schedule="@daily",
    start_date=datetime(2026, 9, 9),
    catchup=False,
    tags=["spotify", "etl"],
    params={
        "nombre_artista": Param(default="Bad Bunny", type="string", description="Nombre del artista a buscar en Spotify",),
        "proceso_artista": Param(default=True, type="boolean", description="Realiza el proceso para la tabla artista crear, extraer y subir inf."),
        "proceso_album": Param(default=True, type="boolean", description="Realiza el proceso para la tabla album crear, extraer y subir inf."),
        "proceso_canciones": Param(default=True, type="boolean", description="Realiza el proceso para la tabla canciones crear, extraer y subir inf."),
        "proceso_mas_informacion": Param(default=True, type="boolean", description="Realiza el proceso para la tabla mas_inf crear, extraer y subir inf.")
    },
) as dag:

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

    start_process = BashOperator(
        task_id="inicializador",
        bash_command='echo "Iniciando pipeline de Spotify..."',
    )

    esperar_un_momento = BashOperator(
        task_id="tiempo_espera_30s",
        bash_command="sleep 30",
    )

    verify_spotify_api_conect = PythonSensor(
        task_id = "verify_spotify_api_conect",
        python_callable = verify_conection_spotify,
        poke_interval = 30,
        timeout = 300,
        mode = "reschedule",
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

    @task(multiple_outputs=True)
    def extraer_artista(**kwargs):
        import Spotify_api.Conexiones.conection_api as spotify_api
        import Spotify_api.Spotify_Api.extract_inf_api as extract_inf

        nombre_artista = kwargs["params"]["nombre_artista"]
        sp = spotify_api.conection_spotify()
        id_spotify, artistas = extract_inf.identificador_artistas(sp, nombre_artista)
        logging.info(f"OK. Artista extraído: {artistas}")

        return {
            "id_spotify": id_spotify,
            "datos_artista": artistas
        }

    @task
    def extraer_album(id_artist, **kwargs):
        import Spotify_api.Conexiones.conection_api as spotify_api
        import Spotify_api.Spotify_Api.extract_inf_api as extract_inf
        nombre_artista = kwargs["params"]["nombre_artista"]

        sp = spotify_api.conection_spotify()
        dic_albunes = extract_inf.list_albums(sp, id_artist, nombre_artista)
        logging.info(f"OK. Albunes extraídos: {dic_albunes}")

        return dic_albunes

    @task
    def extraer_tacks(list_albums):
        import Spotify_api.Conexiones.conection_api as spotify_api
        import Spotify_api.Spotify_Api.extract_inf_api as extract_inf

        sp = spotify_api.conection_spotify()
        dicc_track = extract_inf.list_tracks(sp, list_albums)
        logging.info(f"OK. Canciones extraidas: {dicc_track}")

        return dicc_track

    @task
    def extraer_mas_info():
        conection_p = None
        list_info = None
        try:
            import Spotify_api.Conexiones.conection_playwright as con_playwright
            from Spotify_api.Playwright.scrapp_spotify import extract_artist_inf
            from Spotify_api.Models import Sync_Queries, Sync_Artist


            list_artist = Sync_Queries.all_name_artist()
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

        return list_info

    @task
    def create_tabla_artista():
        from Spotify_api.Models import Sync_Artist
        try:
            Sync_Artist.create_table()
            logging.info(f"OK. Se creo la Tabla")
        except Exception as e:
          logging.error(f"ERROR. No se creo ni se encontro la tabla : {e}")

    @task
    def create_tabla_album():
        from Spotify_api.Models import Sync_Album
        try:
            Sync_Album.create_table()
            logging.info(f"OK. Se creo la Tabla.")
        except Exception as e:
            logging.error(f"ERROR. No se creo ni se encontro la tabla : {e}")

    @task
    def create_tabla_canciones():
        from Spotify_api.Models import Sync_Track
        try:
            Sync_Track.create_table()
            logging.info(f"OK. Se creo la Tabla.")
        except Exception as e:
            logging.error(f"ERROR. No se creo ni se encontro la tabla : {e}")

    @task
    def create_table_mas_inf():
        from Spotify_api.Models import Sync_Inf
        try:
            Sync_Inf.create_table()
            logging.info(f"OK. Se creo la Tabla.")
        except Exception as e:
            logging.error(f"ERRO. No se creo ni se encontro la tabla : {e}")

    @task
    def subir_inf_artista(dic_artist):
        from Spotify_api.Models import Sync_Artist
        try:
            Sync_Artist.insert_to_table(dic_artist)
            logging.info(f"OK. Se ingreso la informacion en la tabla")
        except Exception as e:
            logging.error(f"ERROR. Al subir la informacion : {e}")

    @task
    def subir_inf_albums(list_albunes):
        from Spotify_api.Models import Sync_Album
        try:
            Sync_Album.insert_to_table(list_albunes)
            logging.info(f"OK. Se ingreso la informacion en la tabla")
        except Exception as e:
            logging.error(f"ERROR. Al subir la informacion : {e}")

    @task
    def subir_inf_tracks(dic_artist):
        from Spotify_api.Models import Sync_Track
        try:
            Sync_Track.insert_to_table(dic_artist)
            logging.info(f"OK. Se ingreso la informacion en la tabla")
        except Exception as e:
            logging.error(f"ERROR. Al subir la informacion : {e}")

    @task
    def subir_mas_inf_artista(dic_mas_inf):
        from Spotify_api.Models import Sync_Inf
        try:
            Sync_Inf.delete_all()
            Sync_Inf.insert_to_table(dic_mas_inf)
            logging.info(f"OK. Se ingreso la informacion en la tabla")
        except Exception as e:
            logging.error(f"ERROR. Al subir la informacion : {e}")

    @task_group(group_id="artista")
    def procesar_artista():
        create_tabla_artista()
        resultado_artista_xcom = extraer_artista()
        subir_inf_artista(resultado_artista_xcom["datos_artista"])
        return resultado_artista_xcom

    @task_group(group_id="albun")
    def procesar_albunes(id_spotify):
        create_tabla_album()
        return_dicc_albums = extraer_album(id_artist=id_spotify)
        subir_inf_albums(return_dicc_albums)
        return return_dicc_albums

    @task_group(group_id="canciones")
    def procesar_canciones(lista_albums):
        create_tabla_canciones()
        return_dicc_tracks = extraer_tacks(lista_albums)
        subir_inf_tracks(return_dicc_tracks)
        return return_dicc_tracks

    @task_group(group_id="mas_informacion")
    def procesar_mas_info():
        create_table_mas_inf()
        return_dicc_mas_ifor=extraer_mas_info()
        subir_mas_inf_artista(return_dicc_mas_ifor)

    xcom_artista = procesar_artista()
    xcom_albums = procesar_albunes(xcom_artista["id_spotify"])
    xcom_canciones = procesar_canciones(xcom_albums)
    xcom_mas_inf = procesar_mas_info()


    (
    start_process
    >> verify_spotify_api_conect
    >> xcom_artista
    >> esperar_un_momento
    >> condicion_proceso_album >> xcom_albums
    >> condicion_proceso_canciones >> xcom_canciones
    >> condicion_mas_inf >> verify_playwright_conect >> xcom_mas_inf
    >> verify_big_querry_conect
    )

