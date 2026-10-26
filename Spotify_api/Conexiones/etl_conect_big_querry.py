import time

from google.cloud.exceptions import NotFound, Forbidden, GoogleCloudError
from datetime import datetime, timezone
from google.cloud import bigquery
import pandas as pd
import platform
import os

def get_bq_client(gcp_project):
    return bigquery.Client(project=gcp_project)

def verificar_conexion_bigquery():
    """
    Reproduce, paso a paso, la misma verificación que se hizo
    manualmente dentro del contenedor:
      1. Instanciar el cliente
      2. Confirmar el project_id
      3. Listar datasets existentes (llamada real a la API)
    """
    try:

        if platform.system() == 'Darwin':
            from dotenv import load_dotenv
            load_dotenv('/Users/axel/Documents/Portafolio/Spotify_api/env_var/varaibles_credential.env')
            gcp_project = os.getenv('GCP_PROJECT_ID')

        else:
            gcp_project = os.environ['GCP_PROJECT_ID']

        if not gcp_project:
            print("ERROR. Error de Configuración falta la variable de entorno 'GCP_PROJECT_ID'.")
            raise ValueError("ERROR. Error de Configuración falta la variable de entorno 'GCP_PROJECT_ID'.")

        print(f"OK. Instanciando cliente de BigQuery.")
        client = bigquery.Client(project=gcp_project)  # toma el proyecto de GOOGLE_APPLICATION_CREDENTIALS / ADC
        print(f"OK. Cliente conectado al proyecto: {client.project}")

        print(f"OK. Listando datasets existentes:")
        datasets = list(client.list_datasets())

        if datasets:
            print(f"OK. Se encontraron {len(datasets)} dataset(s):")
            for ds in datasets:
                print(f" - {ds.dataset_id}")
        else:
            print(f"OK. Conexión exitosa. Aún no existen datasets en este proyecto.")

        print(f"OK. Verificación de conexión a BigQuery completada sin errores.")

        # Deja el project_id disponible en XCom por si otra tarea lo necesita después
        return gcp_project

    except NotFound as e:
        # Aquí mapeas específicamente tu prueba del error 404
        print(f"ERROR. 404 El proyecto o recurso de GCP no fue encontrado. Verifica el ID: '{gcp_project}'")
        raise (f"BigQuery 404 Not Found: {e}")

    except Forbidden as e:
        # Error 403 de permisos/credenciales
        print("ERROR. 403 Problema de permisos. Revisa las credenciales de la cuenta de servicio.")
        raise (f"BigQuery 403 Forbidden: {e}")

    except GoogleCloudError as e:
        # Cualquier otro error genérico de la API de Google
        print(f"ERROR. Error de la API de Google Cloud: {e}")
        raise e

    except KeyError as e:
        # Por si no existe la variable de entorno 'GCP_PROJECT_ID' en el contenedor
        print(f"ERROR. Error de Configuración: Falta la variable de entorno obligatoria {e}")
        raise e

    except Exception as e:
        # Tu salvavidas para cualquier otra falla inesperada de Python
        print(f"ERROR. Error inesperado en el proceso: {e}")
        raise e

def asegurar_dataset(client, dataset_id):
    """Crea el dataset si no existe. Idempotente."""
    print(type(client))
    dataset_ref = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print('Dataset Verificado.')
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print('Dataser Creado.')

def obtener_siguiente_id_artista(table_ref, client):

    query = f"SELECT MAX(id_artista) as max_id FROM `{table_ref}`"

    try:
        result = list(client.query(query).result())
        max_id = result[0].max_id
        return (max_id or 0) + 1
    except Exception:
        return 1

def cargar_dataframe(client, datos_artista: list, dataset_id, nombre_table, schema=None):

    asegurar_dataset(client, dataset_id)

    table_ref = f"{client.project}.{dataset_id}.{nombre_table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True if schema is None else False,
        schema=schema,
    )

    time.sleep(5)


    df = pd.DataFrame(datos_artista)
    print(df.columns)
    #id_siguiente = obtener_siguiente_id_artista(table_ref, client)
    #df.insert(0,'id_artista', id_siguiente)

    if nombre_table == 'album':

        def normalizar_fecha(fecha_str):
            if not isinstance(fecha_str, str) or not fecha_str:
                return datetime(1998, 1, 27)
            try:
                # Fecha completa: "2003-06-15"
                if len(fecha_str) == 10:
                    return datetime.strptime(fecha_str, "%Y-%m-%d")
                # Solo año y mes: "2003-06"
                elif len(fecha_str) == 7:
                    return datetime.strptime(fecha_str, "%Y-%m")
                # Solo año: "2003"
                elif len(fecha_str) == 4:
                    return datetime.strptime(fecha_str, "%Y")
                else:
                    return datetime(1998, 1, 27)
            except:
                return datetime(1998, 1, 27)

        col_drop = ['id_artista']
        df.drop(col_drop, axis=1, inplace=True)

        df['fecha_lanzamiento'] = df["fecha_lanzamiento"].apply(lambda x: normalizar_fecha(x))

    if nombre_table == 'track':
        col_drop = ['id_album']
        df.drop(col_drop, axis=1, inplace=True)

    df["fecha_ingesta"] = datetime.now(timezone.utc)

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return job.output_rows

if __name__ == "__main__":
    retorno = verificar_conexion_bigquery()
    cliente = get_bq_client(retorno)
    asegurar_dataset(cliente, 'Spotify_dataset')
