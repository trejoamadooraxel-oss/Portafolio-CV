import os
from google.cloud import bigquery
from datetime import datetime, timezone
import pandas as pd

def get_bq_client():
    return bigquery.Client(project=os.environ['GCP_PROJECT_ID'])

def asegurar_dataset(client, dataset_id):
    """Crea el dataset si no existe. Idempotente."""
    dataset_ref = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)

def obtener_siguiente_id(client, dataset_id, table_id):
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    query = f"SELECT MAX(id_artista) as max_id FROM `{table_ref}`"

    try:
        result = list(client.query(query).result())
        max_id = result[0].max_id
        return (max_id or 0) + 1
    except Exception:
        return 1

def cargar_dataframe(client, datos_artista: list, dataset_id, nombre_table, schema=None):

    df = pd.DataFrame(datos_artista)

    df["fecha_ingesta"] = datetime.now(timezone.utc)

    table_ref = f"{client.project}.{dataset_id}.{nombre_table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True if schema is None else False,
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return job.output_rows