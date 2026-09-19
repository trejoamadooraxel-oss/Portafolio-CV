from google.cloud import bigquery


def verify_conect_bigquerry():
    print("Paso 1: Instanciando cliente de BigQuery...")
    client = bigquery.Client()
    print(f"Paso 1 OK. Cliente conectado al proyecto: {client.project}")

    print("Paso 2: Listando datasets existentes...")
    datasets = list(client.list_datasets())

    if datasets:
        print(f"Paso 2 OK. Se encontraron {len(datasets)} dataset(s):")
        for ds in datasets:
            print(f"  - {ds.dataset_id}")
    else:
        print("Paso 2 OK. Conexión exitosa. Aún no existen datasets en este proyecto.")

    print("Verificación de conexión a BigQuery completada sin errores.")