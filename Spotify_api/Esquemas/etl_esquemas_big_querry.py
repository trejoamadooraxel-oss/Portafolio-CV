from google.cloud import bigquery

SCHEMAS_BQ = {
    "artista": [
        bigquery.SchemaField("nombre_artista", "STRING", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("id_spotify", "STRING", mode="NULLABLE", description="Cantidad de seguidores"),
        bigquery.SchemaField("fecha_ingesta", "TIMESTAMP", mode="REQUIRED", description="Fecha de auditoría del ETL")
    ],
    "album":[
        bigquery.SchemaField("nombre_album", "STRING", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("id_spotify", "STRING", mode="NULLABLE", description="Cantidad de seguidores"),
        bigquery.SchemaField("fecha_lanzamiento", "DATE", mode="REQUIRED", description="Fecha de auditoría del ETL"),
        bigquery.SchemaField("num_canciones", "INTEGER", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("fecha_ingesta", "TIMESTAMP", mode="REQUIRED", description="Fecha de auditoría del ETL")
    ],
    "track":[
        bigquery.SchemaField("nombre_cancion", "STRING", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("id_spotify", "STRING", mode="NULLABLE", description="Cantidad de seguidores"),
        bigquery.SchemaField("num_cancion", "INTEGER", mode="REQUIRED", description="Fecha de auditoría del ETL"),
        bigquery.SchemaField("duracion", "INTEGER", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("id_album_spotify", "STRING", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("fecha_ingesta", "TIMESTAMP", mode="REQUIRED", description="Fecha de auditoría del ETL")
    ]

}