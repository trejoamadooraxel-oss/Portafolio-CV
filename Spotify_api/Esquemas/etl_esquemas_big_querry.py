from google.cloud import bigquery

SCHEMAS_BQ = {
    "artista": [
        bigquery.SchemaField("nombre_artista", "STRING", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("id_spotify", "STRING", mode="NULLABLE", description="ID correspondiente a Spotify"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE", description="URL de redireccionamiendo a Spotify"),
        bigquery.SchemaField("fecha_ingesta", "TIMESTAMP", mode="REQUIRED", description="Fecha de auditoría del ETL")
    ],
    "album":[
        bigquery.SchemaField("nombre_album", "STRING", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("id_spotify", "STRING", mode="NULLABLE", description="ID correspondiente a Spotify del album"),
        bigquery.SchemaField("fecha_lanzamiento", "DATE", mode="REQUIRED", description="Fecha de auditoría del ETL"),
        bigquery.SchemaField("num_canciones", "INTEGER", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("id_spotify_artist", "STRING", mode="REQUIRED", description="ID correspondiente a Spotify del artista"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE", description="URL de redireccionamiendo a Spotify"),
        bigquery.SchemaField("tipo_album", "STRING", mode="NULLABLE", description="Tipo de album o sencillo"),
        bigquery.SchemaField("fecha_precisa", "STRING", mode="NULLABLE", description="Dia"),
        bigquery.SchemaField("fecha_ingesta", "TIMESTAMP", mode="REQUIRED", description="Fecha de auditoría del ETL")

    ],
    "track":[
        bigquery.SchemaField("nombre_cancion", "STRING", mode="NULLABLE", description="Nombre del artista"),
        bigquery.SchemaField("id_spotify", "STRING", mode="NULLABLE", description="ID correspondiente a Spotify de la cancion"),
        bigquery.SchemaField("num_cancion", "INTEGER", mode="REQUIRED", description="Numero de cancion en el disco"),
        bigquery.SchemaField("duracion", "INTEGER", mode="NULLABLE", description="Durancion "),
        bigquery.SchemaField("id_spotify_album", "STRING", mode="REQUIRED", description="ID correspondiente a Spotify del album"),
        bigquery.SchemaField("explicito", "STRING", mode="REQUIRED", description="Descripcion di el disco Explicito"),
        bigquery.SchemaField("discos", "INTEGER", mode="NULLABLE", description="Numero de discos"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE", description="URL de redireccionamiendo a Spotify"),
        bigquery.SchemaField("fecha_ingesta", "TIMESTAMP", mode="REQUIRED", description="Fecha de auditoría del ETL")


    ],
    'mas_inf':[
        bigquery.SchemaField( 'artista', "STRING", mode="NULLABLE", description="Ciudad Oyente Num "),
        bigquery.SchemaField( 'seguidores',"INTEGER", mode="NULLABLE", description="Seguidores Totales "),
        bigquery.SchemaField( 'escuchas_mensuales',"INTEGER", mode="NULLABLE", description="Escuchas Mensuales "),
        bigquery.SchemaField( 'ciudad_oyente_uno',"STRING", mode="NULLABLE", description="Ciudad Oyente Num 1"),
        bigquery.SchemaField( 'num_oyentes_uno',"INTEGER", mode="NULLABLE", description="Num Oyentes Ciudad Num 1"),
        bigquery.SchemaField( 'ciudad_oyente_dos',"STRING", mode="NULLABLE", description="Ciudad Oyente Num 2"),
        bigquery.SchemaField( 'num_oyentes_dos',"INTEGER", mode="NULLABLE", description="Num Oyentes Ciudad Num 2"),
        bigquery.SchemaField( 'ciudad_oyente_tres',"STRING", mode="NULLABLE", description="Ciudad Oyente Num 3"),
        bigquery.SchemaField( 'num_oyentes_tres',"INTEGER", mode="NULLABLE", description="Num Oyentes Ciudad Num 3"),
        bigquery.SchemaField( 'ciudad_oyente_cuatro',"STRING", mode="NULLABLE", description="Ciudad Oyente Num 4"),
        bigquery.SchemaField( 'num_oyentes_cuatro',"INTEGER", mode="NULLABLE", description="Num Oyentes Ciudad Num 4"),
        bigquery.SchemaField( 'ciudad_oyente_cinco',"STRING", mode="NULLABLE", description="Ciudad Oyente Num 5"),
        bigquery.SchemaField( 'num_oyentes_cinco',"INTEGER", mode="NULLABLE", description="Num Oyentes Ciudad Num 5"),
        bigquery.SchemaField("fecha_ingesta", "TIMESTAMP", mode="REQUIRED", description="Fecha de auditoría del ETL")

    ]

}