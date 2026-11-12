select
    id_spotify_artist as id_artista,
    cast(num_canciones as INT64) as num_canciones_album,
    url as url_artista,
    id_spotify as id_album,
    trim(nombre_album) as album_nombre,
    trim(tipo_album) as discografia,
    trim(fecha_precisa) as temporalidad,
    cast(fecha_lanzamiento as DATE) as lanzado_fecha,
    cast(fecha_ingesta as TIMESTAMP) as fecha_actualizacion
from {{ source('spotify_raw', 'album') }}