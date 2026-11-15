select
    id_spotify_album as id_album,
    trim(nombre_cancion) as nombre_cancion,
    id_spotify as id_cancion,
    cast(num_cancion as INT64) as num_cancion_album,
    cast(duracion as INT64) as duracion_milisegundos,
    FORMAT_TIME('%M:%S', TIME_ADD(TIME '00:00:00', INTERVAL duracion MILLISECOND)) AS duracion_segundos,
    cast(discos as INT64) as disco,
    explicito as explicito,
    url as url_cancion,
    cast(fecha_ingesta as TIMESTAMP) as fecha_actualizacion
from {{ source('spotify_raw', 'track') }}