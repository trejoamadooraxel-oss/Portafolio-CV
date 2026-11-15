select
    id_spotify_artist as id_artista,
    cast(num_canciones as INT64) as num_canciones_album,
    url as url_album,
    id_spotify as id_album,
    trim(nombre_album) as nombre_album,

    case
        when num_canciones = 1 then replace(tipo_album, 'album', 'single')
        else tipo_album
    end as discografia,

    trim(fecha_precisa) as temporalidad,
    cast(fecha_lanzamiento as DATE) as lanzado_fecha,
    cast(fecha_ingesta as TIMESTAMP) as fecha_actualizacion
from {{ source('spotify_raw', 'album') }}