select
    trim(nombre_artista) as nombre_artista,
    id_spotify as id_artista,
    url as url_artista,
    cast(fecha_ingesta as TIMESTAMP) as fecha_actualizacion
from {{ source('spotify_raw', 'artista') }}