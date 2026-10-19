select
    cast(id_artista as INT64) as artista_id,
    trim(nombre_artista) as artista_nombre,
    id_spotify as spotify_artista_id,
    cast(fecha_ingesta as TIMESTAMP) as cargado_at
from {{ source('spotify_raw', 'artista') }}