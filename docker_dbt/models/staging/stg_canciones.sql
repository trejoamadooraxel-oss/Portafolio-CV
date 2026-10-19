select
    cast(id_cancion as INT64) as cancion_id,
    trim(nombre_cancion) as cancion_nombre,
    id_spotify as spotify_cancion_id,
    cast(id_album as INT64) as album_id
from {{ source('spotify_raw', 'track') }}