select
    cast(id_album as INT64) as album_id,
    trim(nombre_album) as album_nombre,
    id_spotify as spotify_album_id,
    cast(fecha_lanzamiento as DATE) as lanzado_fecha,
    cast(num_canciones as INT64) as total_tracks_album,
    cast(id_artista as INT64) as artista_id
from {{ source('spotify_raw', 'album') }}