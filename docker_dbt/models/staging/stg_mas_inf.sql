select
    cast(id_artista as INT64) as artista_id,
    cast(seguidores as INT64) as artista_seguidores,
    cast(escuchas_mensuales as INT64) as oyentes_mensuales
from {{ source('spotify_raw', 'mas_inf') }}
