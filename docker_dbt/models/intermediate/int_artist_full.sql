with artistas_base as (
    select * from {{ ref('stg_artistas') }}
),

detalles_extra as (
    select * from {{ ref('stg_mas_inf') }}
)

select
    a.artista_id,
    a.artista_nombre,
    a.spotify_artista_id,
    coalesce(d.artista_seguidores, 0) as artista_seguidores,
    coalesce(d.oyentes_mensuales, 0) as oyentes_mensuales,
    'Sin Género' as genero,
    a.cargado_at
from artistas_base a
left join detalles_extra d on a.artista_id = d.artista_id
