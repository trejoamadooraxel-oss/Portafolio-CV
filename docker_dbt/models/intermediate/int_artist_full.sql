with artistas_base as (
    select * from {{ ref('stg_artistas') }}
),

detalles_extra as (
    select * from {{ ref('stg_mas_inf') }}
)

select

    a.id_artista,
    a.nombre_artista,
    a.url_artista,
    a.fecha_actualizacion as ultima_actualizacion_artista,
    d.seguidores,
    d.oyentes_mensuales,
    d.ciudad_num_uno,
    d.oyentes_ciudad_num_uno,
    d.ciudad_num_dos,
    d.oyentes_ciudad_num_dos,
    d.ciudad_num_tres,
    d.oyentes_ciudad_num_tres,
    d.ciudad_num_cuatro,
    d.oyentes_ciudad_num_cuatro,
    d.ciudad_num_cinco,
    d.oyentes_ciudad_num_cinco,
    d.fecha_actualizacion as ultima_actualizacion_mas_inf

from artistas_base a
left join detalles_extra d on a.nombre_artista = d.nombre_artista
