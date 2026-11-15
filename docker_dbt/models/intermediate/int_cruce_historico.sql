with artistas_base as (
    select * from {{ ref('stg_artistas') }}
),

mas_inf_base as (
    select * from {{ ref('stg_mas_inf') }}
),

albunes_base as (
    select * from {{ ref('stg_albunes') }}
),

canciones_base as (
    select * from {{ ref('stg_canciones') }}
)



select
    arb.id_artista,
    arb.nombre_artista,
    arb.url_artista,
    arb.fecha_actualizacion as ultima_actualizacion_artista,
    mib.seguidores,
    mib.oyentes_mensuales,
    mib.ciudad_num_uno,
    mib.oyentes_ciudad_num_uno,
    mib.ciudad_num_dos,
    mib.oyentes_ciudad_num_dos,
    mib.ciudad_num_tres,
    mib.oyentes_ciudad_num_tres,
    mib.ciudad_num_cuatro,
    mib.oyentes_ciudad_num_cuatro,
    mib.ciudad_num_cinco,
    mib.oyentes_ciudad_num_cinco,
    mib.fecha_actualizacion as ultima_actualizacion_mas_inf,
    alb.id_album,
    alb.nombre_album,
    alb.discografia,
    alb.num_canciones_album,
    alb.temporalidad,
    alb.lanzado_fecha,
    alb.url_album,
    alb.fecha_actualizacion as ultima_actualizacion_album,
    cab.id_cancion,
    cab.nombre_cancion,
    cab.duracion_milisegundos,
    cab.duracion_segundos,
    cab.num_cancion_album,
    cab.disco,
    cab.explicito,
    cab.url_cancion,
    cab.fecha_actualizacion as ultima_actualizacion_canciones,

from artistas_base arb
left join mas_inf_base mib on arb.nombre_artista = mib.nombre_artista
left join albunes_base alb on arb.id_artista = alb.id_artista
left join canciones_base cab on alb.id_album = cab.id_album