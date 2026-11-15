with albunes_base as (
    select * from {{ ref('stg_albunes') }}
    qualify row_number() over(
        partition by id_album
        order by fecha_actualizacion desc
    ) = 1
),

canciones as (
    select * from {{ ref('stg_canciones') }}
    qualify row_number() over(
        partition by id_cancion
        order by fecha_actualizacion desc
    ) = 1
)

select
    a.id_artista,
    a.id_album,
    a.nombre_album,
    a.discografia,
    a.num_canciones_album,
    a.temporalidad,
    a.lanzado_fecha,
    a.url_album,
    a.fecha_actualizacion as ultima_actualizacion_album,
    c.id_cancion,
    c.nombre_cancion,
    c.duracion_milisegundos,
    c.duracion_segundos,
    c.num_cancion_album,
    c.disco,
    c.explicito,
    c.url_cancion,
    c.fecha_actualizacion as ultima_actualizacion_canciones,

from albunes_base a
left join canciones c on a.id_album = c.id_album
