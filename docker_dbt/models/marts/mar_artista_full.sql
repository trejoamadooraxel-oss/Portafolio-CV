{{ config(materialized='table') }}

with album as (

    SELECT
        id_artista,
        COUNT(DISTINCT id_album) AS discografias_totales,
        COUNT(DISTINCT IF(discografia = 'album',  id_album, NULL)) AS albums_total,
        COUNT(DISTINCT IF(discografia = 'single', id_album, NULL)) AS singles_total,
        COUNT(DISTINCT id_cancion) AS canciones_totales,
    FROM {{ ref('int_album_canciones') }}
    GROUP BY id_artista
),

artistas as (

    SELECT * from {{ ref('int_artist_mas_inf') }}

)


SELECT
    ar.ultima_actualizacion_artista,
    ar.id_artista,
    ar.nombre_artista,
    ar.seguidores,
    ar.oyentes_mensuales,
    al.discografias_totales,
    al.albums_total,
    al.singles_total,
    al.canciones_totales,
    ar.ciudad_num_uno,
    ar.oyentes_ciudad_num_uno,
    ar.ciudad_num_dos,
    ar.oyentes_ciudad_num_dos,
    ar.ciudad_num_tres,
    ar.oyentes_ciudad_num_tres,
    ar.ciudad_num_cuatro,
    ar.oyentes_ciudad_num_cuatro,
    ar.ciudad_num_cinco,
    ar.oyentes_ciudad_num_cinco,
    ar.url_artista




FROM artistas ar
left join album al on ar.id_artista = al.id_artista