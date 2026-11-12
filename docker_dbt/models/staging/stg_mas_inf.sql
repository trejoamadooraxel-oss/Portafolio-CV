select
    artista as nombre_artista,
    cast(seguidores as INT64) as seguidores,
    cast(escuchas_mensuales as INT64) as oyentes_mensuales,
    ciudad_oyente_uno as ciudad_num_uno,
    cast(num_oyentes_uno as INT64) oyentes_ciudad_num_uno,
     ciudad_oyente_dos as ciudad_num_dos,
    cast(num_oyentes_dos as INT64) oyentes_ciudad_num_dos,
     ciudad_oyente_tres as ciudad_num_tres,
    cast(num_oyentes_tres as INT64) oyentes_ciudad_num_tres,
     ciudad_oyente_cuatro as ciudad_num_cuatro,
    cast(num_oyentes_cuatro as INT64) oyentes_ciudad_num_cuatro,
     ciudad_oyente_cinco as ciudad_num_cinco,
    cast(num_oyentes_cinco as INT64) oyentes_ciudad_num_cinco,
    cast(fecha_ingesta as TIMESTAMP) as fecha_actualizacion
from {{ source('spotify_raw', 'mas_inf') }}


