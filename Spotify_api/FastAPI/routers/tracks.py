import Spotify_api.Spotify_Api.extract_inf_api_asyn as spotify_api
from Spotify_api.Esquemas.esquemas import TrackSchema
from Spotify_api.Models_async.album import Album
from Spotify_api.Models_async.artist import Artist
from Spotify_api.Models_async.track import Track
from Spotify_api.Models_async.queries import Queries
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import logging

router = APIRouter(
    prefix='/canciones',
    tags=["canciones"]
)

class AlbumSchemaEndpoint(BaseModel):
    nombre_artista: str

@router.post("/",status_code=status.HTTP_201_CREATED)
async def ingresar_canciones_por_album_y_artista(artista:AlbumSchemaEndpoint):
    try:
        nuevo_artista = Artist(nombre_artista = artista.nombre_artista)

        #Verificamos que exista el artista en la tabla y obtener el id_spotify con el id_artista de la tabla artista
        result = await Artist.id_spotify_by_name(artista.nombre_artista)
        if result:
            datos = result[0]
            id_artista = datos["id_artista"]

            # Usamos los valores de retornados para Validar que existe el artista en la tabla de albunes
            list_dicc_album = await Queries.album_por_id_artista(id_artista)

            datos = list_dicc_album[0]

            if datos["id_album"] != None:

                registro = None
                lista_registros = []

                for dic in list_dicc_album:
                    try:
                        registro_validado = TrackSchema(**dic)
                        lista_registros.append(registro_validado.model_dump())

                    except Exception as e:
                        logging.warning(f"Warning. No se pudo validar la informacion de {dic}, {e}")

                sp = spotify_api.conection_spotify()
                dicc_canciones = await spotify_api.list_tracks(sp,list_dicc_album)

                if dicc_canciones:
                    await Track.insert_to_table(dicc_canciones)
                    logging.info(f"OK. Se ingreso la informacion en la tabla canciones")
                    registro = await Queries.search_by_name(artista.nombre_artista)

                    return {
                        "Registro": registro
                    }

                else:
                    return {
                        f"Error": f"El diccionario de canciones viene vacio: {dicc_canciones}."
                    }

            else:
                return {
                    f"Error": f"El artista: {artista.nombre_artista} no cuenta con albunes registrados en al DB."
                }

        else:
            return {
                f"Error": f"El artista: {artista.nombre_artista}  no se encuentra registrado en al DB."
            }

    except Exception as e:
        logging.error(f'Error al intresar el album: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al procesar el album con Spotify: {e}"
        )

@router.get("/")
async def consultar_table_canciones():
    registros = await Queries.all_table('tracks')
    return {"registros":registros}