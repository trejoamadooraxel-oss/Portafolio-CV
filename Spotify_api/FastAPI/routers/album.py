import Spotify_api.Spotify_Api.extract_inf_api_asyn as spotify_api
from Spotify_api.Esquemas.esquemas import AlbumSchema
from Spotify_api.Models_async.album import Album
from Spotify_api.Models_async.artist import Artist
from Spotify_api.Models_async.queries import Queries
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import logging

router = APIRouter(
    prefix='/album',
    tags=["Album"]
)

class ArtistaSchemaEndpoint(BaseModel):
    nombre_artist: str

@router.post("/",status_code=status.HTTP_201_CREATED)
async def ingresar_album_por_artista(artista:ArtistaSchemaEndpoint):
    try:
        nuevo_artista = Artist(nombre_artista = artista.nombre_artist)

        #Verificamos que exista el artista en la tabla y obtener el id_spotify
        result = await Artist.id_spotify_by_name(artista.nombre_artist)


        if result:
            datos = result[0]
            id_spotify = datos["id_spotify"]
            id_artista = datos["id_artista"]

            sp = spotify_api.conection_spotify()
            list_dic = await spotify_api.list_albums(sp,id_spotify,artista.nombre_artist)

            registro = None
            lista_registros = []
            for dic in list_dic:
                try:
                    registro_validado = AlbumSchema(**dic)
                    lista_registros.append(registro_validado.model_dump())
                except Exception as e:
                    logging.warning(f"Warning. No se pudo validar la informacion de {dic}, {e}")

            if lista_registros:
                await Album.insert_to_table(lista_registros)
                logging.info(f"OK. Se ingreso la informacion en la tabla")
                registro = await Queries.album_por_id_artista(id_artista)

            return {
                "Registro": registro
            }

        else:
            return {
                f"Error": f"El artista: {artista.nombre_artist} no se encuentra registrado en al DB."
            }

    except Exception as e:
        logging.error(f'Error al intresar el album: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al procesar el album con Spotify: {e}"
        )

@router.get("/")
async def consultar_tabla_album():
    registros = await Queries.all_table('album')
    return {"registros":registros}
