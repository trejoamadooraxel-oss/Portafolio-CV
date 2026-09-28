import Spotify_api.Playwright.scrapp_spotify as playwright
from Spotify_api.Esquemas.esquemas import TrackSchema

from Spotify_api.Models_async.artist import Artist

from Spotify_api.Models_async.queries import Queries
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import logging

router = APIRouter(
    prefix='/mas_informacion',
    tags=["mas_informacion"]
)

class AlbumSchemaEndpoint(BaseModel):
    nombre_artista: str

@router.post("/",status_code=status.HTTP_201_CREATED)
async def ingresar_mas_informacion_por_artista(artista:AlbumSchemaEndpoint):
    try:
        nuevo_artista = Artist(nombre_artista = artista.nombre_artista)

        #Verificamos que exista el artista en la tabla y obtener el id_spotify con el id_artista de la tabla artista
        result = await Artist.id_spotify_by_name(artista.nombre_artista)
        if result:
            datos = result[0]
            id_artista = datos["id_artista"]

            # Usamos los valores de retornados para Validar que existe el artista en la tabla de albunes
            list_dicc_album = await Queries.artist_missing()
            if list_dicc_album:

                lista_registros = []
                for dic in list_dicc_album:
                    try:
                        registro_validado = TrackSchema(**dic)
                        lista_registros.append(registro_validado.model_dump())
                        playwright.extract_artist_inf()



                    except Exception as e:
                        logging.warning(f"Warning. No se pudo validar la informacion de {dic}, {e}")





                return {
                    'resultado':'hola'
                }
            else:
                return{
                    f"Error": f"No se encontro ningun artista que falte de mas informacion"
                }
        else:
            return {
                f"Error": f"El artista: {artista.nombre_artista}  no se encuentra registrado en al DB."
            }

    except Exception as e:
        logging.error(f'Error al intresar el album: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al procesar la lista de Artistas con Spotify: {e}"
        )

@router.get("/")
async def consultar_table_canciones():
    registros = await Queries.all_table('tracks')
    return {"registros":registros}