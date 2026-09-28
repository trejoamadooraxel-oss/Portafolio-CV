import Spotify_api.Conexiones.conection_playwright as con_playwright
from Spotify_api.Playwright.scrapp_spotify import extract_artist_inf
from Spotify_api.Models.sync_artist import Sync_Artist
from Spotify_api.Models.sync_more_inf import Sync_Inf
from Spotify_api.Models.sync_queries import Sync_Queries
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
def ingresar_mas_informacion_por_artista(artista:AlbumSchemaEndpoint):
    try:
        nuevo_artista = Sync_Artist(nombre_artista = artista.nombre_artista)

        #Verificamos que exista el artista en la tabla y obtener el id_spotify con el id_artista de la tabla artista
        result = Sync_Artist.id_spotify_by_name(artista.nombre_artista)

        if result:
            datos = result[0]
            id_artista = datos["id_artista"]

            # Usamos los valores de retornados para Validar que existe el artista en la tabla de albunes
            list_dicc_album = Sync_Queries.artist_missing()
            if list_dicc_album:

                lista_registros = []
                lista_registros.append(artista.nombre_artista)

                conection_p = con_playwright.Conection_playwright(headless=True)
                list_info = extract_artist_inf(conection_p, 'https://open.spotify.com/', list_dicc_album, Sync_Artist)
                Sync_Inf.insert_to_table(list_info)

                return {
                    'resultado':list_info
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
    registros = await Queries.all_table('mas_inf')
    return {"registros":registros}

@router.get("/list_inf_faltante")
async def lista_de_artististas_sin_mas_informacion():
    registros = await Queries.artist_missing()
    return {"registros":registros}