import Spotify_api.Spotify_Api.extract_inf_api_asyn as spotify_api
from Spotify_api.Esquemas.esquemas import ArtistaSchema
from Spotify_api.Models_async.artist import Artist
from Spotify_api.Models_async.queries import Queries
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import logging

router = APIRouter(
    prefix='/artista',
    tags=["Artistas"]
)


class ArtistaSchemaEndpoint(BaseModel):
    nombre: str


@router.post("/",status_code=status.HTTP_201_CREATED)
async def ingresar_artista(artista:ArtistaSchemaEndpoint):
    try:
        nuevo_artist = Artist(nombre_artista = artista.nombre)
        sp = spotify_api.conection_spotify()
        id_artista, list_dic = spotify_api.identificador_artistas(sp, nuevo_artist)

        registro = None
        lista_registros = []
        for dic in list_dic:
            try:
                registro_validado = ArtistaSchema(**dic)
                lista_registros.append(registro_validado.model_dump())
            except Exception as e:
                logging.warning(f"Warning. No se pudo validar la informacion de {dic}, {e}")

        if lista_registros:
            await Artist.insert_to_table(lista_registros)
            logging.info(f"OK. Se ingreso la informacion en la tabla")

            registro = await Artist.search_by_name(artista.nombre)

        return {
            "Registro":registro
        }

    except Exception as e:
        logging.error(f"Error al ingresar artista: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al procesar el artista con Spotify:{e}"
        )


@router.get("/")
async def consultar_table_artista():
    registros = await Queries.all_table('artistas')
    return {"registros":registros}

