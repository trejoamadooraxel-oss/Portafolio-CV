import Spotify_api.Spotify_Api.extract_inf_api_asyn as spotify_api
from Spotify_api.Esquemas.esquemas import ArtistaSchema, ArtistaSchemaBQ
from Spotify_api.Models_async.artist import Artist
from Spotify_api.Models_async.queries import Queries
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from datetime import datetime, timezone
from pydantic import BaseModel
import logging
import asyncio
import csv
import io


router = APIRouter(
    prefix='/artista',
    tags=["Artistas"]
)

class ArtistaSchemaEndpoint(BaseModel):
    nombre: str

class UpdateSchemaEndpoint(BaseModel):
    id_viejo: str
    id_nuevo: int

@router.post("/",status_code=status.HTTP_201_CREATED)
async def ingresar_artista(artista:ArtistaSchemaEndpoint):
    try:
        sp = spotify_api.conection_spotify()
        try:
            id_artista, list_dic = await asyncio.to_thread(
                spotify_api.identificador_artistas, sp, artista.nombre
            )
        except IndexError:
            raise HTTPException(status_code=404, detail=f"No se encontró '{artista.nombre}' en Spotify")

        lista_registros = []
        for dic in list_dic:
            try:
                lista_registros.append(ArtistaSchema(**dic).model_dump())
            except Exception as e:
                logging.warning(f"No se pudo validar {dic}, {e}")

        if not lista_registros:
            raise HTTPException(status_code=422, detail="La información de Spotify no pasó la validación")

        nombre_spotify = lista_registros[0]["nombre_artista"]

        if await Artist.search_by_name(nombre_spotify):
            raise HTTPException(status_code=409, detail=f"'{nombre_spotify}' ya existe")

        await Artist.insert_to_table(lista_registros)
        return {"Registro": await Artist.search_by_name(nombre_spotify)}

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error al ingresar artista: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno al procesar el artista: {e}")

@router.get("/")
async def consultar_table_artista():
    registros = await Queries.all_table('artistas')
    return {"registros":registros}

@router.delete("/{id_artista}", status_code=status.HTTP_204_NO_CONTENT)
async def eliminar_info_by_id(id_artista: int):
    try:
        existe = await Artist.get_by_id_artist(id_artista)
        if not existe:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No existe una canción con id_track={id_artista}"
            )
        await Artist.delete_register_by_id_artista(id_artista)
        return None

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error al borrar el artista: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al borrar la canción: {e}"
        )

@router.get("/descarga_inf")
async def descargar_tabla_artistas():
    try:
        datos = await Queries.all_table('artist')

        lista_registros = []
        for dic in datos:
            try:
                lista_registros.append(ArtistaSchemaBQ(**dic).model_dump())
            except Exception as e:
                logging.warning(f"No se pudo validar {dic}, {e}")

        if not lista_registros:
            return {"consulta": [], "mensaje": "No se encontraron registros válidos"}

        buffer = io.StringIO()

        cabeceras = list(lista_registros[0].keys())
        writer = csv.DictWriter(buffer, fieldnames=cabeceras)

        writer.writeheader()
        writer.writerows(lista_registros)

        buffer.seek(0)

        headers = {
            "Content-Disposition": f"attachment; filename=Table_Artista_{datetime.now()}.csv"
        }

        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers=headers
        )

    except Exception as e:
        return {"Error":f"{e}"}



@router.patch("/{id_viejo}/{id_nuevo}", status_code=status.HTTP_204_NO_CONTENT)
async def actualizar_parcial_info_by_id(id_viejo: int, id_nuevo: int ):
    try:
        existe = await Artist.get_by_id_artist(id_viejo)
        if not existe:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No existe el artista con id_artista={id_viejo}"
            )
        await Artist.update_by_id_artista(id_viejo, id_nuevo)
        return None

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error al borrar el artista: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al borrar la canción: {e}"
        )

@router.put("/actualizacion_completo/{id_viejo}/{id_nuevo}", status_code=status.HTTP_204_NO_CONTENT)
async def actualizar_total_info_by_id(id_viejo: int, id_nuevo: int, nombre_artista:str, id_spotifiy:str):
    try:
        existe = await Artist.get_by_id_artist(id_viejo)
        if not existe:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No existe el artista con id_artista={id_viejo}"
            )
        await Artist.update_register_by_id_artista(id_viejo, id_nuevo, nombre_artista, id_spotifiy)
        return None

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error al borrar el artista: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al borrar la canción: {e}"
        )
