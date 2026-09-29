import Spotify_api.Spotify_Api.extract_inf_api_asyn as spotify_api
from Spotify_api.Esquemas.esquemas import TrackSchema, TrackSchemaBQ
from Spotify_api.Models_async.album import Album
from Spotify_api.Models_async.artist import Artist
from Spotify_api.Models_async.track import Track
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
    prefix='/canciones',
    tags=["canciones"]
)

class CancionesIngestaInput(BaseModel):
    nombre_artista: str

@router.post("/", status_code=status.HTTP_201_CREATED)
async def ingresar_canciones_por_artista(artista: CancionesIngestaInput):
    try:
        result = await Artist.id_spotify_by_name(artista.nombre_artista)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"El artista '{artista.nombre_artista}' no se encuentra registrado en la DB."
            )
        id_artista = result[0]["id_artista"]

        albums = await Queries.album_por_id_artista(id_artista)
        if not albums:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"'{artista.nombre_artista}' no tiene álbumes registrados. Ingrésalos primero con POST /album/."
            )

        sp = spotify_api.conection_spotify()
        dicc_canciones = await spotify_api.list_tracks(sp, albums)
        if not dicc_canciones:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Spotify no devolvió canciones para los álbumes de este artista."
            )

        validadas = []
        for dic in dicc_canciones:
            try:
                validadas.append(TrackSchema(**dic).model_dump())
            except Exception as e:
                logging.warning(f"No se pudo validar {dic}, {e}")

        if not validadas:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Ninguna canción pasó la validación."
            )

        # Solo insertamos las canciones que todavía no están en la tabla
        existentes = await Track.ids_spotify_existentes([a["id_album"] for a in albums])
        nuevas = [c for c in validadas if c["id_spotify"] not in existentes]

        if not nuevas:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Todas las canciones de este artista ya están registradas."
            )

        await Track.insert_to_table(nuevas)
        logging.info(f"OK. Se ingresaron {len(nuevas)} canción(es)")

        return {"Insertados": len(nuevas), "Registro": nuevas}

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error al ingresar las canciones: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al procesar las canciones con Spotify: {e}"
        )

@router.get("/")
async def consultar_table_canciones():
    registros = await Queries.all_table('tracks')
    return {"registros":registros}

@router.delete("/{id_artista}", status_code=status.HTTP_204_NO_CONTENT)
async def eliminar_cancion_by_id(id_artista: int):
    try:
        existe = await Track.get_by_id_artista(id_artista)
        if not existe:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No existe una canción con id_artista={id_artista}"
            )
        await Track.delete_register_by_id_artista(id_artista)
        return None

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error al borrar la canción: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al borrar la canción: {e}"
        )

@router.get("/descarga_inf")
async def descargar_tabla_canciones():
    try:
        datos = await Queries.all_table('tracks')

        lista_registros = []
        for dic in datos:
            try:
                lista_registros.append(TrackSchemaBQ(**dic).model_dump())
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
            "Content-Disposition": f"attachment; filename=Table_Canciones_{datetime.now()}.csv"
        }

        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers=headers
        )

    except Exception as e:
        return {"Error":f"{e}"}
