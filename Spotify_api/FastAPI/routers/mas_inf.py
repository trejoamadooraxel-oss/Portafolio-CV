import Spotify_api.Conexiones.conection_playwright as con_playwright
from Spotify_api.Playwright.scrapp_spotify import extract_artist_inf
from Spotify_api.Esquemas.esquemas import InfSchema
from Spotify_api.Models.sync_artist import Sync_Artist
from Spotify_api.Models.sync_more_inf import Sync_Inf
from Spotify_api.Models.sync_queries import Sync_Queries
from Spotify_api.Models_async.queries import Queries
from Spotify_api.Models_async.more_inf import Inf
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from datetime import datetime, timezone
from pydantic import BaseModel
import logging
import asyncio
import csv
import io

router = APIRouter(
    prefix='/mas_informacion',
    tags=["mas_informacion"]
)

class MasInfEndpointInput(BaseModel):
    nombre_artista: str

@router.post("/", status_code=status.HTTP_201_CREATED)
def ingresar_mas_informacion_por_artista(artista: MasInfEndpointInput):
    conection_p = None
    try:
        result = Sync_Artist.id_spotify_by_name(artista.nombre_artista)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"El artista '{artista.nombre_artista}' no se encuentra registrado en la DB."
            )

        faltantes = Sync_Queries.artist_missing()
        nombre_real = next(
            (n for n in faltantes if n.lower() == artista.nombre_artista.lower()),
            None
        )
        if not nombre_real:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"'{artista.nombre_artista}' ya tiene información registrada."
            )

        conection_p = con_playwright.Conection_playwright(headless=True)
        list_info = extract_artist_inf(conection_p, 'https://open.spotify.com/', [nombre_real], Sync_Artist)

        if not list_info:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"No se pudo extraer información de '{nombre_real}' desde Spotify."
            )

        Sync_Inf.insert_to_table(list_info)
        logging.info(f"OK. Se ingresó la información extra de {nombre_real}")

        return {"resultado": list_info}

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f'Error al ingresar la información extra: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al procesar la información con Spotify: {e}"
        )
    finally:
        if conection_p:
            conection_p.close_browser()
            conection_p.close_conection_p()

@router.get("/")
async def consultar_table_canciones():
    registros = await Queries.all_table('mas_inf')
    return {"registros":registros}

@router.get("/list_inf_faltante")
async def lista_de_artististas_sin_mas_informacion():
    registros = await Queries.artist_missing()
    return {"registros":registros}


@router.delete("/{id_artista}", status_code=status.HTTP_204_NO_CONTENT)
async def eliminar_info_by_id(id_artista: int):
    try:
        existe = await Inf.get_by_id_artist(id_artista)
        if not existe:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No existe una canción con id_track={id_artista}"
            )
        await Inf.delete_register_by_id_artista(id_artista)
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
        datos = await Queries.all_table('mas_inf')

        lista_registros = []
        for dic in datos:
            try:
                lista_registros.append(InfSchema(**dic).model_dump())
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
            "Content-Disposition": f"attachment; filename=Table_Mas_Inf_{datetime.now()}.csv"
        }

        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers=headers
        )

    except Exception as e:
        return {"Error":f"{e}"}