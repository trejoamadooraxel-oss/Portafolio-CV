import Spotify_api.Spotify_Api.extract_inf_api_asyn as spotify_api
from Spotify_api.Esquemas.esquemas import AlbumSchema, AlbumSchemaBQ
from Spotify_api.Models_async.album import Album
from Spotify_api.Models_async.artist import Artist
from Spotify_api.Models_async.queries import Queries
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from datetime import datetime, timezone
from pydantic import BaseModel
import logging
import csv
import io

router = APIRouter(
    prefix='/album',
    tags=["Albums"]
)

class ArtistaSchemaEndpoint(BaseModel):
    nombre_artist: str

@router.post("/",status_code=status.HTTP_201_CREATED)
async def ingresar_album_por_artista(artista:ArtistaSchemaEndpoint):
    try:
        result = await Artist.id_spotify_by_name(artista.nombre_artist)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"El artista '{artista.nombre_artist}' no se encuentra registrado en la DB."
            )

        id_spotify = result[0]["id_spotify"]
        id_artista = result[0]["id_artista"]

        sp = spotify_api.conection_spotify()
        list_dic = await spotify_api.list_albums(sp, id_spotify, artista.nombre_artist)

        lista_validada = []
        for dic in list_dic:
            dic["id_artista"] = id_artista  # el de la DB, no el deducido por nombre
            try:
                lista_validada.append(AlbumSchema(**dic).model_dump())
            except Exception as e:
                logging.warning(f"No se pudo validar {dic}, {e}")

        if not lista_validada:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Ningún álbum de Spotify pasó la validación"
            )

        # Solo insertamos los álbumes que todavía no están en la tabla
        existentes = await Queries.album_por_id_artista(id_artista)
        ids_existentes = {a["id_spotify"] for a in existentes}
        nuevos = [a for a in lista_validada if a["id_spotify"] not in ids_existentes]

        if not nuevos:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Todos los álbumes de este artista ya están registrados"
            )

        await Album.insert_to_table(nuevos)
        logging.info(f"OK. Se ingresaron {len(nuevos)} álbum(es)")

        registro = await Queries.album_por_id_artista(id_artista)
        return {"Insertados": len(nuevos), "Registro": registro}

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error al ingresar el album: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno al procesar el album con Spotify: {e}"
        )

@router.get("/")
async def consultar_tabla_album():
    registros = await Queries.all_table('album')
    return {"registros":registros}

@router.delete("/{id_artista}", status_code=status.HTTP_204_NO_CONTENT)
async def eliminar_albunes_by_id(id_artista: int):
    try:
        existe = await Album.get_by_id_artist(id_artista)
        if not existe:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No existe una canción con id_track={id_artista}"
            )
        await Album.delete_register_by_id_artista(id_artista)
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
async def descargar_tabla_album():
    try:
        datos = await Queries.all_table('album')

        lista_registros = []
        for dic in datos:
            try:
                lista_registros.append(AlbumSchemaBQ(**dic).model_dump())
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
            "Content-Disposition":f"attachment; filename=Tabla_Album_{datetime.now()}.csv"
        }

        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers=headers
        )

    except Exception as e:
        return {"Error":f"{e}"}
