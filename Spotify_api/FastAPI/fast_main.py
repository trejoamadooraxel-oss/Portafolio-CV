from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from Models import Artist, Album, Track, Queries
from fastapi.middleware.cors import CORSMiddleware
import conection_api as spotify_api


'''
Mostrar todo lo de las tablas

buscar albunes por artistas
buscar canciones por artista
buscar canciones por album

insertar un nuevo artista
insertar un nuevos album 
insertar un nuevas canciones

### Developer perfil ###
update
delete

'''


app = FastAPI()

##### GENERAL --------------------

@app.get("/buscar/all/{table}",
         summary="Listar todos los registros de la tabla a buscar",
         tags=["General"],
         response_description="Lista de artistas con su nombre e id_spotify"
         )
async def buscar(table: str):
    querry = await Queries.all_table(table)
    return {"all_artistas": querry}

##### Album ----------------------

@app.get("/buscar/album/{id_artista}",
         summary="Lista total de albunes por id de artista",
         tags=["Album"],
         response_description="Lista total de albunes por id de artista"
         )

async def buscar_album_por_id_artista(id_artista: int):
    querry = await Queries.album_por_id_artista(id_artista)
    return {"albums": querry}

##### Canciones ----------------------

@app.get("/buscar/album/{id_artista}/{id_album}",
         summary="Lista total de canciones por id_artista y id_album",
         tags=["Canciones"],
         response_description="Lista total de canciones por id_artista y id_album"
         )

async def buscar_canciones_por_id_album(nombre_album: str):
    querry = await Queries.canciones_por_id_album(nombre_album)
    return {"albums": querry}

#####  INGRESAR NUEVO ARTISTA----------------------
#Con GET usabas parámetros sueltos (termino: str). Con POST, cuando mandas varios campos juntos, se agrupan en una clase:
class ArtistaInput(BaseModel):
    nombre_artista: str

@app.post("/artistas",
          status_code=201,
          summary="Ingestar un nuevo artista",
          tags=["Insercion de Datos"],
          response_description="Ingestar un nuevo artista"
          )

async def ingresar_artista(artista: ArtistaInput):
    sp = spotify_api.conection_spotify()
    id_artista, dic_artist = spotify_api.identificador_artistas(sp, artista.nombre_artista)
    await Artist.insert_to_table(dic_artist)

    dicc_albums = await spotify_api.list_albums(sp, id_artista, artista.nombre_artista)
    await Album.insert_to_table(dicc_albums)

    dic_canciones = await spotify_api.list_tracks(sp, dicc_albums)
    await Track.insert_to_table(dic_canciones)

    return {"status": "insertado", "artista": dic_artist}



#####----------------------
class ArtistaPatch(BaseModel):
    nombre: str | None = None

@app.patch("/artista/{id_artista}")
def actualizacion_parcial(id_artista: int, cambio: ArtistaPatch):
    if id_artista not in diccionario_artistas:
        raise HTTPException(status_code=404, detail="El id_artista no se encuentra en la base de datos")
    if cambio.nombre is not None:
        diccionario_artistas[id_artista] = cambio.nombre
    return {"id": id_artista, "nombre":diccionario_artistas[id_artista]}

#####----------------------
@app.delete("/artista/{id_artista}", status_code=204)
def borrar_artista(id_artista: int):
    if id_artista not in diccionario_artistas:
        raise HTTPException(status_code=404, detail="El id_artista no se encuentra en la base de datos")
    del diccionario_artistas[id_artista]
    return None

#####----------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # o una lista específica de dominios permitidos
    allow_methods=["*"],
    allow_headers=["*"],
)

#####----------------------