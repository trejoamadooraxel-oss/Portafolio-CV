from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

@app.get("/")
def inicio():
    return {"Mensaje": "Hola iniciando en FastAPI"}

#pasamos un valor de tipo nombre y ese
#parametro es el que mostrara en el mensaje del querry
@app.get("/saludo/{nombre}")
def saludar(nombre: str):
    return {"mensaje": f"Hola, {nombre}"}

#simulamos una consulta a la base mediante sqlalchemy donde nos regresa
#lista una lista de artistas para validar solamente si existe y de lo
#contrario mandar un msj de error

diccionario_artistas = {

                    1:"Hocico",
                    2:"Kendrick Lamar",
                    3:"Metallica",
                    4:"EXO",
                    5:"Tyga",
                    6:"Hapax",
                    9:"Mecano"
                    }

diccionario_albunes = {

                    1:"Hola",
                    2:"Violeta",
                    3:"Enter Sandan",
                    4:"Off The Wall",
                    5:"Motivos",
                    6:"obsured",
                    9:"Dog eat Dog"
                    }
#####----------------------
#Funciona para hacer querrys
@app.get("/buscar")
def buscar(id_artista: int):
    if id_artista not in diccionario_artistas:
        raise HTTPException(status_code=404, detail="El id_artista no se encuentra en la base de datos")
    return {"nombre_artista": diccionario_artistas[id_artista], "id_artista": id_artista}

#####----------------------
@app.get("/buscar/{entidad}")
def buscar_for_entidad(entidad: str):
    entidades_disponibles = {
        "artistas": diccionario_artistas,
        "albunes": diccionario_albunes,
    }
    if entidad not in entidades_disponibles:
        raise HTTPException(status_code=404, detail="La entidad que buscas obtener no se encuentra")
    return {f"all_{entidad}": entidades_disponibles[entidad]}

#####----------------------
#Con GET usabas parámetros sueltos (termino: str). Con POST, cuando mandas varios campos juntos, se agrupan en una clase:
class ArtistaInput(BaseModel):
    nombre: str

@app.post("/artista", status_code=201)
def crear_artista(artista: ArtistaInput):
    nuevo_id = max(diccionario_artistas.keys()) + 1
    diccionario_artistas[nuevo_id] = artista.nombre
    return{"id":nuevo_id, "nombre":artista.nombre}

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