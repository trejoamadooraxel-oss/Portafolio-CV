from pydantic import BaseModel
from datetime import date
from typing import Optional

class ArtistaSchema(BaseModel):
    nombre_artista: str
    id_spotify: str

class ArtistaSchemaBQ(BaseModel):
    id_artista: int
    nombre_artista: str
    id_spotify: str


class AlbumSchema(BaseModel):
    nombre_album: str
    id_spotify: str
    fecha_lanzamiento: Optional[date] = None
    num_canciones: int
    id_artista: int

class AlbumSchemaBQ(BaseModel):
    id_album: int
    nombre_album: str
    id_spotify: str
    fecha_lanzamiento: Optional[date] = None
    num_canciones: int
    id_artista: int

class TrackSchema(BaseModel):
    nombre_cancion: str
    id_spotify: str
    num_cancion: int
    duracion: int
    id_album: int

class TrackSchemaBQ(BaseModel):
    id_cancion: int
    nombre_cancion: str
    id_spotify: str
    num_cancion: int
    duracion: int
    id_album: int

class InfSchema(BaseModel):
    id_artista: int
    artista: str
    seguidores: int
    escuchas_mensuales: int
    ciudad_oyente_uno: str
    num_oyentes_uno: int
    ciudad_oyente_dos: str
    num_oyentes_dos: int
    ciudad_oyente_tres: str
    num_oyentes_tres: int
    ciudad_oyente_cuatro: str
    num_oyentes_cuatro: int
    ciudad_oyente_cinco: str
    num_oyentes_cinco: int