from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional

class ArtistaSchema(BaseModel):
    nombre_artista: str
    id_spotify: str

class ArtistaSchemaBQ(BaseModel):
    nombre_artista: str
    id_spotify: str
    url: str

class AlbumSchema(BaseModel):
    nombre_album: str
    id_spotify: str
    fecha_lanzamiento: Optional[date] = None
    num_canciones: int
    id_artista: int

class AlbumSchemaBQ(BaseModel):
    id_spotify_artist: str
    num_canciones: int
    url: str
    id_spotify: str
    nombre_album: str
    tipo_album: str
    fecha_precisa: str
    fecha_lanzamiento: Optional[date] = None


class TrackSchema(BaseModel):
    nombre_cancion: str
    id_spotify: str
    num_cancion: int
    duracion: int
    id_album: int

class TrackSchemaBQ(BaseModel):
    id_spotify_album: str
    nombre_cancion: str
    id_spotify: str
    num_cancion: int
    duracion: int
    discos: int
    explicito: str
    url: str


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