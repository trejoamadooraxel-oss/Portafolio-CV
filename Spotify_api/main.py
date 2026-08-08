import time

from conect_sqlalchemy import engine
from sqlalchemy import Table
from Models.artist import Artist
from Models.album import Album
from Models.track import Track
from Models.users_db import DB_admin
import conection_api as spotify_api


def creacion_tablas():

    Artist.create_table()
    time.sleep(3)
    Album.create_table()
    time.sleep(3)
    Track.create_table()

def informacion_artista(sp, name_artist):
    id_artista, dic_artist = spotify_api.identificador_artistas(sp,name_artist)
    #insertar_artista(dic_artist)

    dicc_albums = spotify_api.list_albums(sp, id_artista, name_artist)
    Album.insert_to_table(dicc_albums)

    dic_canciones = spotify_api.list_tracks(sp, dicc_albums)
    Track.insert_to_table(dic_canciones)

def insertar_artista(dic_artist):
    Artist.insert_to_table(dic_artist)

def insertar_albums(dicc_albums):
    Album.insert_to_table(dicc_albums)

def insertar_canciones(dic_canciones):
    Track.insert_to_table(dic_canciones)


if __name__ == '__main__':

    #Conexcion y extracion de informaion de API

    sp = spotify_api.conection_spotify()
    search_artista = input('Ingresa el Artista que deseas bucar: ')
    creacion_tablas()
    time.sleep(1)
    dic_artist, dicc_albums, dic_canciones = informacion_artista(sp,search_artista)



    #Area de Administracion de usuarios y permisos
    """ """
    #DB_admin.crear_usuario('miranda','mona')
    """ """









    """
    
    Artist.__table__.create(bind=engine, checkfirst=True)   # crear tabla
    Artist.__table__.drop(bind=engine, checkfirst=True)     # eliminar tabla
    Artist.__table__.exists(bind=engine)                    # verificar si existe
    Artist.__table__.name                                   # nombre de la tabla
    Artist.__table__.columns                                # columnas
    Artist.__table__.primary_key        
        
    """