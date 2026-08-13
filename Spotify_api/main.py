import asyncio
from Models import Artist, Album, Track
from Models.users_db import DB_admin
import conection_api as spotify_api

async def creacion_tablas():
    await Artist.create_table()
    await asyncio.sleep(3)
    await Album.create_table()
    await asyncio.sleep(3)
    await Track.create_table()

async def informacion_artista(sp, name_artist):
    id_artista, dic_artist = spotify_api.identificador_artistas(sp, name_artist)
    await Artist.insert_to_table(dic_artist)

    dicc_albums = await spotify_api.list_albums(sp, id_artista, name_artist)
    await Album.insert_to_table(dicc_albums)

    dic_canciones = await spotify_api.list_tracks(sp, dicc_albums)
    await Track.insert_to_table(dic_canciones)

    return dic_artist, dicc_albums, dic_canciones

async def main():
    sp = spotify_api.conection_spotify()
    search_artista = input('Ingresa el Artista que deseas buscar: ')

    await creacion_tablas()
    await asyncio.sleep(1)
    dic_artist, dicc_albums, dic_canciones = await informacion_artista(sp, search_artista)

    print(dic_artist, dicc_albums, dic_canciones)


if __name__ == '__main__':
    asyncio.run(main())