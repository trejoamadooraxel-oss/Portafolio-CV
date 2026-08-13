import asyncio
from Models import Artist, Album, Track
from sqlalchemy import insert, delete, select, desc, asc
from conect_sqlalchemy import Base, engine, AsyncSessionLocal


class Queries:
    @staticmethod
    async def all_table(table):
        async with AsyncSessionLocal() as session:
            lista = []
            try:
                if table == 'artistas' or table == 'artist' :
                    search = Artist
                elif table == 'album':
                    search = Album
                elif table == 'tracks' or table == 'Tracks' or table == 'canciones' or table == 'Canciones' :
                    search = Track
                else:
                    search = None
                stmt = select(search)
                result = await session.execute(stmt)
                resultados = result.scalars().all()
                for registro in resultados:
                    datos = registro.__dict__.copy()
                    #Quitamos el ruido que al pasar por .__dict__ que le añade para
                    datos.pop('_sa_instance_state', None)
                    lista.append(datos)

            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")

        return lista

    @staticmethod
    async def album_por_id_artista(id_artista):
        async with AsyncSessionLocal() as session:
            lista = []
            try:

                stmt = (
                    select(
                        Artist.id_artista,
                        Artist.nombre_artista,
                        Album.id_album,
                        Album.nombre_album,
                        Album.id_spotify,
                        Album.fecha_lanzamiento,
                        Album.num_canciones
                    ).
                    join(Album, Artist.id_artista == Album.id_artista, isouter=True).
                    where(Artist.id_artista == id_artista).
                    order_by(Album.id_album.asc())
                )
                result = await session.execute(stmt)
                resultados = result.mappings().all()

                for registro in resultados:
                    lista.append(registro)

            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")

        return lista

    @staticmethod
    async def canciones_por_id_album(nombre_album):
        async with AsyncSessionLocal() as session:
            lista = []
            try:

                stmt = (
                    select(
                        Artist.id_artista,
                        Artist.nombre_artista,
                        Album.id_album,
                        Album.nombre_album,
                        Track.id_cancion,
                        Track.nombre_cancion,
                        Track.num_cancion,
                        Track.duracion
                    ).
                    join(Album, Artist.id_artista == Album.id_artista, isouter=True).
                    join(Track, Album.id_album == Track.id_album, isouter=True).
                    where(Album.nombre_album == nombre_album ).
                    order_by(Track.id_cancion)
                )

                result = await session.execute(stmt)
                resultados = result.mappings().all()

                for registro in resultados:
                    lista.append(registro)

            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")

        return lista

    @staticmethod
    async def alrtista_album_cancion():
        async with AsyncSessionLocal() as session:
            resultados = None
            try:
                stmt = (
                    select(
                        Artist.nombre_artista,
                        Album.nombre_album,
                        Track.nombre_cancion,
                        Album.fecha_lanzamiento
                    ).
                        join(Album, Artist.id_artista == Album.id_artista, isouter=True ).
                        join(Track, Album.id_album == Track.id_album, isouter=True).
                        where(Artist.id_artista == 3).
                        order_by(Track.nombre_cancion)
                )
                result = await session.execute(stmt)
                resultados = result.mappings().all()

            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")

        return resultados


async def main():
    infor = await Queries.all_artist()
    print(infor)

if __name__ == '__main__':
    asyncio.run(main())