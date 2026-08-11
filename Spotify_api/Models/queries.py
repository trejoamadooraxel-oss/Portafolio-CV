import sys
import os

from requests import session

from .artist import Artist
from .album import Album
from .track import Track
from sqlalchemy import Column, Integer, String, Numeric, Date
from sqlalchemy import insert, delete, select, desc, asc
from conect_sqlalchemy import Base, engine, Session


class Queries:
    @staticmethod
    def all_artist():
        session = Session()
        lista = None
        try:

            stmt = select(
                    Artist
                    )
            resultados = session.execute(stmt).scalars().all()
            lista = []
            for r in resultados:
                d = r.__dict__
                d.pop('_sa_instance_state', None)  # quitar metadata interna de SQLAlchemy
                lista.append(d)


        except Exception as e:
            session.rollback()
            print(f"ERROR: {e}")
        finally:
            session.close()
            return lista

    @staticmethod
    def alrtista_album():
        session = Session()
        resultados = None
        try:

            stmt = (
                select(
                    Artist.nombre_artista,
                    Album.nombre_album
                ).
                join(Album, Artist.id_artista == Album.id_artista, isouter=True).
                order_by(Artist.nombre_artista)
            )
            resultados = session.execute(stmt).mappings().all()
            #print(' index | nombre_artista | nombre_album |')
            #for index, row in enumerate(resultados, 1):
            #    print(f' {index} | {row.nombre_artista} | {row.nombre_album} |')

        except Exception as e:
            session.rollback()
            print(f"ERROR: {e}")
        finally:
            return resultados
            session.close()

    @staticmethod
    def alrtista_album_cancion():
        session = Session()
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
            resultados = session.execute(stmt).mappings().all()
            print(type(resultados))
            #print(' index | nombre_artista | nombre_album | nombre_cancion | fecha_lanzamiento')
            #for index, row in enumerate(resultados, 1):
            #    print(f' {index} | {row.nombre_artista} | {row.nombre_album} | {row.nombre_cancion} | {row.fecha_lanzamiento} |')

        except Exception as e:
            session.rollback()
            print(f"ERROR: {e}")
        finally:
            return resultados
            session.close()

if __name__ == '__main__':

    infor = all_artist()
    print(infor)