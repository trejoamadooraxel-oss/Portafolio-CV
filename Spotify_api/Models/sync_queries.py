from sqlalchemy import Column, Integer, String, ForeignKey, Date
from sqlalchemy import insert, select
from Spotify_api.Conexiones.sync_conect_sqlalchemy import Base, engine, SessionLocal
from Spotify_api.Models.sync_album import Sync_Album
from Spotify_api.Models.sync_artist import Sync_Artist
from Spotify_api.Models.sync_trancks import Sync_Track
from Spotify_api.Models.sync_more_inf import Sync_Inf


class Sync_Queries:

    @classmethod
    def all_name_artist(cls):
        with SessionLocal() as session:
            lista = []
            try:
                stmt = (
                    select(Sync_Artist.nombre_artista)
                    .order_by(Sync_Artist.nombre_artista.asc())
                )
                result = session.execute(stmt)

                lista = list(result.scalars().all())

            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

            return lista


    @classmethod
    def artist_missing(cls):
        with SessionLocal() as session:
            lista = []
            try:
                stmt = (
                    select(Sync_Artist.nombre_artista)
                    .outerjoin(Sync_Inf, Sync_Artist.nombre_artista==Sync_Inf.artista)
                    .where(Sync_Inf.artista.is_(None)))
                result = session.execute(stmt)

                lista = list(result.scalars().all())

            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

            return lista

    @classmethod
    def all_inf_artist(cls):
        with SessionLocal() as session:
            lista = []
            try:
                stmt = (select(Sync_Artist))
                result = session.execute(stmt)

                for registro in result.scalars().all():
                    lista.append({c.name: getattr(registro, c.name) for c in Sync_Artist.__table__.columns})

            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

            return lista

    @classmethod
    def all_inf_album(cls):
        with SessionLocal() as session:
            lista = []
            try:
                stmt = (select(Sync_Album))
                result = session.execute(stmt)

                for registro in result.scalars().all():
                    lista.append({c.name: getattr(registro, c.name) for c in Sync_Album.__table__.columns})

            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

            return lista

    @classmethod
    def all_inf_tracks(cls):
        with SessionLocal() as session:
            lista = []
            try:
                stmt = (select(Sync_Track))
                result = session.execute(stmt)

                for registro in result.scalars().all():
                    lista.append({c.name: getattr(registro, c.name) for c in Sync_Track.__table__.columns})

            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

            return lista

    @classmethod
    def all_inf_mas_inf(cls):
        with SessionLocal() as session:
            lista = []
            try:
                stmt = (select(Sync_Inf))
                result = session.execute(stmt)

                for registro in result.scalars().all():
                    lista.append({c.name: getattr(registro, c.name) for c in Sync_Inf.__table__.columns})

            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

            return lista


