from sqlalchemy import Column, Integer, String, ForeignKey, Date
from sqlalchemy import insert, select
from Spotify_api.Conexiones.sync_conect_sqlalchemy import Base, engine, SessionLocal
from Spotify_api.Models.sync_album import Sync_Album
from Spotify_api.Models.sync_artist import Sync_Artist
from Spotify_api.Models.sync_trancks import Sync_Track


class Sync_Queries:

    @classmethod
    def all_name_artist(cls):  # Es buena práctica incluir 'cls' en métodos de clase
        with SessionLocal() as session:
            lista = []
            try:
                stmt = (
                    select(Sync_Artist.nombre_artista)
                    .order_by(Sync_Artist.nombre_artista.asc())
                )
                result = session.execute(stmt)

                # scalars().all() extrae directamente el texto del nombre, ignorando la tupla/mapeo
                lista = list(result.scalars().all())

            except Exception as e:
                session.rollback()  # <-- CORREGIDO: Alineado perfectamente con el print
                print(f"ERROR: {e}")

            return lista


