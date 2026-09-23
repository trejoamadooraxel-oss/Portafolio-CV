from sqlalchemy import Column, Integer, String
from sqlalchemy import insert, select
from Spotify_api.Conexiones.sync_conect_sqlalchemy import Base, engine, SessionLocal


class Sync_Artist(Base):
    __tablename__ = 'artista'

    id_artista = Column(Integer, primary_key=True, autoincrement=True)
    nombre_artista = Column(String(100), nullable=False, unique=True)
    id_spotify = Column(String(100), nullable=False, unique=True)

    def __str__(self):
        return self.username

    @classmethod
    def create_table(cls):
        """Crea la tabla de forma síncrona en la base de datos"""
        try:
            cls.__table__.create(bind=engine, checkfirst=True)
            print("Tabla 'artista' verificada/creada correctamente.")
        except Exception as e:
            print(f"ERROR: al crear la tabla: {e}")

    @classmethod
    def insert_to_table(cls, values):
        with SessionLocal() as session:
            try:
                session.execute(insert(cls), values)
                session.commit()
                print("Registro insertado correctamente")
            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

    @classmethod
    def id_db(cls, value):
        with SessionLocal() as session:
            id_artist = None
            try:
                stmt = select(Sync_Artist.id_artista).where(Sync_Artist.nombre_artista == value)
                result = session.scalars(stmt)
                id_artist = result.first()
                print(f"El id_artista asociado con {value} es: {id_artist}")
            except Exception as e:
                print(f"ERROR: {e}")

        return id_artist
