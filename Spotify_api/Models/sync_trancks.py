
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import insert, select
from Spotify_api.Conexiones.sync_conect_sqlalchemy import Base, engine, SessionLocal


class Sync_Track(Base):
    __tablename__ = 'canciones'

    id_cancion = Column(Integer, primary_key=True, autoincrement=True)
    nombre_cancion = Column(String(100), nullable=False)
    id_spotify = Column(String(100), nullable=False, )
    num_cancion = Column(Integer)
    duracion = Column(Integer)
    id_album = Column(Integer, ForeignKey("album.id_album"), nullable=False)


    def __str__(self):
        return self.username

    @classmethod
    def create_table(cls):
        try:
            cls.__table__.create(bind=engine, checkfirst=True)
            print("Tabla 'canciones' verificada/creada correctamente.")
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
    def id_db(cls, values):
        with SessionLocal() as session:
            id = None
            try:
                stmt = select(Sync_Track.id_cancion).where(Sync_Track.nombre_cancion == values)
                result = session.scalars(stmt)
                id = result.first()
                print(f"El id_cancion asociado con {values} es: {id}")
            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

        return id