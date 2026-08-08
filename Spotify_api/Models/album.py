import sys
import os
# Agrega la carpeta raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import Column, Integer, String, Numeric, Date, ForeignKey
from sqlalchemy import insert, delete, select
from conect_sqlalchemy import Base, engine, Session

class Album(Base):
    __tablename__ = 'album'

    id_album = Column(Integer, primary_key=True,  autoincrement=True)
    nombre_album = Column(String(100), nullable=False, unique=True)
    id_spotify = Column(String(100), nullable=False, unique=True)
    fecha_lanzamiento = Column(Date)
    num_canciones = Column(Integer)
    id_artista = Column(Integer, ForeignKey("artista.id_artista"), nullable=False)


    def __str__(self):
        return self.username

    def create_table():
        Album.__table__.create(bind=engine, checkfirst=True)

    @classmethod
    def insert_to_table(cls, values):
        session = Session()
        try:
            session.execute(insert(cls), values)
            session.commit()
            print("Registro insertado correctamente")
        except Exception as e:
            session.rollback()
            print(f"ERROR: {e}")
        finally:
            session.close()

    @classmethod
    def id_db(cls, value):
        session = Session()
        try:
            stmt = select(Album.id_album).where(Album.id_spotify == value)
            id = session.scalars(stmt).first()
            print(f"El id_album asociado con {value} es: {id}")
        except Exception as e:
            session.rollback()
            print(f"ERROR: {e}")
        finally:
            session.close()

        return id