import sys
import os

from requests import session

# Agrega la carpeta raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import Column, Integer, String, Numeric, Date
from sqlalchemy import insert, delete, select
from conect_sqlalchemy import Base, engine, Session

class Artist(Base):
    __tablename__ = 'artista'

    id_artista = Column(Integer, primary_key=True, autoincrement=True)
    nombre_artista = Column(String(100), nullable=False, unique=True)
    id_spotify = Column(String(100), nullable=False, unique=True)

    def __str__(self):
        return self.username

    def create_table():
        Artist.__table__.create(bind=engine, checkfirst=True)

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
            stmt = select(Artist.id_artista).where(Artist.nombre_artista == value)
            id_artist = session.scalars(stmt).first()
            print(f"El id_artista asociado con {value} es: {id_artist}")
        except Exception as e:
            session.rollback()
            print(f"ERROR: {e}")
        finally:
            session.close()

        return id_artist


