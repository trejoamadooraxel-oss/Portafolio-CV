import sys
import os
# Agrega la carpeta raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import Column, Integer, String, Numeric, Date, ForeignKey
from sqlalchemy import insert, delete, select
from conect_sqlalchemy import Base, engine, Session

class Track(Base):
    __tablename__ = 'canciones'

    id_cancion = Column(Integer, primary_key=True, autoincrement=True)
    nombre_cancion = Column(String(100), nullable=False)
    id_spotify = Column(String(100), nullable=False, )
    num_cancion = Column(Integer)
    duracion = Column(Integer)
    id_album = Column(Integer, ForeignKey("album.id_album"), nullable=False)


    def __str__(self):
        return self.username

    def create_table():
        Track.__table__.create(bind=engine, checkfirst=True)

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
    def id_db(cls, values):
        session = Session()
        try:
            stmt = select(Track.id_cancion).where(Track.nombre_cancion == values)
            id = session.scalars(stmt).first()
            print(f"El id_cancion asociado con {values} es: {id}")
        except Exception as e:
            session.rollback()
            print(f"ERROR: {e}")
        finally:
            session.close()