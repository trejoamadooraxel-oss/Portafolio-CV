
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import insert, select, delete
from Spotify_api.Conexiones.sync_conect_sqlalchemy import Base, engine, SessionLocal


class Sync_Inf(Base):
    __tablename__ = 'mas_inf'

    id_artista = Column(Integer, ForeignKey("artista.id_artista"), primary_key=True, nullable=False)
    artista = Column(String(100), nullable=False)
    seguidores = Column(Integer)
    escuchas_mensuales = Column(Integer)
    ciudad_oyente_uno = Column(String(100), nullable=False)
    num_oyentes_uno = Column(Integer)
    ciudad_oyente_dos = Column(String(100), nullable=False)
    num_oyentes_dos = Column(Integer)
    ciudad_oyente_tres = Column(String(100), nullable=False)
    num_oyentes_tres = Column(Integer)
    ciudad_oyente_cuatro = Column(String(100), nullable=False)
    num_oyentes_cuatro = Column(Integer)
    ciudad_oyente_cinco = Column(String(100), nullable=False)
    num_oyentes_cinco = Column(Integer)


    def __str__(self):
        return self.username

    @classmethod
    def create_table(cls):
        try:
            cls.__table__.create(bind=engine, checkfirst=True)
            print("Tabla 'mas_inf' verificada/creada correctamente.")
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
    def delete_all(cls):
        with SessionLocal() as session:
            try:
                session.execute(delete(cls))
                session.commit()
                print("Tabla 'mas_inf' limpiada correctamente.")
            except Exception as e:
                session.rollback()
                print(f"ERROR al limpiar la tabla: {e}")
                raise

