
from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy import insert, select, delete
from Spotify_api.Conexiones.conect_sqlalchemy import Base, engine, AsyncSessionLocal

class Album(Base):
    __tablename__ = 'album'

    id_album = Column(Integer, primary_key=True,  autoincrement=True)
    nombre_album = Column(String(100), nullable=False, unique=True)
    id_spotify = Column(String(100), nullable=False, unique=True)
    fecha_lanzamiento = Column(Date)
    num_canciones = Column(Integer)
    id_artista = Column(Integer, ForeignKey("artista.id_artista"), nullable=False)


    def __str__(self):
        return self.nombre_album

    #def create_table():
    #    Album.__table__.create(bind=engine, checkfirst=True)

    @classmethod
    async def create_table(cls):
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync_conn: cls.__table__.create(bind=sync_conn, checkfirst=True))

    @classmethod
    async def insert_to_table(cls, values):
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(insert(cls), values)
                await session.commit()
                print("Registro insertado correctamente")
            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")

    @classmethod
    async def id_db(cls, value):
        async with AsyncSessionLocal() as session:
            id = None
            try:
                stmt = select(Album.id_album).where(Album.id_spotify == value)
                result = await session.scalars(stmt)
                id = result.first()
                print(f"El id_album asociado con {value} es: {id}")
            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")
            return id

    @classmethod
    async def delete_register_by_id_artista(cls, values):
        async with AsyncSessionLocal() as session:
            try:
                stmt = delete(cls).where(cls.id_artista == values)
                await session.execute(stmt)
                await session.commit()
                print("Tabla 'mas_inf' limpiada correctamente.")
            except Exception as e:
                session.rollback()
                print(f"ERROR al limpiar la tabla: {e}")
                raise

    @classmethod
    async def get_by_id_artist(cls, values):
        async with AsyncSessionLocal() as session:
            registro = None
            try:
                stmt = select(cls).where(cls.id_artista == values)
                result = await session.scalars(stmt)
                registro = result.first()
                print(f"El registro asociado con id_artista {values} es: {registro}")
            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")
                raise

        return registro

