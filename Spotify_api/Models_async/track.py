
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import insert, select, delete
from Spotify_api.Conexiones.conect_sqlalchemy import Base, engine, AsyncSessionLocal
from Spotify_api.Models_async.album import Album

class Track(Base):
    __tablename__ = 'canciones'

    id_cancion = Column(Integer, primary_key=True, autoincrement=True)
    nombre_cancion = Column(String(100), nullable=False)
    id_spotify = Column(String(100), nullable=False, )
    num_cancion = Column(Integer)
    duracion = Column(Integer)
    id_album = Column(Integer, ForeignKey("album.id_album"), nullable=False)


    def __str__(self):
        return self.nombre_cancion

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
    async def id_db(cls, values):
        async with AsyncSessionLocal() as session:
            id = None
            try:
                stmt = select(Track.id_cancion).where(Track.nombre_cancion == values)
                result = await session.scalars(stmt)
                id = result.first()
                print(f"El id_cancion asociado con {values} es: {id}")
            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")

        return id

    @classmethod
    async def ids_spotify_existentes(cls, ids_album):
        async with AsyncSessionLocal() as session:
            result = await session.scalars(
                select(cls.id_spotify).where(cls.id_album.in_(ids_album))
            )
            return set(result.all())

    @classmethod
    async def delete_register_by_id_artista(cls, values):
        async with (AsyncSessionLocal() as session):
            try:
                subq = select(Album.id_album).where(Album.id_artista == values)
                stmt = delete(cls).where(cls.id_album.in_(subq))
                await session.execute(stmt)
                await session.commit()
                print("Tabla 'mas_inf' limpiada correctamente.")
            except Exception as e:
                session.rollback()
                print(f"ERROR al limpiar la tabla: {e}")
                raise

    @classmethod
    async def get_by_id_artista(cls, values):
        async with (AsyncSessionLocal() as session):
            registro = None
            try:
                stmt = (select(cls)
                .join(Album, cls.id_album == Album.id_album)
                .where(Album.id_artista == values))
                result = await session.scalars(stmt)
                registro = result.first()
                print(f"El registro asociado con id_artista {values} es: {registro}")
            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")
                raise

        return registro