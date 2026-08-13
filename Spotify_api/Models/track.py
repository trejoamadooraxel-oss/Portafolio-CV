
from sqlalchemy import Column, Integer, String, Numeric, Date, ForeignKey
from sqlalchemy import insert, delete, select
from conect_sqlalchemy import Base, engine, AsyncSessionLocal

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