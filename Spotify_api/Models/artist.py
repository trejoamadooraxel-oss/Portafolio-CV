
from sqlalchemy import Column, Integer, String, Numeric, Date
from sqlalchemy import insert, delete, select
from conect_sqlalchemy import Base, engine, AsyncSessionLocal

class Artist(Base):
    __tablename__ = 'artista'

    id_artista = Column(Integer, primary_key=True, autoincrement=True)
    nombre_artista = Column(String(100), nullable=False, unique=True)
    id_spotify = Column(String(100), nullable=False, unique=True)

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
    async def id_db(cls, value):
        async with AsyncSessionLocal() as session:
            id_artist = None
            try:
                stmt = select(Artist.id_artista).where(Artist.nombre_artista == value)
                result = await session.scalars(stmt)
                id_artist = result.first()
                print(f"El id_artista asociado con {value} es: {id_artist}")
            except Exception as e:
                await session.rollback()
                print(f"ERROR: {e}")

        return id_artist
