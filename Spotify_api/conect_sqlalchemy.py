import os
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from dotenv import load_dotenv

load_dotenv('/Users/axel/Documents/Portafolio/Spotify_api/env_var/varaibles_credential.env')
credenciales = os.getenv('SQLALCHEMY_HOST')  # debe iniciar con postgresql+asyncpg://

engine = create_async_engine(
    credenciales,
    echo=True,
    pool_size=10,
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=1800
)


class Base(DeclarativeBase):
    pass


AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False
)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session