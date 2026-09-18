import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#from dotenv import load_dotenv
#load_dotenv('/Users/axel/Documents/Portafolio/Spotify_api/env_var/varaibles_credential.env')

credenciales = os.getenv('SQLALCHEMY_HOST_SYNC')

# Creamos el motor síncrono tradicional
engine = create_engine(
    credenciales,
    echo=True,
    pool_size=10,
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=1800
)

Base = declarative_base()

# Configuramos el administrador de sesiones síncronas estándar
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False
)

def get_db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()