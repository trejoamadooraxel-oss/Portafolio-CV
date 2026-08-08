import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime #Tipo de datos que vamos estar manejando
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from dotenv import load_dotenv

load_dotenv('/Users/axel/Documents/Portafolio/Spotify_api/env_var/varaibles_credential.env')
 # reads from a .env file
credenciales = os.getenv('SQLALCHEMY_HOST')
engine = create_engine(credenciales, echo=True)


class Base(DeclarativeBase):
    pass

Session = sessionmaker(bind=engine)

