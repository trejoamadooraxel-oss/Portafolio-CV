
from sqlalchemy import Column, Integer, String, ForeignKey, Date
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from Spotify_api.Conexiones.sync_conect_sqlalchemy import Base, engine, SessionLocal



class Sync_Album(Base):
    __tablename__ = 'album'

    id_album = Column(Integer, primary_key=True,  autoincrement=True)
    nombre_album = Column(String(100), nullable=False, unique=False)
    id_spotify = Column(String(100), nullable=False, unique=True)
    fecha_lanzamiento = Column(Date)
    num_canciones = Column(Integer)
    id_artista = Column(Integer, ForeignKey("artista.id_artista"), nullable=False)


    def __str__(self):
        return self.username

    @classmethod
    def create_table(cls):
        try:
            cls.__table__.create(bind=engine, checkfirst=True)
            print("Tabla 'album' verificada/creada correctamente.")
        except Exception as e:
            print(f"ERROR: al crear la tabla: {e}")

    @classmethod
    def insert_to_table(cls, values):
        with SessionLocal() as session:
            try:
                stmt = insert(cls)

                # 2. Si el nombre del id_spotify ya existe, no hacemos nada (evita el rollback por UniqueViolation)
                stmt_on_conflict = stmt.on_conflict_do_nothing(index_elements=['id_spotify'])

                # 3. Ejecutamos pasando el lote de valores
                session.execute(stmt_on_conflict, values)
                session.commit()
                print("Registro insertado correctamente")
            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

    @classmethod
    def id_db(cls, value):
        with SessionLocal() as session:
            id = None
            try:
                stmt = select(Sync_Album.id_album).where(Sync_Album.id_spotify == value)
                result = session.scalars(stmt)
                id = result.first()
                print(f"El id_album asociado con {value} es: {id}")
            except Exception as e:
                session.rollback()
                print(f"ERROR: {e}")

        return id

