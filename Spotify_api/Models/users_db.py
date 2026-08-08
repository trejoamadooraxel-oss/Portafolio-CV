import sys
import os

# Agrega la carpeta raíz al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import insert, delete, select, text
from conect_sqlalchemy import Base, engine, Session

class DB_admin:

    @staticmethod
    def crear_usuario(nom_user, contrasenia):
        session = Session()
        try:
            session.execute(text(f"CREATE USER {nom_user} WITH PASSWORD '{contrasenia}'"))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"ERROR al otorgar permisos: {e}")
        finally:
            session.close()

    @staticmethod
    def otorgar_permisos(nom_user, tabla, permisos):
        permisos_str = ', '.join(permisos)
        session = Session()
        try:
            session.execute(text(f"GRANT {permisos_str} ON TABLE {tabla} TO {nom_user}"))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"ERROR al otorgar permisos: {e}")
        finally:
            session.close()

    @staticmethod
    def quitar_permisos(nom_user, tabla, permisos):
        permisos_str = ', '.join(permisos)
        session = Session()
        try:
            session.execute(text(f"REVOKE {permisos_str} ON TABLE {tabla} FROM {nom_user}"))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"ERROR al otorgar permisos: {e}")
        finally:
            session.close()

    @staticmethod
    def otorgar_persimos_db(nom_user):
        session = Session()
        try:
            session.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {nom_user}"))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"ERROR al otorgar permisos: {e}")
        finally:
            session.close()

    @staticmethod
    def usar_usuario(nom_user, contrasenia):
        return 0



if __name__ == '__main__':

    #crear_usuario('omar', 'omarcitogluglu')

    permisos = ['INSERT', 'UPDATE', 'DELETE']
    otorgar_persimos_db('omar')
