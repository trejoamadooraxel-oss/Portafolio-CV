from psycopg2.extras import execute_values
import psycopg2
import base64
import codecs

class conection_postgres_sql():
    def __init__(self, host, user, passw, name_db):
        self.db_postgres = psycopg2.connect(host = codecs.decode(base64.b64decode(host)),
                                        user = codecs.decode(base64.b64decode(user)),
                                        password = codecs.decode(base64.b64decode(passw)),
                                        database = name_db)
        self.db_postgres.autocommit = True
        self.cli = self.db_postgres.cursor()

    def db_cli(self):
        return self.db_postgres, self.cli, 
        
    def create_db(self, db_new):
        try:
            self.cli.execute(f'CREATE DATABASE {db_new}')
            print(f'EXITO. La nueva database {db_new} se creo exitosamente.')
        except Exception as e:
            print(f'ERROR. La nueva database {db_new} no pudo ser creada: {e}.')
    
    def delete_db(self, db):

        try:
            self.cli.execute(f'DROP DATABASE {db}')
            print(f'EXITO. La database {db} se elimino exitosamente.')
        except Exception as e:
            print(f'ERROR. La database {db} no pudo ser eliminda: {e}.')
    
    def close_conection(self):
        try:
            self.db_postgres.close()
            print('EXITO. Se cerro la conexion con la bd y postgres.')
        except Exception as e:
            print(f'ERROR. Error al querrer cerrar la conexion a la bd y postgresSQL: {e}')