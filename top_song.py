from psycopg2.extras import execute_values
import psycopg2
import base64
import codecs


class Top_songs:

    def __init__(self, bd_postgres, cli):
        self.bd_postgres = bd_postgres
        self.cli = cli

    def creacion_table(self):
        try:
            self.cli.execute("""
            CREATE TABLE IF NOT EXISTS top_songs (
                id_top_songs BIGSERIAL PRIMARY KEY,
                name_artist TEXT,
                song_name TEXT,
                num_reproduction NUMERIC,
                time_duration TEXT,
                id_artist BIGSERIAL,
                FOREIGN KEY (id_artist) REFERENCES artist(id_artist)
            );
            """)
        
            self.bd_postgres.commit()
            print('Tabla verificada/creada correctamente')
        
        except Exception as e:
                print(f'ERROR. No se pudo crear la tabla "top_songs", {e}')
        
    def insert_registers(self, colums, data ):
        try:
            query = (f"""INSERT INTO top_songs ({colums}) VALUES %s""")
            execute_values(self.cli, query, data)
            self.bd_postgres.commit()
            print('EXITO. Registros incertados correctamente')

        except Exception as e:
            print(f'ERROR. No se pudieron insertar los registros a la tabla "top_songs", {e}')
    
    def columns_table(self, table_name='top_songs'):
        try:
            self.cli.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                AND table_schema = 'public'
                ORDER BY ordinal_position;
            """, (table_name,))

            #Obtenmos el resultado en una 
            columns = [col[0] for col in self.cli.fetchall()]
            #Une y concatena lo de una lista en un string
            columns_artist = ', '.join(col for col in columns if col != 'id_top_songs')

            print(f'Las columnas de la tabla {table_name} son: {columns_artist}')
            return columns_artist

        except Exception as e:
            print(f'ERROR. No se pudieron traer las columnas a la tabla "albunes", {e}')
            return None
            