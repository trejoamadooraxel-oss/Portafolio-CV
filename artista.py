from psycopg2.extras import execute_values
import psycopg2
import base64
import codecs


class Artist:

    def __init__(self, bd_postgres, cli):
        self.bd_postgres = bd_postgres
        self.cli = cli

    def creacion_table(self):
        try:
            self.cli.execute("""
            CREATE TABLE IF NOT EXISTS artist (
                id_artist BIGSERIAL PRIMARY KEY,
                name_artist TEXT,
                reproduction_top_10 NUMERIC,
                real_name TEXT,
                musical_genre TEXT,
                artist_type TEXT,
                start_carrer DATE,
                end_carrer DATE,
                number_albums NUMERIC,
                followers NUMERIC,
                monthly_listeners NUMERIC
            );
            """)        
            self.bd_postgres.commit()
            print('Tabla verificada/creada correctamente')
        
        except Exception as e:
                print(f'ERROR. No se pudo crear la tabla "artist", {e}')
    
    def insert_registers(self, colums, data ):
        try:
            query = (f"""INSERT INTO artist ({colums}) VALUES %s""")
            execute_values(self.cli, query, data)
            self.bd_postgres.commit()
            print('Registros incertados correctamente')

        except Exception as e:
            print(f'ERROR. No se pudieron insertar los registros a la tabla "artist", {e}')
    
    def columns_table(self, table_name='artist'):
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
            columns_artist = ', '.join(col for col in columns if col != 'id_artist')

            print(f'Las columnas de la tabla {table_name} son: {columns_artist}')
            return columns_artist

        except Exception as e:
            print(f'ERROR. No se pudieron traer las columnas a la tabla "artist", {e}')
            return None

    def select_artist(self):
        try:

            self.cli.execute("""
                select name_artist from artist order by id_artist;
            """)

            #Obtenmos el resultado en una 
            columns = [col[0] for col in self.cli.fetchall()]
            #Une y concatena lo de una lista en un string
            #columns_artist = ', '.join(col for col in columns)

            #print(f'Las columnas de la tabla "artist" son: {columns}')
            return columns
        
        except Exception as e:
            print(f'ERROR. No se pudieron traer las columnas a la tabla "artist", {e}')
            return None