from psycopg2.extras import execute_values
import psycopg2
import base64
import codecs


class Products:

    def __init__(self, bd_postgres, cli):
        self.bd_postgres = bd_postgres
        self.cli = cli

    def creacion_table(self,table):
        try:
            self.cli.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                categoria TEXT,
                sub_categoria TEXT,
                nombre_product TEXT,
                sku TEXT,
                url_general TEXT,
                url_product TEXT,
                price_original DOUBLE PRECISION,
                precio_oferta DOUBLE PRECISION,
                promo_producto TEXT,
                type_promo_by_product TEXT,
                bonus_product TEXT,
                type_bonus_by_product TEXT, 
                time_update DATE 
            );
            """)
        
            self.bd_postgres.commit()
            print('EXITO. Tabla verificada/creada correctamente')

        except Exception as e:
            print(f'ERROR. No se pudo crear la tabla "albunes", {e}')

    def insert_registers(self, colums, data, table ):
        try:
            query = (f"""INSERT INTO {table} ({colums}) VALUES %s""")
            execute_values(self.cli, query, data)
            self.bd_postgres.commit()
            print('EXITO. Registros incertados correctamente')

        except Exception as e:
            print(f'ERROR. No se pudieron insertar los registros a la tabla "{table}", {e}')
    
    def columns_table(self, table_name='albunes'):
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
            columns_artist = ', '.join(col for col in columns if col != 'id_album')

            print(f'EXITO. Las columnas de la tabla {table_name} son: {columns_artist}')
            return columns_artist
        
        except Exception as e:
            print(f'ERROR. No se pudieron traer las columnas a la tabla "albunes", {e}')
            return None