from playwright.sync_api import sync_playwright, Error, TimeoutError
from psycopg2.extras import execute_values
import pandas as pd
import psycopg2
import base64
import codecs
import time
import os


def scraping_spotyfy(artist, time_await_defaut, song_caracters, song_albunes,host):

    with sync_playwright() as p:

        try:
            print('Configuramos el navegador.')
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            print(f'Redireccionamos al {host}')
            page.goto(host, timeout=10000)
            page.wait_for_timeout(time_await_defaut * 10)

        except TimeoutError:
            print(f'Timeout: El portal no cargo la url {host}')

        except Error as e:
            print('Error Playwright:', e)

        except Exception as e:
            print('Error inesperado:', e)

        print(f'Buscando a: {artist}')
        try:
            print(f'Ingresamos en el buscador: {artist}.')
            page.locator('[role="combobox"]').click()
            page.wait_for_timeout(time_await_defaut * 3)
            page.locator('[role="combobox"]').fill(f'{artist}')
            page.wait_for_timeout(time_await_defaut * 10)

            #obtenemos la verificacion del artista a buscar de una manera mas desglozada
            print('Valdiando que exista el artista a buscar.')
            card_results = page.locator('[data-testid="top-result-card"]')
            a_tag = card_results.locator('a').first
            artist_tag = a_tag.locator('div')
            name_artist = artist_tag.first.inner_text()
            artist_tag.click()

        except TimeoutError:
            print('Timeout durante la buscar del artista.')

        except Error as e:
            print('Error Playwright durante la busqueda del artista:', e)

        except Exception as e:
            print('Error inesperado durante la bsuqueda del artista:', e)


        #Obtenemos la lista de las canciones mas reproducidas del artista y sus atributos.
        print('Obtenemos las lista de canciones mas escuchadas.')
        try:
            page.get_by_text('Ver más').click()
            page.wait_for_timeout(time_await_defaut * 3)
            card_list_songs = page.locator('[role="presentation"] div[data-testid="tracklist-row"]')
            print(card_list_songs.count())

            for i in range(card_list_songs.count()):

                atributos = card_list_songs.nth(i).locator('[role=gridcell] div[data-encore-id="text"]')
                
                if atributos.count() == 3:
                    #for atributo in range(atributos.count()):
                    song_name = atributos.nth(0).inner_text()
                    song_num_repoduccion = atributos.nth(1).inner_text()
                    song_time = atributos.nth(2).inner_text()
                
                    song_caracters.append({
                        'artist': artist,
                        'name song': song_name.replace(',','').strip(),
                        'num reproduction': song_num_repoduccion.replace(',','').strip(),
                        'time duration': song_time.replace(',','').strip()
                    })

                else:
                    raise('El numero de atributos cambio ya no es 3, revisar el portal')

        except TimeoutError:
            print('Timeout: no apareció el elemento')

        except Error as e:
            print('Error Playwright:', e)

        except Exception as e:
            print('Error inesperado:', e)

        #Obtenemos la discografia del artista y sus atributos.
        print('Obtenemos toda la discografia.')
        try:

            page_discografia = page.locator('[aria-label="Discografía"] span[data-encore-id="text"]')
            page_discografia.click()
            page.wait_for_timeout(time_await_defaut * 3)
            page.get_by_text("Fecha de lanzamiento").click()
            page.wait_for_timeout(time_await_defaut * 3)
            page.get_by_text("Cuadrícula").click()
            page.wait_for_timeout(time_await_defaut * 5)

            all_discografia = page.locator('[role="presentation"] div[data-encore-id="card"]')
            for j in range(all_discografia.count()):
                disc_name = all_discografia.nth(j).locator('[data-encore-id="cardTitle"]').get_attribute('title')
                disc_type = all_discografia.nth(j).locator('[data-encore-id="cardSubtitle"]').text_content()

                song_albunes.append({
                        'artist': artist,
                        'name album': disc_name.replace(',','').strip(),
                        'year': disc_type.split(' ')[0].replace(',','').strip(),
                        'type_disc': disc_type.split(' ')[-1].replace(',','').strip()
                    })


        except TimeoutError:
            print('Timeout: no apareció el elemento')

        except Error as e:
            print('Error Playwright:', e)

        except Exception as e:
            print('Error inesperado:', e)

def creacion_csv_artist(artist,song_caracters,song_albunes,path_fuentes):

    print(f'Iniciando Creacion de csv de albunes para: {artist}')
    df = pd.DataFrame(song_albunes)
    df.to_csv(f'{path_fuentes}/{artist}_albunes.csv',encoding='utf8', index=True)

    print(f'Iniciando Creacion de csv de canciones top para: {artist}')
    df = pd.DataFrame(song_caracters)
    df.to_csv(f'{path_fuentes}/{artist}_top_10.csv',encoding='utf8', index=True)

    

def transformacion_analisis(path_fuente):

    print('Genrando archivo total de Album y Top 10.')    
    archivos = [name for name in os.listdir(path_fuente) if name.endswith(".csv") or name.endswith(".CSV")]

    
    for archivo in archivos:
        
        if 'albunes' in archivo:
            path_album = f'{path_fuente}/Albunes of artist.csv'
            #print('Leyendo archivos de los albunes.')
            df_album = pd.read_csv(f'{path_fuente}/{archivo}', encoding='utf-8', dtype='str')
            
            for columna in df_album.columns:
                df_album[columna] = df_album[columna].apply(lambda x: x.replace(',','').strip())

            df_album[['artist','name album','year','type_disc']].to_csv(path_album,
                                                                        mode='a', 
                                                                        encoding='utf-8', 
                                                                        index=False, 
                                                                        header=not os.path.exists(path_album))

        if 'top' in archivo:
            path_top = f'{path_fuente}/top 10 of artist.csv'
            #print('Leyendo archivos de los top 10.')
            df_top = pd.read_csv(f'{path_fuente}/{archivo}', encoding='utf-8', dtype='str')
            
            for columna in df_top.columns:
                df_top[columna] = df_top[columna].apply(lambda x: x.replace(',','').strip())

            df_top[['artist','name song','num reproduction','time duration']].to_csv(path_top, 
                                                                                     mode='a', 
                                                                                     encoding='utf-8', 
                                                                                     index=False, 
                                                                                     header=not os.path.exists(path_top))
       
    print('Generando archivo principal de artistas.')   
    archivos = [name for name in os.listdir(path_fuente) if name.endswith("top 10 of artist.csv")]
    print(archivos)
    for archivo in archivos:
        df_artist = pd.read_csv(f'{path_fuente}/{archivo}', encoding='utf-8', dtype=str)
        df_artist = df_artist[df_artist["artist"] != "artist"]
        df_artist['num reproduction'] = df_artist['num reproduction'].astype(int)

    df_grouped = df_artist.groupby("artist").agg({
        "num reproduction": "sum"
    }).reset_index()

    columas_new_add = [ 
                'real_name' ,
                'musical_genre',
                'artist_type' ,
                'start_carrer' ,
                'end_carrer' ,
                'number_albums' ,
                'followers' ,
                'monthly_listeners']

    df_grouped = df_grouped.assign(**{col: '' for col in columas_new_add})
    df_grouped.to_csv(f'{path_fuente}/list_artist.csv', encoding='utf-8', index=False, header=True)

def conection_bd(host_postres, user_postgres, pass_postgres, database_postgres):

        #Realizamos la conexion a PostgreSQL a una base ya existente, de preferencia
        #a la que viene por defecto ya que no podemos crear una base de datos sin acceder
        #a una anteiormente.....
        try:
            db_postgres = psycopg2.connect(host=host_postres,
                                        user=user_postgres,
                                        password=pass_postgres,
                                        database=database_postgres)
            
            db_postgres.autocommit = True
            print('Conexion Exitosa...')

        except Exception as e:
            print('Conexion Fallida')
        
        return db_postgres

def close_bd_and_postgres(db_postgres):
    try:
        db_postgres.close()
        print('Cerrando conexion con la bd y postgres.')
    except Exception as e:
        print(f'Error. Error al querrer cerrar la conexion a la bd y postgresSQL: {e}')

def creation_new_database(database_new, db_database):
    try:
        cli = db_database.cursor()
        cli.execute(f'CREATE DATABASE {database_new}')
        bases = cli.fetchall()
        print(bases)

    except Exception as e:
        print(f'Error. Error al crear la base de datos "{database_new}": {e}')
    
    cli.close()
    

def conection_postgresql(host_postres, user_postgres, pass_postgres, database_new):

    db_postgres = conection_bd(codecs.decode(base64.b64decode(host_postres)),
                        codecs.decode(base64.b64decode(user_postgres)),
                        codecs.decode(base64.b64decode(pass_postgres)),
                        codecs.decode(base64.b64decode(user_postgres)))

    creation_new_database(database_new, db_postgres)

    close_bd_and_postgres(db_postgres)

    db_postgres = conection_bd(codecs.decode(base64.b64decode(host_postres)),
                        codecs.decode(base64.b64decode(user_postgres)),
                        codecs.decode(base64.b64decode(pass_postgres)),
                        database_new)

    return db_postgres

def read_csv_principals(path_fuente, csv_artist, csv_top_songs, csv_albunes, table):

    if table == 'artist':    
        print(f'Leyendo archivo {csv_artist} extraer columnas e informacion:')
        archivos = [name for name in os.listdir(path_fuente) if name.endswith(csv_artist)]
        for archivo in archivos:
            df_artist = pd.read_csv(f'{path_fuente}/{archivo}', encoding='utf-8', dtype=str)
            df_artist = df_artist.where(pd.notnull(df_artist), None)

            data = list(df_artist.itertuples(index=False, name=None))

   
    if table == 'top_song':

        print(f'Leyendo archivo {csv_artist} para extraer index:')
        archivos = [name for name in os.listdir(path_fuente) if name.endswith(csv_artist)]
        for archivo in archivos:
            df_artist = pd.read_csv(f'{path_fuente}/{archivo}', encoding='utf-8', dtype=str)
            df_artist = df_artist.where(pd.notnull(df_artist), None)

            artist_name = list(df_artist['artist'])
            artist_map = {artist: i+1 for i, artist in enumerate(artist_name)}

        print(f'Leyendo archivo {csv_top_songs} para informacion y añadir index:')
        archivos = [name for name in os.listdir(path_fuente) if name.endswith(csv_top_songs)]
        for archivo in archivos:
            df_top = pd.read_csv(f'{path_fuente}/{archivo}', encoding='utf-8', dtype=str)
            df_top = df_top.where(pd.notnull(df_top), None)
            
            df_top['id artist'] = df_top['artist'].map(artist_map)

        data = list(df_top.itertuples(index=False, name=None))

    if table == 'albunes':

        print(f'Leyendo archivo {csv_artist} para extraer index:')
        archivos = [name for name in os.listdir(path_fuente) if name.endswith(csv_artist)]
        for archivo in archivos:
            df_artist = pd.read_csv(f'{path_fuente}/{archivo}', encoding='utf-8', dtype=str)
            df_artist = df_artist.where(pd.notnull(df_artist), None)

            artist_name = list(df_artist['artist'])
            artist_map = {artist: i+1 for i, artist in enumerate(artist_name)}
        
        print(f'Leyendo archivo {csv_albunes} para informacion y añadir index:')
        archivos = [name for name in os.listdir(path_fuente) if name.endswith(csv_albunes)]
        for archivo in archivos:
            df_album = pd.read_csv(f'{path_fuente}/{archivo}', encoding='utf-8', dtype=str)
            df_album = df_album.where(pd.notnull(df_album), None)
            
            df_album['id artist'] = df_album['artist'].map(artist_map)

        data = list(df_album.itertuples(index=False, name=None))

    return data
    


class Artist:

    def __init__(self, bd_postgres):
        self.bd_postgres = bd_postgres
        self.cli = bd_postgres.cursor()

    def creacion_table(self):
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
    
class Top_songs:

    def __init__(self, bd_postgres):
        self.bd_postgres = bd_postgres
        self.cli = bd_postgres.cursor()

    def creacion_table(self):
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

class Albunes:

    def __init__(self, bd_postgres):
        self.bd_postgres = bd_postgres
        self.cli = bd_postgres.cursor()

    def creacion_table(self):
        self.cli.execute("""
        CREATE TABLE IF NOT EXISTS albunes (
            id_album BIGSERIAL PRIMARY KEY,
            artist TEXT,
            name_album TEXT,
            year NUMERIC,
            type_disc TEXT,
            id_artist BIGSERIAL,
            FOREIGN KEY (id_artist) REFERENCES artist(id_artist)
        );
        """)
    
        self.bd_postgres.commit()
        print('Tabla verificada/creada correctamente')

class Acction_global:

    def __init__(self, bd_postgres):
        self.bd_postgres = bd_postgres
        self.cli = bd_postgres.cursor()
        

    def see_table(self, table):
        self.cli.execute(f"SELECT * FROM {table};")
        rows = self.cli.fetchall()
        for row in rows:
            print(row)
    
    def drop_table(self, table):
        self.cli.execute(f'DROP TABLE IF EXISTS {table};')
        self.bd_postgres.commit()
        print(f'Tabla "{table}" eliminada')

    def insert_registers(self, table, colums, data ):
        query = (f"""INSERT INTO {table} ({colums}) VALUES %s""")
        execute_values(self.cli, query, data)
        self.bd_postgres.commit()
        print('Registros incertados correctamente')


def main():
    url = "https://open.spotify.com/"
    song_caracters = []
    song_albunes = []
    time_await_defaut = 1000
    artist = 'Kings of Leon'
    path_fuente='/Users/axel/Documents/Portafolio'
    database_new = 'spotify'

    #Parametros PostgreSQL variables codificadas a base64
    host_postgres = 'bG9jYWxob3N0'
    user_postgres = 'cG9zdGdyZXM='
    pass_postgres = 'SXBob25lLjI3'


    #scraping_spotyfy(artist,time_await_defaut,song_caracters, song_albunes,url)
    #creacion_csv_artist(artist,song_caracters,song_albunes,path_fuente)
    #transformacion_analisis(path_fuente)

    #Creamos la conexion para crear y cargar la informacion.
    bd_postgres = conection_postgresql(host_postgres,user_postgres,pass_postgres,database_new)
    #Creamos la tabla de artistas
    
    #Creamos un objeto Artista para mandar a crear la tabla
    #event = Artist(bd_postgres)
    #event.creacion_table()

    #Creamos un objeto Top_songs para mandar a crear la tabla
    event = Albunes(bd_postgres)
    event.creacion_table()


    csv_artist = 'list_artist.csv'
    csv_top_songs = 'top 10 of artist.csv'
    csv_albunes = 'Albunes of artist.csv'

    #solo cambia al final de la funcion que informacion vas a traer, pon el nombre de la tabla
    data = read_csv_principals(path_fuente, csv_artist, csv_top_songs, csv_albunes,'albunes')
    columns_artist = 'name_artist, reproduction_top_10, real_name, musical_genre, artist_type, start_carrer, end_carrer, number_albums, followers, monthly_listeners'
    columns_top = 'name_artist, song_name, num_reproduction, time_duration, id_artist'
    columns_album = 'artist,name_album,year,type_disc,id_artist'
    
    event_global = Acction_global(bd_postgres)
    event_global.insert_registers('albunes',columns_album, data)
    


if __name__ == "__main__":
    main()

