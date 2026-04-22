import conection_postgres as con_postgres
import conection_playwright as con_playwright
import actions_csv_for_db as ccdb
import artista as art
import albunes as alb
import top_song as top
import scraping_spotify

def creation_db_new(host, user, passw, db_name, db_new):
    postgres = con_postgres.conection_postgres_sql(host, user, passw, db_name)
    postgres.create_db(db_new)
    postgres.close_conection()


def process_by_top_ten(host, user, passw, db_name, artists, url, time_await_defaut):    
    
    #Realizamos el scrapeo para obtener los albunes y cerramos la conexion
    conection_p = con_playwright.Conection_playwright()  
    song_top_ten = scraping_spotify.top_ten_song_artists(conection_p, url, artists,time_await_defaut) 
    conection_p.close_browser()
    conection_p.close_conection_p()

    postgres = con_postgres.conection_postgres_sql(host, user, passw, db_name)
    db, cli = postgres.db_cli() 
    actions_csv = ccdb.Acctios_csv()     

    #Guardamos el la informacion en un csv
    actions_csv.creation_csv_top_ten(song_top_ten)

    process_by_artist(host, user, passw, db_name)

  
    #Verificamos que la tabla sea creada
    top_ten = top.Top_songs(db, cli)
    top_ten.creacion_table()

    #Obtenemos la lista de artistas existentes para obtener el id_artist para Foregin Key
    artist = art.Artist(db, cli)
    list_artists = artist.select_artist()

    #Lee los datos almacenados en el archivo que generamos arriba para insertar a la tabla
    data = actions_csv.read_csv_top_ten(list_artists)

    #Obtenemos las columnas de la tabla para pasar a insertar los datos
    columns = top_ten.columns_table()

    #Insertamos a la tabla
    top_ten.insert_registers(columns, data)

    #Cerramos conexion a postgres
    postgres.close_conection()


def process_by_album(host, user, passw, db_name, artists, url, time_await_defaut):    
    conection_p = con_playwright.Conection_playwright()   
    song_albunes = scraping_spotify.all_albuns_artists(conection_p, url, artists,time_await_defaut) 
    conection_p.close_browser()
    conection_p.close_conection_p()

    postgres = con_postgres.conection_postgres_sql(host, user, passw, db_name)
    db, cli = postgres.db_cli() 
    actions_csv = ccdb.Acctios_csv()     

    #Guardamos el la informacion en un csv
    actions_csv.creacion_csv_albunes(song_albunes)

    #Verificamos que la tabla sea creada
    album = alb.Albunes(db, cli)
    album.creacion_table()

    #Obtenemos la lista de artistas existentes para obtener el id_artist para Foregin Key
    artist = art.Artist(db, cli)
    list_artists = artist.select_artist()

    #Lee los datos almacenados en el archivo que generamos arriba para insertar a la tabla
    data = actions_csv.read_csv_album(list_artists)

    #Obtenemos las columnas de la tabla para pasar a insertar los datos
    columns = album.columns_table()

    #Insertamos a la tabla
    album.insert_registers(columns, data)

    #Cerramos conexion a postgres
    postgres.close_conection()


def process_by_artist(host, user, passw, db_name):
    postgres = con_postgres.conection_postgres_sql(host, user, passw, db_name)
    db, cli = postgres.db_cli()
    artista = art.Artist(db, cli)
    artista.creacion_table()
    columns = artista.columns_table()
    actions_csv = ccdb.Acctios_csv() 
    actions_csv.creation_csv_artist(columns.split(','))
    data = actions_csv.read_csv_artist()
    artista.insert_registers(columns, data)
    postgres.close_conection()


def main():
    #Variables
    #--Scrapping
    url = "https://open.spotify.com/"
    time_await_defaut = 1000
    artists = ['Queen', 'Michael Jackson', 'Interpol', 'She Past Away']

    #--Conexion a postgres
    host = 'bG9jYWxob3N0'
    user = 'cG9zdGdyZXM='
    passw = 'SXBob25lLjI3'
    db_name = 'spotify'

    #Funcion que sirve para crear una nueva DB
    #creation_db_new(host,user, passw, 'postgres', 'axel_ejemplo')

    #Funcion para scrapear, crear
    process_by_top_ten(host, user, passw, db_name, artists, url, time_await_defaut)
    process_by_album(host, user, passw, db_name, artists, url, time_await_defaut)
   


if __name__ == "__main__":
    main()
