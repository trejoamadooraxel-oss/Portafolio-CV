import soriana_portal as soriana
import conection_playwright as playwright
import conection_postgres as con_postgres
import action_products as actions_products
import actions_csv_for_db as ccdb


def get_url(url):
    conn_play = playwright.Conection_playwright()
    page = conn_play.get_page()
    print(f'Ingresando a la pagina {url}')
    page.goto(url)
    page.wait_for_timeout(1000 * 10)
    return page


def get_information(url):
    try:
        page = get_url(url)
        soriana.search_products(page, url)
    except Exception as e:
        raise (f"Error: en la extraccion de informacion: {e}")


def creation_db_new(host, user, passw, db_name, db_new):
    postgres = con_postgres.conection_postgres_sql(host, user, passw, db_name)
    postgres.create_db(db_new)
    postgres.close_conection()


def process_by_products(host, user, passw, db_name,table):
    postgres = con_postgres.conection_postgres_sql(host, user, passw, db_name)
    db, cli = postgres.db_cli()
    actions_csv = ccdb.Acctios_csv()

    #Verificamos que la tabla sea creada
    products = actions_products.Products(db, cli)
    products.creacion_table(table)

    #Lee los datos almacenados en el archivo que generamos arriba para insertar a la tabla
    data = actions_csv.read_csv_products(db_name)

    #Obtenemos las columnas de la tabla para pasar a insertar los datos
    columns = products.columns_table(table)

    #Insertamos a la tabla
    products.insert_registers(columns, data, table)

    #Cerramos conexion a postgres
    postgres.close_conection()


def main():

    #Conexion y estraccion de datos.
    time_await_defaut = 1000
    url = 'https://www.soriana.com/farmacia/'
    table = str(url).split('/')[-2].replace('-','_')
    get_information(url)

    #Conexion y creacion de bd para postgres
    host = 'bG9jYWxob3N0'
    user = 'cG9zdGdyZXM='
    passw = 'SXBob25lLjI3'
    db_name = 'soriana'
    creation_db_new(host, user, passw, 'postgres', db_name)
    process_by_products(host, user, passw, db_name, table)

    

if __name__ == "__main__":

    main()


