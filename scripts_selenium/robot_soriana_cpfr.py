

'''
--------------------------------------------------------------------------

DEPARTMENT: DA "DATA ACQUISITION"
ENVIRONMENT: 2028
PORTAL: STORE CHECK
VERSION: V5
CREATION DATE: 2025/05/02
CHAIN: 00005
DEVELOPER: AXEL TREJO AMADOR
REVIEWED BY: AXEL TREJO AMADOR
CREATION DATE: 2025/07/31
LAST UPDATE: 2025/08/07 - 12:00 PM
UPDATE: 'Se realizo el ajuste en la seccion de descarga'
--------------------------------------------------------------------------

OBSERVACIONES.

  Las descarga se segmenta en 7 partes ya que esta buscando el mes completo.
  siendo:

  'stoche1': del dia 1 al 5
  'stoche2': del dia 6 al 10
  'stoche3': del dia 11 al 15
  'stoche4': del dia 16 al 20
  'stoche5': del dia 21 al 25
  'stoche6': del dia 26 al 28
  'stoche7': del dia 29 al fin de mes

   Tambien ya que cada dia va a mes vencido, lo que se hace para cuando el archivo no encuentre
   dias dentro de un rango hace una comapracion si de un rango a rango no encuentra archivos
   termina la tarea como F, sin bajar y sin pasar por transformacion, si encuentra al menos un
   archivo pasa a transformaicon.

   Las tareas se configuraron en la Catalog_det como "SI" y no como "DA" o "DL", dado que por la
   configuracion creada no permite correr una "DL" sin antes una "SI" esto por que hay subtipes que
   no bajarn nada y yienen que si o si finalizar. A su vez una "SI" no puede mandar
   a generar con la "Next_Order" otra "SI" rompiendo la cadena que si temrina una empiece otra y una
   "DA" no deja finalizar tareas sin entrar al proceso de transformacion de Pablo.

--------------------------------------------------------------------------
'''

import robot_ft
import common
import json
import os
import time
import re
import requests
import base64
import codecs
import sys
import zipfile
import shutil
import calendar
import pandas as pd
import gc
import unicodedata
from datetime import date, timedelta, datetime
from azure.storage.filedatalake import DataLakeServiceClient

# from selenium.webdriver.common.keys import K
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.common.by import By

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

# region global vars

params = None
trace = None
paths = None
driver = None
fileHelper = None
robot_result = None
current_subtask = None

# endregion global vars

# region core no tocar


def end_program(code, message, exception=""):
    global robot_result
    # Exec.upload_files_out(params, paths)
    # driver.quit()
    if exception == "":
        trace.write('I', message)
    else:
        trace.write('E', f'{message}. Exception: {exception}')
    # para que pueda recibir un enum o un int
    try:
        code_val = code.value
    except Exception as e:
        code_val = code
    if code_val != 0:
        status = 'E'
    else:
        status = 'F'

    robot_result.set_status(status, code_val, message)
    robot_result.call_response()
    sys.exit(0)


def get_subtypes_to_process():
    return list(set(list(map(lambda item: item.subtype, list(filter(lambda x: x.status != 'F', params.tasklist))))))


def process_subtype(f_download, subtype, data, allowDateRange):
    global robot_result, current_subtask
    trace.write("I", f"Procesando el subtype: {subtype}")
    result_subtasks = []

    fact_time = list(filter(lambda x: (x.subtype == subtype), params.subtypes_conf))[0].fact_time
    if allowDateRange == common.AllowDateRange.No:
        subtype_tasks = list(filter(lambda x: (x.subtype == subtype and x.status != 'F'), params.tasklist))
        days = set()
        trace.write('I', 'Revisando tareas a ejecutar....')
        sortedtask = sorted(params.tasklist, key=lambda item: item.date)
        for elem in sortedtask:  # conf.tasklist:
            if elem.status != 'F' and elem.subtype == subtype:
                days.add(elem.date)
        error_code = 0
        trace.write("I", f"recorriendo tareas de {subtype}...")
        for date in days:
            current_subtask = common.Subtask(subtype, date)
            trace.write("I", f"date: {date}")
            last_date = ''
            if 'last_date' in data:
                last_date = data['last_date']
            if last_date != '' and date > last_date:
                error_code = common.ResultType.DATA_NOT_AVAILABLE
                error_message = f"La fecha solicitada {date} es mayor a la fecha disponible {last_date}"
                trace.write('E', error_message)
                add_subtask_error(error_code, error_message)
            else:
                try:
                    filename = f'da_{params.chain_id}_{params.supplier_id}_{params.drt_id}_{subtype}_day_{date}_{date}'
                    f_download(subtype, fact_time, filename, date, date)
                    trace.write('I', f'Tarea: {subtype}, date: {date}. Finalizada Ok')
                except Exception as e:
                    error_code = common.ResultType.EXCEPTION
                    error_message = repr(e)
                    trace.write('E', error_message)


def add_subtask_error(error_code, error_message):
    global current_subtask, robot_result
    current_subtask.set_status('E', error_code, error_message)
    robot_result.add(current_subtask)
    robot_result.set_status('E', error_code, error_message)


# endregion core

# region variables

Months = [
    {"name": "enero", "id": 1, "language": "es-AR"},
    {"name": "febrero", "id": 2, "language": "es-AR"},
    {"name": "marzo", "id": 3, "language": "es-AR"},
    {"name": "abril", "id": 4, "language": "es-AR"},
    {"name": "mayo", "id": 5, "language": "es-AR"},
    {"name": "junio", "id": 6, "language": "es-AR"},
    {"name": "julio", "id": 7, "language": "es-AR"},
    {"name": "agosto", "id": 8, "language": "es-AR"},
    {"name": "septiembre", "id": 9, "language": "es-AR"},
    {"name": "octubre", "id": 10, "language": "es-AR"},
    {"name": "noviembre", "id": 11, "language": "es-AR"},
    {"name": "diciembre", "id": 12, "language": "es-AR"},
    {"name": "january", "id": 1, "language": "en-US"},
    {"name": "february", "id": 2, "language": "en-US"},
    {"name": "march", "id": 3, "language": "en-US"},
    {"name": "april", "id": 4, "language": "en-US"},
    {"name": "may", "id": 5, "language": "en-US"},
    {"name": "june", "id": 6, "language": "en-US"},
    {"name": "july", "id": 7, "language": "en-US"},
    {"name": "august", "id": 8, "language": "en-US"},
    {"name": "september", "id": 9, "language": "en-US"},
    {"name": "october", "id": 10, "language": "en-US"},
    {"name": "november", "id": 11, "language": "en-US"},
    {"name": "deceember", "id": 12, "language": "en-US"},
]


# endregion variables

# region functions

def close_note():
    driver.implicitly_wait(2)
    try:
        modales = driver.find_elements(By.XPATH, '//div[@class="v-window-closebox"]')
        while len(modales) > 0:
            ActionChains(driver).click(modales[0]).key_down(Keys.ENTER).perform()
            modales = driver.find_elements(By.XPATH, '//div[@class="v-window-closebox"]')
    except Exception as e:
        pass


def clearn_paths(carpetas):
    # Verifica si la carpeta existe
    for carpeta in carpetas:
        # Verifica si la carpeta existe
        if os.path.exists(carpeta):
            print(f"Procesando carpeta: {carpeta}")
            # Lista todos los archivos y carpetas en la carpeta
            for item in os.listdir(carpeta):
                ruta_item = os.path.join(carpeta, item)
                # Si es un archivo, lo elimina
                if os.path.isfile(ruta_item):
                    os.remove(ruta_item)
                    trace.write('I', f"Archivo {item} eliminado.")
                # Si es un directorio, lo elimina junto con su contenido
                elif os.path.isdir(ruta_item):
                    shutil.rmtree(ruta_item)
                    trace.write('I', f"Carpeta {item} y su contenido eliminados.")
        else:
            print(f"La carpeta {carpeta} no existe.")


def select_date(date):
    time.sleep(3)
    calendarDate = driver.find_element(By.XPATH, '//span[@class="v-datefield-calendarpanel-month"]').text

    for x in range(int(date[-4::]), int(calendarDate[-4::])):
        time.sleep(1)
        driver.find_element(By.XPATH, '//button[@aria-label="Previous year"]').click()

    dateMonth = list(filter(lambda x: x['name'] == calendarDate[0:-5].lower(), Months))[0]['id']
    if (int(date[3:5]) > dateMonth):
        aria_label_month = "Next month"
    else:
        aria_label_month = "Previous month"
    for m in range(int(date[3:5]), dateMonth):
        time.sleep(1)
        driver.find_element(By.XPATH, f'//button[@aria-label="{aria_label_month}"]').click()

    try:
        if int(date[:2]) > 15:
            driver.find_elements(By.XPATH, f'//td[@role="gridcell"]/span[text()="{int(date[:2])}"]')[-1].click()
        else:
            driver.find_elements(By.XPATH, f'//td[@role="gridcell"]/span[text()="{int(date[:2])}"]')[0].click()
    except Exception as e:
        raise common.FactException(common.ResultType.DATA_NOT_AVAILABLE,
                                   f'El archivo para el dia {date} no esta disponible')


def login_complete():
    for i in range(60):
        if driver.current_url == "https://b2b.smu.cl/BBRe-commerce/main":
            return True
        time.sleep(2)
    raise Exception("Se paso el tiempo de espera para la resolucion del captcha!")


def login():
    try:
        driver.implicitly_wait(10)
        # time.sleep(5)
        driver.switch_to.default_content()
        driver.find_element(By.ID, "username").send_keys(params.username)
        time.sleep(1)
        driver.find_element(By.ID, "password").send_keys(codecs.decode(base64.b64decode(params.password)))
        time.sleep(1)

        driver.find_element(By.XPATH, '//button[text()="Login"]').click()
        time.sleep(2)
        try:
            driver.find_element(By.ID, 'invalidError')
            fileHelper.save_screen(driver)
            end_program(common.ResultType.USER_ERROR, 'Error en login!')
        except Exception as e:
            trace.write('I', "Login ok!")
        time.sleep(5)

    except Exception as e:
        fileHelper.save_screen(driver)
        trace.write('E', 'Finalizado inicio de sesion con error!')
        end_program(common.ResultType.EXCEPTION, 'Finalizado con Error Desconocido.', f'Exception: {repr(e)}')


def login_captcha():
    try:
        driver.implicitly_wait(10)
        if not params.wait:
            try:
                res_captcha = common.res_captcha_sound(driver, trace, fileHelper, paths, params)
                if os.name != "nt" and not res_captcha:
                    fileHelper.save_screen(driver)
                    end_program(common.ResultType.CAPTCHA_RESOLVE, 'No se pudo resolver el Captcha!')
            except NoSuchElementException:
                try:
                    driver.find_element(By.XPATH, '//h1[text()="Verify Your Identity"]')
                    fileHelper.save_screen(driver)
                    end_program(common.ResultType.EXCEPTION, 'Error la pagina no cargo completamente el captcha')
                except NoSuchElementException:
                    trace.write('E', 'Ya no hay mensaje de validacion de datos.')
            except Exception as e:
                fileHelper.save_screen(driver)
                end_program(common.ResultType.EXCEPTION, 'Finalizado con Error Desconocido',
                            f'Exception: {e}')

        # time.sleep(5)
        driver.switch_to.default_content()
        driver.find_element(By.ID, "username").send_keys(params.username)
        driver.find_element(By.ID, "password").send_keys(codecs.decode(base64.b64decode(params.password)))

        if params.wait:
            login_complete()
        else:
            driver.find_element(By.ID, "kc-login").click()
            time.sleep(2)
            try:
                driver.find_element(By.ID, 'input-error')
                fileHelper.save_screen(driver)
                end_program(common.ResultType.USER_ERROR, 'Error en login!')
            except Exception as e:
                trace.write('I', "Login ok!")
            time.sleep(5)

    except Exception as e:
        fileHelper.save_screen(driver)
        trace.write('E', 'Finalizado inicio de sesion con error!')
        end_program(common.ResultType.EXCEPTION, 'Finalizado con Error Desconocido.', f'Exception: {repr(e)}')


# endregion functions


def dias_en_mes(anio, mes):
    if mes < 1 or mes > 12:
        raise ValueError("Mes debe estar entre 1 y 12")
    return calendar.monthrange(anio, mes)[1]


def comprimir_files(carpeta_origen, nombre_zip, extencion):
    ruta_zip = os.path.join(carpeta_origen, nombre_zip)

    # Crear el archivo zip en la misma carpeta
    with zipfile.ZipFile(ruta_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for archivo in os.listdir(carpeta_origen):
            if archivo.endswith(extencion):
                ruta_completa = os.path.join(carpeta_origen, archivo)
                # Añadir al zip, pero sin incluir la ruta completa
                zipf.write(ruta_completa, arcname=archivo)

    print(f'Archivo zip creado en: {ruta_zip}')


def descomprimi_files(carpeta_origen, nombre_zip):
    ruta_completa = os.path.join(carpeta_origen, nombre_zip)
    with zipfile.ZipFile(ruta_completa, 'r') as zip_ref:
        zip_ref.extractall(carpeta_origen)


def consulta_de_archivos(fecha_search, num_archivos):

    cadena = f'{params.extra_params.report_base}{fecha_search}{params.extra_params.extencion}'
    directory = str(params.extra_params.paths_azure)
    url = f"{params.host}/{directory}/{cadena}{params.password}"
    trace.write('I', f"URL: {url}")

    response = requests.get(url)

    # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        # Ruta local donde deseas guardar el archivo
        file_path = os.path.join(paths.source, cadena)
        # Cambia el nombre y la extensión según el tipo de archivo
        # print(file_path)
        # Guardar el contenido del archivo en el sistema local
        with open(file_path, 'wb') as file:
            file.write(response.content)  # Escribir el contenido binario del archivo

        trace.write('I', f"Archivo {cadena} descargado.")
        fileHelper.wait_download(num_archivos, f'{str(params.extra_params.extencion).split(".")[1]}')


    else:
        trace.write('I', f"No se encontro el archvo {cadena}.")

    """files_parquets = [name for name in os.listdir(paths.source) if name.endswith(".parquet")]

    if len(files_parquets) == 0:
        download_status = False
    else:
        download_status = True

    return download_status"""


def download_report_catalogos(subtype, fact_time, filename, date_from, date_to):
    global robot_result, current_subtask
    trace.write('I', f'Comenzando descarga de {subtype}, fecha: {date_from}.')

    fileHelper.clear_paths([paths.temp, paths.source, paths.transformed])
    # clearn_paths([paths.temp, paths.source])
    fecha = datetime.strptime(str(date_from), '%Y-%m-%d').strftime('%d%m%Y')

    num_archivos = 1
    step = ""

    try:
        # Otenemos el numero del mes y lo ocupamos para poder obtener el numero de dias
        if subtype == 'catclie':
            consulta_de_archivos(fecha, num_archivos)

        


        comprimir_files(paths.source, f'{filename}.zip', params.extra_params.extencion)

        fileHelper.send_to_gs(paths.source, filename, 'zip', params.bucket_src, False)

        # fileHelper.rename_files(filename, paths.source, 'csv')

        # fileHelper.save_download_file(driver, f'{filename}', 'zip', num_archivos)
        pre_transforms(subtype, date_from, filename)
        trace.write('I', 'Proceso de navegacion  Finalizado.')

        # trace.write('I', 'Subiendo el archivo al bucket de salida.')
        # fileHelper.send_to_gs(paths.transformed, filename, 'csv', params.bucket_gen, False)

        # produccion
        current_subtask.set_src([filename + '.csv'], 'csv')

        robot_ft.transform_report(trace, params, paths, fileHelper, current_subtask, common.ZipType.NONE)
        robot_result.add(current_subtask)

    except common.FactException as e:
        fileHelper.save_screen(driver)
        error_code = common.ResultType.TRANSFORM_ERROR
        error_message = repr(e)
        add_subtask_error(error_code, error_message)
        raise common.FactException(e.code, e.message)
    except common.TransformException as e:
        error_code = common.ResultType.TRANSFORM_ERROR
        error_message = repr(e)
        add_subtask_error(error_code, error_message)
        raise common.TransformException(e.code, e.message)
    except Exception as e:
        fileHelper.save_screen(driver)
        error_code = common.ResultType.TRANSFORM_ERROR
        error_message = repr(e)
        add_subtask_error(error_code, error_message)
        trace.write('E', f'Error en el paso: {step}. Finalizado con error en area de descarga. Excepcion: {repr(e)}')
        raise common.FactException(common.ResultType.EXCEPTION.value, 'Finalizado con Error Desconocido')


def obtener_campos(subtype):
    conf_subtype = list(filter(lambda x: x.subtype == subtype, params.subtypes_conf))[0]
    fields = conf_subtype.fields
    name_columns = [field.file_name for field in fields]

    return name_columns


def pre_transforms(subtype, date_from, filename):
    file_name_out = f'{filename}.csv'
    try:
        trace.write('I', f'Iniciando proceso de pre-transformación para {subtype}.')
        file_path = os.path.join(paths.source, file_name_out)

        name_columns = obtener_campos(subtype)

        files_csvs = [name for name in os.listdir(paths.source) if name.endswith(".csv")]
        for file_csv in files_csvs:
            df = pd.read_csv(paths.source + file_csv, encoding='utf-8', dtype=str)

            df.fillna('', inplace=True)

            # Limpiamos las columnas de acentos
            columnas_sin_acentos = []
            for columna_df in df.columns:
                texto_normalizado = unicodedata.normalize('NFD', columna_df)
                texto_sin_acentos = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')
                columnas_sin_acentos.append(texto_sin_acentos)
            df.columns = columnas_sin_acentos

            for column in df.columns:
                df.columns = columnas_sin_acentos
                df[column] = df[column].apply(lambda x: str(x).replace(',', '').strip())

            id_name = file_csv.split('_')[-1]
            date_file = id_name.split('.')[0]
            df['rm_time_id_file'] = datetime.strptime(str(date_file), '%d%m%Y').strftime('%Y-%m-%d')

            if (sorted(name_columns) == sorted(df.columns)):
                df.to_csv(file_path, encoding='utf-8', header=False, index=False, mode='a')

            else:
                trace.write('E', f'Las columas no coinciden')
                trace.write('E', f'Columas del archivo: {df.columns}')
                trace.write('E', f'Columas del json: {name_columns}')
                raise Exception('E', f'Las columas no coinciden')

            # Eliminar explícitamente el DataFrame

            trace.write('I', f'Archivo {file_name_out} Generado')
            #os.remove(paths.source + file_csv)

            # Eliminar explícitamente el DataFrame
            del df_all
            gc.collect()



    except Exception as e:
        trace.write('E', f'{e}')
        raise Exception(f'Ocurrio un error en la 2da. Transformacion: {e}')


def set_driver(params, trace, paths):
    trace.write('I', 'configurando driver.')
    chrome_options = Options()
    # if os.name != "nt":
    #    chrome_options.add_argument("--headless")
    # chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_argument(
        '--disable-dev-shm-usage')  # utilizado para evitar errores en el docker por falta de memoria
    chrome_options.add_argument("-allow-running-insecure-content")
    prefs = {
        "download.default_directory": paths.source,
        "download.directory_upgrade": True,
        "download.prompt_for_download": False,
        "download.extensions_to_open": 'zip:csv:xls:xlsx:exe:txt',
        "safebrowsing.enabled": True
    }

    chrome_options.add_experimental_option("prefs", prefs)
    # chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36")
    chrome_options.add_argument("--safebrowsing-disable-download-protection")
    chrome_options.add_argument("safebrowsing-disable-extension-blacklist")
    trace.write('I', 'Iniciando driver.')

    serv = Service(r'C:\\chromedriver.exe')
    if os.name == "nt":
        driver = webdriver.Chrome(service=serv, options=chrome_options)
    else:
        driver = webdriver.Chrome(options=chrome_options)

    try:
        trace.write('I', 'Version: 1.0. Driver Listo')
        trace.write('I', 'Accediendo a {}.'.format(params.host))
        driver.get(params.host)
        time.sleep(10)

    except Exception as e:
        pass
        # end_program(common.ResultType.EXCEPTION, 'Finalizado con Error Desconocido.', f'Exception: {e}')
    return driver


def main():
    global trace, params, paths, fileHelper, driver, robot_result

    params = common.load_json_params()
    trace = common.Trace(params)
    paths = common.Paths()
    fileHelper = common.FileHelper(trace, paths, params)

    # comprimir_files(paths.source, f'Fuentes.zip', 'csv')
    # descomprimi_files(paths.source, 'Fuentes.zip')
    # print(paths.transformed)
    # filename = 'da_00005_01_1_stoche1_day_2025-07-30_2025-07-30'
    # pre_transforms('stoche1', '2025-07-31', filename)
    # curr = common.Subtask('stoche1', '2025-07-29')
    # curr.set_src([filename + '.csv'], 'csv')

    # robot_ft.transform_report(trace, params, paths, fileHelper, curr, common.ZipType.NONE)

    # Siempre se ven asi
    error_code = 0
    robot_result = common.Result(params)

    subtypes = get_subtypes_to_process()
    trace.write("I", f"subtypes a procesar: {subtypes}")

    last_date_inoh = ''
    last_date_sale = ''
    last_date_reca = ''

    for subtype in subtypes:
        match subtype:
            case 'catclie':
                gc.collect()
                process_subtype(download_report_catalogos, subtype, last_date_inoh, common.AllowDateRange.No)

            case _:
                result_status = {"status": "E", "status_code": 910,
                                 "status_message": f"No esta definido el subtype {subtype} en el codigo del robot!"}
                pass

    end_program(error_code, 'Tarea Finalizada.')


if __name__ == "__main__":
    main()
