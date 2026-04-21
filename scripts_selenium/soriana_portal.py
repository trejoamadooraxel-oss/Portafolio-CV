
'''
# ------------------------------------------------------------------------------------------------------------------- #

DEPARTMENT: DA "DATA ACQUISITION"
ENVIRONMENT: 2031, 2052
PORTAL: SORIANA PORTAL
VERSION: V5
CREATION DATE: 2025/09/09
CHAIN: 00004
DEVELOPER: AXEL TREJO AMADOR
REVIEWED BY: AXEL TREJO AMADOR
CREATION DATE: 2025/09/09
LAST UPDATE DEVELOPER: AXEL TREJO AMADOR
LAST UPDATE: 2025/09/08 00:00
UPDATE: Se migro a V5.

# ------------------------------------------------------------------------------------------------------------------- #
'''
# UPDATE
'''
# ------------------------------------------------------------------------------------------------------------------- #
UPDATE:
    |Ambiente -> DATE UPDATE -> Descripcion.|
    
    - GENERAL -> 2025/09/09 -> Se migro a V5.

    
# ------------------------------------------------------------------------------------------------------------------- #
'''
# OBSERVACIONES GENERALES.
'''
# ------------------------------------------------------------------------------------------------------------------- #

OBSERVACIONES GENERALES.

    2031
       |--> 02
            |--> sori00v                                    |--> sori00b                                    |--> sori00i                       
            |--> codigo_tienda = pos_import                 |--> codigo_tienda = pos_import                 |--> codigo_tienda = pos_import                       
            |--> codigo_de_barras = prod_import             |--> codigo_de_barras = prod_import             |--> codigo_de_barras = prod_import                        
            |--> venta_pesos = sale_price                   |--> venta_pesos = sale_price                   |--> inventario_actual = inoh_amount                
            |--> venta_unidades = sale_amount               |--> venta_unidades = sale_amount               |--> 0.0 = inoh_price             
       
    2052
       |--> 00
            |--> sori00v                                    |--> sori00b                                    |--> sori00i                       
            |--> codigo_tienda = pos_import                 |--> codigo_tienda = pos_import                 |--> codigo_tienda = pos_import                       
            |--> codigo_de_barras = prod_import             |--> codigo_de_barras = prod_import             |--> codigo_de_barras = prod_import                        
            |--> venta_pesos = sale_price                   |--> venta_pesos = sale_price                   |--> inventario_actual = inoh_amount                
            |--> venta_unidades = sale_amount               |--> venta_unidades = sale_amount               |--> 0.0 = inoh_price             

--------------------------------------------------------------------------
'''


import robot_ft
import common
import json
import os
import time
import calendar
import requests
import unicodedata
import base64
import codecs
import sys
import zipfile
import shutil
import pandas as pd
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
#from azure.storage.filedatalake import DataLakeServiceClient

#from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException, JavascriptException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
#from selenium.webdriver.common.action_chains import ActionChains
#from selenium.webdriver.common.by import By

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
    #Exec.upload_files_out(params, paths)
    #driver.quit()
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
    return list(set(list(map(lambda item: item.subtype, list(filter(lambda x:  x.status != 'F', params.tasklist))))))


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
           { "name": "enero", "id":1, "language":"es-AR" },
           { "name": "febrero", "id":2, "language":"es-AR" },
           { "name": "marzo", "id":3, "language":"es-AR" },
           { "name": "abril", "id":4, "language":"es-AR" },
           { "name": "mayo", "id":5, "language":"es-AR" },
           { "name": "junio", "id":6, "language":"es-AR" },
           { "name": "julio", "id":7, "language":"es-AR" },
           { "name": "agosto", "id":8, "language":"es-AR" },
           { "name": "septiembre", "id":9, "language":"es-AR" },
           { "name": "octubre", "id":10, "language":"es-AR" },
           { "name": "noviembre", "id":11, "language":"es-AR" },
           { "name": "diciembre", "id":12, "language":"es-AR" },
           { "name": "january", "id":1, "language":"en-US" },
           { "name": "february", "id":2, "language":"en-US" },
           { "name": "march", "id":3, "language":"en-US" },
           { "name": "april", "id":4, "language":"en-US" },
           { "name": "may", "id":5, "language":"en-US" },
           { "name": "june", "id":6, "language":"en-US" },
           { "name": "july", "id":7, "language":"en-US" },
           { "name": "august", "id":8, "language":"en-US" },
           { "name": "september", "id":9, "language":"en-US" },
           { "name": "october", "id":10, "language":"en-US" },
           { "name": "november", "id":11, "language":"en-US" },
           { "name": "deceember", "id":12, "language":"en-US" },
           ]

#endregion variables

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

    passw = codecs.decode(base64.b64decode(params.password))

    try:
        WebDriverWait(driver, 30).until(EC.visibility_of_element_located((
            By.XPATH, '//input[@placeholder="Correo"]')))

        driver.find_element(By.XPATH, '//input[@placeholder="Correo"]').send_keys(params.username)
        time.sleep(2)
        driver.find_element(By.XPATH, '//input[contains(@placeholder, "Contrase")]').send_keys(passw)
        time.sleep(2)
        driver.find_element(By.XPATH, '//button[@id="__button1"]').click()
        time.sleep(2)

    except NoSuchElementException as e:
        # FileHelper.save_screen(driver, paths, conf)
        trace.write('E', f'Finalizado Error: No se encntro algun elemento para iniciar sesion.')
        end_program(common.ResultType.EXCEPTION, 'Finalizado Error: No se encntro algun elemento para iniciar sesion.')

    except TimeoutException as e:
        # FileHelper.save_screen(driver, paths, conf)
        trace.write('E', f'Finalizado Error: El portal esta tardando mas de 30 segundos en cargar.')
        end_program(common.ResultType.EXCEPTION, 'Finalizado Error: No se encntro algun elemento para iniciar sesion.')

    except Exception as e:
        trace.write('E', f'Finalizado Error: Desconocido {e}.')
        end_program(common.ResultType.EXCEPTION, f'Finalizado Error: Desconocido {e}.')

    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((
            By.XPATH, '//span[contains(text(), "Usuario o contrase") and contains(text(), "a incorrectos.")]')))
        end_program(common.ResultType.USER_ERROR, 'Finalizado con Error Usuario y/o Password Incorrecto.', 'Error')
    except TimeoutException:
        trace.write('E', f'Acceso Correcto.')


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

        #time.sleep(5)
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
        end_program(common.ResultType.EXCEPTION, 'Finalizado con Error Desconocido.',  f'Exception: {repr(e)}')


def obtener_campos_extra_params():

    extra_params_metrics = params.extra_params

    try:
        proveedor = extra_params_metrics.proveedor
    except:
        proveedor = ''

    return proveedor

def obtener_campos(subtype):

    conf_subtype = list(filter(lambda x: x.subtype == subtype, params.subtypes_conf))[0]

    # Obtenmos el cambio
    fields = conf_subtype.fields
    name_columns = [field.file_name for field in fields]
    lista_columnas = [col for col in name_columns]
    numeric_columns = [field.file_name for field in fields if field.bd_type == 'FLOAT64']

    try:
        if conf_subtype.columns_cero:
            colums_cero = conf_subtype.columns_cero
    except:
        colums_cero = 0

    return lista_columnas, numeric_columns, colums_cero


def obtener_filtros_navegacion(subtype):
    conf_subtype = list(filter(lambda x: x.subtype == subtype, params.subtypes_conf))[0]
    match subtype:
        case 'sori00v':

            try:
                if conf_subtype.dimensiones:
                    dimensiones = conf_subtype.dimensiones
            except:
                dimensiones = []

            try:
                if conf_subtype.metricas:
                    metricas = conf_subtype.metricas
            except:
                metricas = []

            try:
                if conf_subtype.formatos:
                    formatos = conf_subtype.formatos
            except:
                formatos = []

            return dimensiones, metricas, formatos

        case 'sori00b':

            try:
                if conf_subtype.dimensiones:
                    dimensiones = conf_subtype.dimensiones
            except:
                dimensiones = []

            try:
                if conf_subtype.metricas:
                    metricas = conf_subtype.metricas
            except:
                metricas = []

            try:
                if conf_subtype.formatos:
                    formatos = conf_subtype.formatos
            except:
                formatos = []

            return dimensiones, metricas, formatos

        case 'sori00i':

            try:
                if conf_subtype.dimensiones:
                    dimensiones = conf_subtype.dimensiones
            except:
                dimensiones = []

            try:
                if conf_subtype.metricas:
                    metricas = conf_subtype.metricas
            except:
                metricas = []

            try:
                if conf_subtype.formatos:
                    formatos = conf_subtype.formatos
            except:
                formatos = []

            return dimensiones, metricas, formatos

        case 'sori00q':

            try:
                if conf_subtype.dimensiones:
                    dimensiones = conf_subtype.dimensiones
            except:
                dimensiones = []

            try:
                if conf_subtype.metricas:
                    metricas = conf_subtype.metricas
            except:
                metricas = []

            try:
                if conf_subtype.formatos:
                    formatos = conf_subtype.formatos
            except:
                formatos = []

            return dimensiones, metricas, formatos


def cerrar_aviso_informacion(driver):
    """
    Cierra el anuncio de política o aviso de información si aparece.
    Si no aparece, continúa sin errores.
    """


    trace.write('I', "Validando si existen anuncios.")

    try:
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, '//*[text()="Comunicados"]')))
        bandera = 1
    except TimeoutException:
        trace.write('I', "No se encontraron Anuncios.")
        bandera = 0


    if bandera == 1:
        try:
            trace.write('I', "Cerrando aviso de Comunicados.")
            while True:
                try:
                    xpath_poupop = '//bdi[contains(text(),"Siguiente")] | //bdi[contains(text(),"Le") and contains(text(),"do")]'
                    WebDriverWait(driver, 30).until(EC.visibility_of_element_located((
                        By.XPATH, xpath_poupop)))
                    time.sleep(5)
                    elementos = driver.find_elements(By.XPATH, xpath_poupop)
                    if len(elementos) == 1:
                        WebDriverWait(driver, 30).until(
                            EC.element_to_be_clickable(elementos[0]))
                        elementos[0].click()
                    time.sleep(10)
                except TimeoutException:
                    try:
                        trace.write('I',"Cerrando Ultimo Comunicado.")
                        WebDriverWait(driver, 30).until(EC.visibility_of_element_located((
                            By.XPATH, '//bdi[contains(text(),"Terminar")]'))
                        )
                        WebDriverWait(driver, 30).until(
                            EC.element_to_be_clickable((By.XPATH, '//bdi[contains(text(),"Terminar")]'))).click()
                        time.sleep(5)
                        trace.write('I', "Confirmando cerrar Comunicados.")
                        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                            By.XPATH, '//bdi[contains(text(),"S")]')))
                        WebDriverWait(driver, 30).until(
                            EC.element_to_be_clickable((By.XPATH, '//bdi[contains(text(),"S")]'))).click()
                        time.sleep(2)
                        break
                    except Exception as q:
                        trace.write('E', f"Error al cerrar los Comunicados {q}.")
                        raise Exception(f"Error al cerrar los Comunicados {q}.")

        except Exception as e:
            raise Exception (f'Error al cerrar los comunicados: {e}.')




def download_report_sori00v_and_sori00b(subtype, fact_time, filename, date_from, date_to):
    global robot_result, current_subtask
    trace.write('I', f'Comenzando descarga de {subtype}, fecha: {date_from}.')

    fileHelper.clear_paths([paths.temp, paths.source])
    months = {
        '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
    }

    num_archivos = 1
    step = ""

    try:

        proveedor = obtener_campos_extra_params()

        dimensiones, metricas, formatos = obtener_filtros_navegacion(subtype)

        cerrar_aviso_informacion(driver)

        trace.write('I', f'Iniciando proceso de Navegacion de ventas {subtype}.')

        driver.switch_to.default_content()

        trace.write('I', f'Apartado Comercial.')
        trace.write('I', f'Seleccionamos: Indicadores comerciales.')
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
            By.XPATH, '//span[text()="Indicadores comerciales"]'))).click()
        time.sleep(2)


        if proveedor == '':
            pass
        else:
            trace.write('I', f'Elegimos Proveedor.')
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//input[@placeholder="Proveedor"]'))).click()
            time.sleep(5)

            driver.find_element(By.XPATH, f'//div[text()="{proveedor}"]/parent::div').click()
            time.sleep(7)

            assert proveedor in driver.find_element(By.XPATH, '//input[@placeholder="Proveedor"]').get_attribute(
                'value'), "Error Seleccionando usuario dentro del portal."

            trace.write('I', f'Seleccion de proveedor con exito.')

        iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
            By.XPATH, '//iframe[@id="__xmlview1--map_iframe"]'))
        )
        driver.switch_to.frame(iframe_1)

        trace.write('I', f'Seleccionamos: Generacion de reportes.')

        trace.write('I', f'Seleccionamos: Asistente de generacion  de reportes.')
        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((
            By.XPATH, '//button[contains(text(),"Generaci") and contains(text(),"n de reportes")]'))).click()
        time.sleep(2)
        driver.find_element(By.XPATH, '//*[contains(text(),"Asistente de generaci") and contains(text(),"n  de reportes")]').click()
        driver.switch_to.frame(driver.find_element(By.XPATH, '//div[@class="Embed-container"]/iframe'))
        time.sleep(2)

        if subtype == 'sori00v':
            fecha_inicio = datetime.strptime(date_from, '%Y-%m-%d').strftime('%d/%m/%Y')
        elif subtype == 'sori00b':
            fecha_inicio = (datetime.strptime(date_from, '%Y-%m-%d') - timedelta(days=2)).strftime('%d/%m/%Y')

        mes = months[fecha_inicio.split('/')[1]]
        año = fecha_inicio.split('/')[-1]

        trace.write('I', f'Rango de fechas a buscar: {fecha_inicio} - {fecha_inicio}.')

        trace.write('I', f'Seleccionamos Mes Anio: {mes} {año}.')

        WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
            By.XPATH, '//div[@aria-label="Meses"]')))
        driver.find_element(By.XPATH, '//div[@aria-label="Meses"]').click()
        time.sleep(3)

        try:
            WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, f'//span[text()="{mes} {año}"]')))
            mes_dimension = driver.find_element(By.XPATH, f'//span[text()="{mes} {año}"]/parent::div')
            if mes_dimension.get_attribute('aria-selected') == 'true':
                mes_dimension.click()
            time.sleep(2)

            accion = ActionChains(driver)

            for i in range(14):
                try:
                    mes_dimension.click()
                    break
                except:
                    if i == 13:
                        raise Exception(f'El mes {mes} no se encontro en la lista')
                    accion.key_down(Keys.DOWN)
                    accion.perform()
        except:
            raise Exception(f'El mes {mes} no se encontro en la lista')

        trace.write('I', f"Seleccionamos: Aplicar Filtro")
        try:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Aplicar todas las segmentaciones . Haga clic aquí para seguir"]')
            apply_filter.location_once_scrolled_into_view
        except:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Apply all slicers . Click here to follow"]')
            apply_filter.location_once_scrolled_into_view

        time.sleep(2)
        apply_filter.click()
        time.sleep(2)


        trace.write('I', f"Ingresando fecha: {fecha_inicio}")

        lang = driver.execute_script("return navigator.language || navigator.userLanguage;")
        if lang == 'en-US':
            in_fecha_ini = datetime.strptime(fecha_inicio, '%d/%m/%Y')
            in_fecha_ini = f"{in_fecha_ini.month}/{in_fecha_ini.day}/{in_fecha_ini.year}"
        else:
            in_fecha_ini = datetime.strptime(date_from, '%Y-%m-%d').strftime('%d/%m/%Y')

        try:
            inp_fecha_ini = WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, '//div[@class="date-slicer-range"]/div[1]/div/input'))
            )
        except TimeoutException:
            inp_fecha_ini = WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, '//div[@class="date-slicer-range"]/div[1]/div/input'))
            )

        try:
            inp_fecha_fin = driver.find_element(By.XPATH, '//div[@class="date-slicer-range"]/div[2]/div/input')
        except NoSuchElementException:
            inp_fecha_fin = driver.find_element(By.XPATH, '//div[@class="date-slicer-range"]/div[2]/div/input')

        inp_fecha_ini.clear()
        time.sleep(1)
        inp_fecha_fin.clear()
        time.sleep(1)
        inp_fecha_fin.send_keys(in_fecha_ini)
        time.sleep(1)
        inp_fecha_ini.send_keys(in_fecha_ini)
        time.sleep(1)

        try:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"tart date")]').get_attribute(
                "value")
        try:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"nd date")]').get_attribute(
                "value")

        if fecha_inicio_seleccionada != in_fecha_ini:
            inp_fecha_ini.clear()
            time.sleep(1)
            inp_fecha_ini.send_keys(in_fecha_ini)
            time.sleep(1)
        if fecha_final_seleccionada != in_fecha_ini:
            inp_fecha_fin.clear()
            time.sleep(1)
            inp_fecha_fin.send_keys(in_fecha_ini)
            inp_fecha_fin.clear()

        try:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"tart date")]').get_attribute(
                "value")
        try:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"nd date")]').get_attribute(
                "value")

        trace.write('I', f"Verificando fecha seleccionada.")
        if fecha_inicio_seleccionada == fecha_final_seleccionada:
            trace.write('I', "Fechas seleccionadas correctamente.")

        else:

            trace.write('I',
                        f"Fecha de inicio o final mal seleccionadas, {fecha_inicio_seleccionada}, {fecha_final_seleccionada}")
            trace.write('I', f"Volviendo a ingresar las fechas: {in_fecha_ini}")

            inp_fecha_ini.clear()
            time.sleep(1)
            inp_fecha_fin.clear()
            time.sleep(1)
            inp_fecha_fin.send_keys(in_fecha_ini)
            time.sleep(1)
            inp_fecha_ini.send_keys(in_fecha_ini)
            time.sleep(2)

            try:
                fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                                '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                    "value")
            except NoSuchElementException:
                fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                                '//input[contains(@aria-label,"tart date")]').get_attribute(
                    "value")
            try:
                fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                               '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                    "value")
            except NoSuchElementException:
                fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                               '//input[contains(@aria-label,"nd date")]').get_attribute(
                    "value")

            if fecha_inicio_seleccionada == fecha_final_seleccionada:
                trace.write('I', "Fechas seleccionadas correctamente.")

            else:
                trace.write('E', "Error: Fechas mal seleccionadas.")
                raise Exception("Error: Fechas mal seleccionadas.")

        trace.write('I', "Apartado de Dimensiones a mostrar.")
        trace.write('I', "Limpiando apartado de Dimensiones a mostrar.")
        try:
            dimension_box = driver.find_element(By.XPATH,
                                                '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]')
            ActionChains(driver).move_to_element(dimension_box).perform()
            time.sleep(2)
            driver.find_element(By.XPATH, '//span[@aria-label="Clear selections"]').click()
            time.sleep(2)
            trace.write('I', "Dimensiones a mostrar limpios.")
        except Exception as e:
            trace.write('E', "Error al limpiar los filtros de Dimensiones a mostrar.")
            raise Exception('E', f"Error al limpiar los filtros Dimensiones a mostrar: {e}.")

        trace.write('I', f"Seleccionando dimensiones: {dimensiones}")
        driver.find_element(By.XPATH, '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]/div[text()="All"]').click()
        time.sleep(2)

        try:
            WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                By.XPATH, '//span[text()="Formato"]/parent::div')))
        except TimeoutException:
            raise Exception(
                "Error, tardó demasiado en cargar la sublista de dimensiones a mostrar(cod. barras, cod. articulo)")

        for dimension in dimensiones:
            try:
                trace.write('I', f"Seleccionando: {dimension}.")
                if dimension == 'Código Tienda':
                    dimension = 'digo Tienda'
                elif dimension == 'Código de Barras':
                    dimension = 'digo de Barras'
                elif dimension == 'División':
                    dimension = 'Divisi'
                elif dimension == 'Categoría':
                    dimension = 'Categor'
                elif dimension == 'Artículo':
                    dimension = 'Art'
                elif dimension == 'Fecha':
                    for i in range(5): accion.send_keys(Keys.DOWN).perform();

                if dimension == 'Tienda':
                    dim_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                        By.XPATH, f'//span[text()="{dimension}"]/parent::div')))
                else:
                    dim_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                        By.XPATH, f'//span[contains(text(),"{dimension}")]/parent::div')))

                if dim_select.get_attribute('aria-selected') == 'false':
                    dim_select.click()
                    time.sleep(1)
            except TimeoutException:
                trace.write('E', f"Error: {dimension}, no se encontro en el apartado de Dimensiones a mostrar.")
                raise Exception(f'Error: {dimension}, no se encontro en el apartado de Dimensiones a mostrar')

        trace.write('I', "Seleccion de Metricas Correcto.")
        time.sleep(2)

        trace.write('I', "Apartado de Metricas.")
        trace.write('I', f"Seleccionando metricas: {metricas}")
        for metrica in metricas:
            trace.write('I', f"Seleccionando: {metrica}.")
            try:
                metric_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[text()="{metrica}"]/parent::div')))
                if metric_select.get_attribute('aria-selected') == 'false':
                    metric_select.click()
                    time.sleep(1)
            except TimeoutException:
                trace.write('E', f"Error: {metrica}, no se encontro en el apartado de Metricas.")
                raise Exception(f'Error: {metrica}, no se encontro en el apartado de Metricas')

        trace.write('I', "Seleccion de Metricas Correcto.")
        time.sleep(2)


        trace.write('I', "Apartado de Filtros.")
        trace.write('I', "Limpiando apartado de Formato.")
        try:
            formato_box = driver.find_element(By.XPATH, '//div[@aria-label="Formato" and @role="combobox"]')
            ActionChains(driver).move_to_element(formato_box).perform()
            time.sleep(2)
            driver.find_element(By.XPATH, '//h3[text()="Formato"]/parent::div/span').click()
            time.sleep(2)
            trace.write('I', "Formatos limpios.")
            formato_box.click()
        except Exception as e:
            trace.write('E', "Error al limpiar los filtros de Formato.")
            raise Exception('E', f"Error al limpiar los filtros Formato: {e}.")
        time.sleep(2)

        trace.write('I', f"Seleccionando formatos: {formatos}")
        for formato in formatos:
            trace.write('I', f"Seleccionando: {formato}.")
            try:
                format_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[text()="{formato}"]/parent::div')))

                if format_select.get_attribute('aria-selected') == 'false':
                    format_select.click()
                    time.sleep(1)

            except TimeoutException:
                trace.write('E', f'Error: {formato}, no se encontro en el apartado de Formato.')
                raise Exception(f'Error: {formato}, no se encontro en el apartado de Formato')

        trace.write('I', "Seleccion de Formatos Correcto.")
        time.sleep(2)

        trace.write('I', f"Seleccionamos: Aplicar Filtro")
        try:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Aplicar todas las segmentaciones . Haga clic aquí para seguir"]')
            apply_filter.location_once_scrolled_into_view
        except:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Apply all slicers . Click here to follow"]')
            apply_filter.location_once_scrolled_into_view
        time.sleep(2)
        apply_filter.click()
        time.sleep(2)

        # espera que aparezca el efecto de "cargando.."
        try:
            WebDriverWait(driver, 15).until(
                EC.visibility_of_element_located((By.XPATH, '//div[@class="powerbi-spinner xsmall"]'))
            )
        except TimeoutException:
            pass

        trace.write('I', "Esperando que se genere el informe...")

        # espera que desaparezca el efecto de "cargando.."
        try:
            WebDriverWait(driver, 300).until(
                EC.invisibility_of_element
                ((By.XPATH, '//div[@class="powerbi-spinner xsmall"]'))
            )
            trace.write('I', 'Reporte listo')
        except TimeoutException:
            trace.write('E', 'Error: Reporte tardó mas de 5 minutos en generarse.')
            raise Exception(f'Error: Reporte tardó mas de 5 minutos en generarse.')

        trace.write('I', "Nos movemos al reporte y vamos a los tres puntos.")
        ActionChains(driver).move_to_element(driver.find_element(By.XPATH, '//div[@class="top-viewport"]')).perform()
        time.sleep(2)

        trace.write('I', "Seleccionamos: Más opciones")
        try:
            driver.find_element(By.XPATH, '//*[@aria-label="Más opciones"]').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH, '//*[@aria-label="More options"]').click()
        time.sleep(2)

        trace.write('I', "Seleccionamos: Exportar Datos")
        try:
            driver.find_element(By.XPATH, '//button[@aria-description="Exportar datos"]').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH, '//button[@aria-description="Export data"]').click()
        time.sleep(5)

        trace.write('I',"Seleccionamos: Datos con diseño actual(xlsx)")
        try:
            driver.find_element(By.XPATH,
                                '//*[contains(text(),"Datos con dise")]/parent::span/preceding-sibling::div/span').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH,
                                '//*[text()="Data with current layout"]/parent::span/preceding-sibling::div/span').click()
        time.sleep(5)

        trace.write('I',"Exportando archivo.")
        try:
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Exportar"]')
        except NoSuchElementException:
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Export"]')
        btn_exportar.location_once_scrolled_into_view
        btn_exportar.click()
        time.sleep(10)

        trace.write('I', "Regresando al menu inicial.")
        driver.switch_to.default_content()
        driver.find_element(By.XPATH, '//button[@aria-label = "Inicio"]').click()

        fileHelper.wait_download(num_archivos, 'xlsx')

        fileHelper.rename_files(filename, paths.source, 'xlsx')

        fileHelper.send_to_gs(paths.source, filename, 'xlsx', params.bucket_src, False)

        trace.write('I', 'Proceso de navegacion  Finalizado.')

        pre_transform_sori00v_and_sori00b(filename, date_from, subtype)

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


def download_report_sori00q(subtype, fact_time, filename, date_from, date_to):
    global robot_result, current_subtask
    trace.write('I', f'Comenzando descarga de {subtype}, fecha: {date_from}.')

    fileHelper.clear_paths([paths.temp, paths.source])
    months = {
        '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
    }

    num_archivos = 1
    step = ""

    try:

        proveedor = obtener_campos_extra_params()

        dimensiones, metricas, formatos = obtener_filtros_navegacion(subtype)

        cerrar_aviso_informacion(driver)

        trace.write('I', f'Iniciando proceso de Navegacion de Qual {subtype}.')

        driver.switch_to.default_content()

        accion = ActionChains(driver)

        trace.write('I', f'Apartado Comercial.')
        trace.write('I', f'Seleccionamos: Indicadores comerciales.')
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
            By.XPATH, '//span[text()="Indicadores comerciales"]'))).click()
        time.sleep(2)


        if proveedor == '':
            pass
        else:
            trace.write('I', f'Elegimos Proveedor.')
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//input[@placeholder="Proveedor"]'))).click()
            time.sleep(5)

            driver.find_element(By.XPATH, f'//div[text()="{proveedor}"]/parent::div').click()
            time.sleep(7)

            assert proveedor in driver.find_element(By.XPATH, '//input[@placeholder="Proveedor"]').get_attribute(
                'value'), "Error Seleccionando usuario dentro del portal."

            trace.write('I', f'Seleccion de proveedor con exito.')

        iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
            By.XPATH, '//iframe[@id="__xmlview1--map_iframe"]'))
        )
        driver.switch_to.frame(iframe_1)

        trace.write('I', f'Seleccionamos: Generacion de reportes.')

        trace.write('I', f'Seleccionamos: Asistente de generacion  de reportes.')
        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((
            By.XPATH, '//button[contains(text(),"Generaci") and contains(text(),"n de reportes")]'))).click()
        time.sleep(2)
        driver.find_element(By.XPATH, '//*[contains(text(),"Asistente de generaci") and contains(text(),"n  de reportes")]').click()
        driver.switch_to.frame(driver.find_element(By.XPATH, '//div[@class="Embed-container"]/iframe'))
        time.sleep(2)

        fecha_inicio = (datetime.strptime(date_from, '%Y-%m-%d') - relativedelta(years=0)).strftime('%d/%m/%Y')

        #Fecha fin es la fecha final empezando del año corriente hacia atras ejemplo "sep 2025" fecha final es "sep 2023"
        fecha_fin = (datetime.strptime(date_from, '%Y-%m-%d') - relativedelta(years=2)).strftime('%d/%m/%Y')

        mes = months[fecha_inicio.split('/')[1]]
        año = fecha_inicio.split('/')[-1]

        trace.write('I', f'Rango de fechas a buscar: {fecha_inicio} - {fecha_fin}.')

        trace.write('I', f'Limpiando Filtro de Meses.')

        trace.write('I',f"Seleccionando los ultimos 24 Meses o lo que permita Qual.")

        metric_meses = WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
            By.XPATH, '//div[@aria-label="Meses"]')))
        ActionChains(driver).move_to_element(metric_meses).perform()
        time.sleep(2)
        driver.find_element(By.XPATH, '//*[@aria-label="Meses"]/parent::div/span[@aria-label="Clear selections"]').click()
        time.sleep(2)
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(1)
        driver.find_element(By.XPATH, '//div[@aria-label="Meses"]').click()
        time.sleep(3)

        for i in range(25):
            try:
                mes_search = (datetime.strptime(date_from, '%Y-%m-%d') - relativedelta(months=i)).strftime('%d/%m/%Y')
                mes = months[mes_search.split('/')[1]]
                año = mes_search.split('/')[-1]
                mes_dimension = driver.find_element(By.XPATH, f'//span[text()="{mes} {año}"]/parent::div')
                if mes_dimension.get_attribute('aria-selected') == 'false':
                    trace.write('I', f'Seleccionamos Mes Anio: {mes} {año}.')
                    ActionChains(driver).key_down(Keys.CONTROL).click(mes_dimension).key_up(Keys.CONTROL).perform()
                time.sleep(1)
            except NoSuchElementException:
                trace.write('I', f'No se encontro el mes {mes} {año}')

        trace.write('I', f"Seleccionamos: Aplicar Filtro")
        try:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Aplicar todas las segmentaciones . Haga clic aquí para seguir"]')
            apply_filter.location_once_scrolled_into_view
        except:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Apply all slicers . Click here to follow"]')
            apply_filter.location_once_scrolled_into_view

        time.sleep(2)
        apply_filter.click()
        time.sleep(2)


        trace.write('I', f"Ingresando fechas: {fecha_inicio} - {fecha_fin}")

        lang = driver.execute_script("return navigator.language || navigator.userLanguage;")
        if lang == 'en-US':
            in_fecha_ini = datetime.strptime(fecha_inicio, '%d/%m/%Y')
            in_fecha_ini = f"{in_fecha_ini.month}/{in_fecha_ini.day}/{in_fecha_ini.year}"

            in_fecha_fin = datetime.strptime(fecha_fin, '%d/%m/%Y')
            in_fecha_fin = f"{in_fecha_fin.month}/{in_fecha_fin.day}/{in_fecha_fin.year}"

        else:
            in_fecha_ini = datetime.strptime(date_from, '%Y-%m-%d').strftime('%d/%m/%Y')

        try:
            inp_fecha_ini = WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, '//div[@class="date-slicer-range"]/div[1]/div/input'))
            )
        except TimeoutException:
            inp_fecha_ini = WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, '//div[@class="date-slicer-range"]/div[1]/div/input'))
            )

        try:
            inp_fecha_fin = driver.find_element(By.XPATH, '//div[@class="date-slicer-range"]/div[2]/div/input')
        except NoSuchElementException:
            inp_fecha_fin = driver.find_element(By.XPATH, '//div[@class="date-slicer-range"]/div[2]/div/input')

        inp_fecha_ini.clear()
        time.sleep(1)
        inp_fecha_fin.clear()
        time.sleep(1)
        inp_fecha_fin.send_keys(in_fecha_ini)
        time.sleep(1)
        inp_fecha_ini.send_keys(in_fecha_fin)
        time.sleep(1)

        try:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"tart date")]').get_attribute(
                "value")
        try:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"nd date")]').get_attribute(
                "value")

        if fecha_inicio_seleccionada != in_fecha_fin:
            inp_fecha_ini.clear()
            time.sleep(1)
            inp_fecha_ini.send_keys(in_fecha_fin)
            time.sleep(1)
        if fecha_final_seleccionada != in_fecha_ini:
            inp_fecha_fin.clear()
            time.sleep(1)
            inp_fecha_fin.send_keys(in_fecha_ini)


        try:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"tart date")]').get_attribute(
                "value")
        try:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"nd date")]').get_attribute(
                "value")

        trace.write('I', f"Verificando fecha seleccionada.")
        fecha_fin_str = fecha_final_seleccionada.split('/')
        fecha_fin_str = fecha_fin_str[0] + fecha_fin_str[1]

        fecha_ini_str = fecha_inicio_seleccionada.split('/')
        fecha_ini_str = fecha_ini_str[0] + fecha_ini_str[1]

        if fecha_ini_str == fecha_fin_str:
            trace.write('I', "Fechas seleccionadas correctamente.")

        else:

            trace.write('I',
                        f"Fecha de inicio o final mal seleccionadas, {fecha_inicio_seleccionada}, {fecha_final_seleccionada}")
            trace.write('I', f"Volviendo a ingresar las fechas: {fecha_inicio} - {fecha_fin}")

            inp_fecha_ini.clear()
            time.sleep(1)
            inp_fecha_fin.clear()
            time.sleep(1)
            inp_fecha_fin.send_keys(in_fecha_ini)
            time.sleep(1)
            inp_fecha_ini.send_keys(in_fecha_fin)
            time.sleep(2)

            try:
                fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                                '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                    "value")
            except NoSuchElementException:
                fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                                '//input[contains(@aria-label,"tart date")]').get_attribute(
                    "value")
            try:
                fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                               '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                    "value")
            except NoSuchElementException:
                fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                               '//input[contains(@aria-label,"nd date")]').get_attribute(
                    "value")
            time.sleep(2)

            fecha_fin_str = fecha_final_seleccionada.split('/')
            fecha_fin_str = fecha_fin_str[0] + fecha_fin_str[1]

            fecha_ini_str = fecha_inicio_seleccionada.split('/')
            fecha_ini_str = fecha_ini_str[0] + fecha_ini_str[1]

            if fecha_ini_str == fecha_fin_str:
                trace.write('I', "Fechas seleccionadas correctamente.")

            else:
                trace.write('E', "Error: Fechas mal seleccionadas.")
                raise Exception("Error: Fechas mal seleccionadas.")

        trace.write('I', "Apartado de Dimensiones a mostrar.")
        trace.write('I', "Limpiando apartado de Dimensiones a mostrar.")
        try:
            dimension_box = driver.find_element(By.XPATH,
                                                '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]')
            ActionChains(driver).move_to_element(dimension_box).perform()
            time.sleep(2)
            driver.find_element(By.XPATH, '//span[@aria-label="Clear selections"]').click()
            time.sleep(2)
            trace.write('I', "Dimensiones a mostrar limpios.")
        except Exception as e:
            trace.write('E', "Error al limpiar los filtros de Dimensiones a mostrar.")
            raise Exception('E', f"Error al limpiar los filtros Dimensiones a mostrar: {e}.")

        trace.write('I', f"Seleccionando dimensiones: {dimensiones}")
        driver.find_element(By.XPATH, '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]/div[text()="All"]').click()
        time.sleep(2)

        try:
            WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                By.XPATH, '//span[text()="Formato"]/parent::div')))
        except TimeoutException:
            raise Exception(
                "Error, tardó demasiado en cargar la sublista de dimensiones a mostrar(cod. barras, cod. articulo)")

        for dimension in dimensiones:
            try:
                trace.write('I', f"Seleccionando: {dimension}.")
                if dimension == 'Código Tienda':
                    dimension = 'digo Tienda'
                elif dimension == 'Código de Barras':
                    dimension = 'digo de Barras'
                elif dimension == 'División':
                    dimension = 'Divisi'
                elif dimension == 'Categoría':
                    dimension = 'Categor'
                elif dimension == 'Artículo':
                    dimension = 'Art'
                elif dimension == 'Fecha':
                    for i in range(5): accion.send_keys(Keys.DOWN).perform();

                if dimension == 'Tienda':
                    dim_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                        By.XPATH, f'//span[text()="{dimension}"]/parent::div')))
                else:
                    dim_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                        By.XPATH, f'//span[contains(text(),"{dimension}")]/parent::div')))

                if dim_select.get_attribute('aria-selected') == 'false':
                    dim_select.click()
                    time.sleep(1)
            except TimeoutException:
                trace.write('E', f"Error: {dimension}, no se encontro en el apartado de Dimensiones a mostrar.")
                raise Exception(f'Error: {dimension}, no se encontro en el apartado de Dimensiones a mostrar')

        trace.write('I', "Seleccion de Metricas Correcto.")
        time.sleep(2)

        trace.write('I', "Apartado de Metricas.")
        trace.write('I', f"Seleccionando metricas: {metricas}")
        for metrica in metricas:
            trace.write('I', f"Seleccionando: {metrica}.")
            try:
                metric_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[text()="{metrica}"]/parent::div')))
                if metric_select.get_attribute('aria-selected') == 'false':
                    metric_select.click()
                    time.sleep(1)
            except TimeoutException:
                trace.write('E', f"Error: {metrica}, no se encontro en el apartado de Metricas.")
                raise Exception(f'Error: {metrica}, no se encontro en el apartado de Metricas')

        trace.write('I', "Seleccion de Metricas Correcto.")
        time.sleep(2)


        trace.write('I', "Apartado de Filtros.")
        trace.write('I', "Limpiando apartado de Formato.")
        try:
            formato_box = driver.find_element(By.XPATH, '//div[@aria-label="Formato" and @role="combobox"]')
            ActionChains(driver).move_to_element(formato_box).perform()
            time.sleep(2)
            driver.find_element(By.XPATH, '//h3[text()="Formato"]/parent::div/span').click()
            time.sleep(2)
            trace.write('I', "Formatos limpios.")
            formato_box.click()
        except Exception as e:
            trace.write('E', "Error al limpiar los filtros de Formato.")
            raise Exception('E', f"Error al limpiar los filtros Formato: {e}.")
        time.sleep(2)

        trace.write('I', f"Seleccionando formatos: {formatos}")
        for formato in formatos:
            trace.write('I', f"Seleccionando: {formato}.")
            try:
                format_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[text()="{formato}"]/parent::div')))

                if format_select.get_attribute('aria-selected') == 'false':
                    format_select.click()
                    time.sleep(1)

            except TimeoutException:
                trace.write('E', f'Error: {formato}, no se encontro en el apartado de Formato.')
                raise Exception(f'Error: {formato}, no se encontro en el apartado de Formato')

        trace.write('I', "Seleccion de Formatos Correcto.")
        time.sleep(2)

        trace.write('I', f"Seleccionamos: Aplicar Filtro")
        try:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Aplicar todas las segmentaciones . Haga clic aquí para seguir"]')
            apply_filter.location_once_scrolled_into_view
        except:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Apply all slicers . Click here to follow"]')
            apply_filter.location_once_scrolled_into_view
        time.sleep(2)
        apply_filter.click()
        time.sleep(2)

        # espera que aparezca el efecto de "cargando.."
        try:
            WebDriverWait(driver, 15).until(
                EC.visibility_of_element_located((By.XPATH, '//div[@class="powerbi-spinner xsmall"]'))
            )
        except TimeoutException:
            pass

        trace.write('I', "Esperando que se genere el informe...")

        # espera que desaparezca el efecto de "cargando.."
        try:
            WebDriverWait(driver, 300).until(
                EC.invisibility_of_element
                ((By.XPATH, '//div[@class="powerbi-spinner xsmall"]'))
            )
            trace.write('I', 'Reporte listo')
        except TimeoutException:
            trace.write('E', 'Error: Reporte tardó mas de 5 minutos en generarse.')
            raise Exception(f'Error: Reporte tardó mas de 5 minutos en generarse.')

        trace.write('I', "Nos movemos al reporte y vamos a los tres puntos.")
        ActionChains(driver).move_to_element(driver.find_element(By.XPATH, '//div[@class="top-viewport"]')).perform()
        time.sleep(2)

        trace.write('I', "Seleccionamos: Más opciones")
        try:
            driver.find_element(By.XPATH, '//*[@aria-label="Más opciones"]').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH, '//*[@aria-label="More options"]').click()
        time.sleep(2)

        trace.write('I', "Seleccionamos: Exportar Datos")
        try:
            driver.find_element(By.XPATH, '//button[@aria-description="Exportar datos"]').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH, '//button[@aria-description="Export data"]').click()
        time.sleep(5)

        trace.write('I',"Seleccionamos: Datos con diseño actual(xlsx)")
        try:
            driver.find_element(By.XPATH,
                                '//*[contains(text(),"Datos con dise")]/parent::span/preceding-sibling::div/span').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH,
                                '//*[text()="Data with current layout"]/parent::span/preceding-sibling::div/span').click()
        time.sleep(5)

        trace.write('I',"Exportando archivo.")
        try:
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Exportar"]')
        except NoSuchElementException:
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Export"]')
        btn_exportar.location_once_scrolled_into_view
        btn_exportar.click()
        time.sleep(10)

        trace.write('I', "Regresando al menu inicial.")
        driver.switch_to.default_content()
        driver.find_element(By.XPATH, '//button[@aria-label = "Inicio"]').click()

        fileHelper.wait_download(num_archivos, 'xlsx')

        fileHelper.rename_files(filename, paths.source, 'xlsx')

        fileHelper.send_to_gs(paths.source, filename, 'xlsx', params.bucket_src, False)

        trace.write('I', 'Proceso de navegacion  Finalizado.')

        pre_transform_sori00v_and_sori00b(filename, date_from, subtype)

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




def download_report_sori00i(subtype, fact_time, filename, date_from, date_to):
    global robot_result, current_subtask
    trace.write('I', f'Comenzando descarga de {subtype}, fecha: {date_from}.')

    fileHelper.clear_paths([paths.temp, paths.source])
    months = {
        '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
    }

    num_archivos = 1
    step = ""

    try:

        proveedor = obtener_campos_extra_params()

        dimensiones, metricas, formatos = obtener_filtros_navegacion(subtype)

        cerrar_aviso_informacion(driver)

        trace.write('I', f'Iniciando proceso de Navegacion de Inventario {subtype}.')

        driver.switch_to.default_content()

        trace.write('I', f'Apartado Comercial.')
        trace.write('I', f'Seleccionamos: Indicadores comerciales.')
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
            By.XPATH, '//span[text()="Indicadores comerciales"]'))).click()
        time.sleep(2)


        if proveedor == '':
            pass
        else:
            trace.write('I', f'Elegimos Proveedor.')
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//input[@placeholder="Proveedor"]'))).click()
            time.sleep(5)

            driver.find_element(By.XPATH, f'//div[text()="{proveedor}"]/parent::div').click()
            time.sleep(7)

            assert proveedor in driver.find_element(By.XPATH, '//input[@placeholder="Proveedor"]').get_attribute(
                'value'), "Error Seleccionando usuario dentro del portal."

            trace.write('I', f'Seleccion de proveedor con exito.')

        iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
            By.XPATH, '//iframe[@id="__xmlview1--map_iframe"]'))
        )
        driver.switch_to.frame(iframe_1)

        trace.write('I', f'Seleccionamos: Generacion de reportes.')

        trace.write('I', f'Seleccionamos: Asistente de generacion  de reportes.')
        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((
            By.XPATH, '//button[contains(text(),"Generaci") and contains(text(),"n de reportes")]'))).click()
        time.sleep(2)
        driver.find_element(By.XPATH, '//*[contains(text(),"Asistente de generaci") and contains(text(),"n  de reportes")]').click()
        driver.switch_to.frame(driver.find_element(By.XPATH, '//div[@class="Embed-container"]/iframe'))
        time.sleep(2)


        fecha_inicio = (datetime.strptime(date_from, '%Y-%m-%d') - timedelta(days=0)).strftime('%d/%m/%Y')

        mes = months[fecha_inicio.split('/')[1]]
        año = fecha_inicio.split('/')[-1]

        trace.write('I', f'Rango de fechas a buscar: {fecha_inicio} - {fecha_inicio}.')

        trace.write('I', f'Seleccionamos Mes Anio: {mes} {año}.')

        WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
            By.XPATH, '//div[@aria-label="Meses"]')))
        driver.find_element(By.XPATH, '//div[@aria-label="Meses"]').click()
        time.sleep(3)

        try:
            WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, f'//span[text()="{mes} {año}"]')))
            mes_dimension = driver.find_element(By.XPATH, f'//span[text()="{mes} {año}"]/parent::div')
            if mes_dimension.get_attribute('aria-selected') == 'true':
                mes_dimension.click()
            time.sleep(2)

            accion = ActionChains(driver)

            for i in range(14):
                try:
                    mes_dimension.click()
                    break
                except:
                    if i == 13:
                        raise Exception(f'El mes {mes} no se encontro en la lista')
                    accion.key_down(Keys.DOWN)
                    accion.perform()
        except:
            raise Exception(f'El mes {mes} no se encontro en la lista')

        trace.write('I', f"Seleccionamos: Aplicar Filtro")
        try:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Aplicar todas las segmentaciones . Haga clic aquí para seguir"]')
            apply_filter.location_once_scrolled_into_view
        except:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Apply all slicers . Click here to follow"]')
            apply_filter.location_once_scrolled_into_view

        time.sleep(2)
        apply_filter.click()
        time.sleep(2)


        trace.write('I', f"Ingresando fecha: {fecha_inicio}")

        lang = driver.execute_script("return navigator.language || navigator.userLanguage;")
        if lang == 'en-US':
            in_fecha_ini = datetime.strptime(fecha_inicio, '%d/%m/%Y')
            in_fecha_ini = f"{in_fecha_ini.month}/1/{in_fecha_ini.year}"
        else:
            in_fecha_ini = datetime.strptime(date_from, '%Y-%m-%d').strftime('01/%m/%Y')

        try:
            inp_fecha_ini = WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, '//div[@class="date-slicer-range"]/div[1]/div/input'))
            )
        except TimeoutException:
            inp_fecha_ini = WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, '//div[@class="date-slicer-range"]/div[1]/div/input'))
            )

        try:
            inp_fecha_fin = driver.find_element(By.XPATH, '//div[@class="date-slicer-range"]/div[2]/div/input')
        except NoSuchElementException:
            inp_fecha_fin = driver.find_element(By.XPATH, '//div[@class="date-slicer-range"]/div[2]/div/input')

        inp_fecha_ini.clear()
        time.sleep(1)
        inp_fecha_fin.clear()
        time.sleep(1)
        inp_fecha_fin.send_keys(in_fecha_ini)
        time.sleep(1)
        inp_fecha_ini.send_keys(in_fecha_ini)
        time.sleep(1)

        try:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"tart date")]').get_attribute(
                "value")
        try:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"nd date")]').get_attribute(
                "value")

        if fecha_inicio_seleccionada != in_fecha_ini:
            inp_fecha_ini.clear()
            time.sleep(1)
            inp_fecha_ini.send_keys(in_fecha_ini)
            time.sleep(1)

        if fecha_final_seleccionada != in_fecha_ini:
            inp_fecha_fin.clear()
            time.sleep(1)
            inp_fecha_fin.send_keys(in_fecha_ini)
            inp_fecha_fin.clear()

        try:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                            '//input[contains(@aria-label,"tart date")]').get_attribute(
                "value")
        try:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                "value")
        except NoSuchElementException:
            fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                           '//input[contains(@aria-label,"nd date")]').get_attribute(
                "value")

        trace.write('I', f"Verificando fecha seleccionada.")
        if fecha_inicio_seleccionada == fecha_final_seleccionada:
            trace.write('I', "Fechas seleccionadas correctamente.")

        else:

            trace.write('I', f"Fecha de inicio o final mal seleccionadas, {fecha_inicio_seleccionada}, {fecha_final_seleccionada}")
            trace.write('I', f"Volviendo a ingresar las fechas: {in_fecha_ini}")

            inp_fecha_ini.clear()
            time.sleep(1)
            inp_fecha_fin.clear()
            time.sleep(1)
            inp_fecha_fin.send_keys(in_fecha_ini)
            time.sleep(1)
            inp_fecha_ini.send_keys(in_fecha_ini)
            time.sleep(2)

            try:
                fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                                '//input[contains(@aria-label,"echa de inicio")]').get_attribute(
                    "value")
            except NoSuchElementException:
                fecha_inicio_seleccionada = driver.find_element(By.XPATH,
                                                                '//input[contains(@aria-label,"tart date")]').get_attribute(
                    "value")
            try:
                fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                               '//input[contains(@aria-label,"echa de finalizaci")]').get_attribute(
                    "value")
            except NoSuchElementException:
                fecha_final_seleccionada = driver.find_element(By.XPATH,
                                                               '//input[contains(@aria-label,"nd date")]').get_attribute(
                    "value")

            if fecha_inicio_seleccionada == fecha_final_seleccionada:
                trace.write('I', "Fechas seleccionadas correctamente.")

            else:
                trace.write('E', "Error: Fechas mal seleccionadas.")
                raise Exception ("Error: Fechas mal seleccionadas.")


        trace.write('I', "Apartado de Dimensiones a mostrar.")
        trace.write('I', "Limpiando apartado de Dimensiones a mostrar.")
        try:
            dimension_box = driver.find_element(By.XPATH,
                                                '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]')
            ActionChains(driver).move_to_element(dimension_box).perform()
            time.sleep(2)
            driver.find_element(By.XPATH, '//span[@aria-label="Clear selections"]').click()
            time.sleep(2)
            trace.write('I', "Dimensiones a mostrar limpios.")
        except Exception as e:
            trace.write('E', "Error al limpiar los filtros de Dimensiones a mostrar.")
            raise Exception('E', f"Error al limpiar los filtros Dimensiones a mostrar: {e}.")

        trace.write('I', f"Seleccionando dimensiones: {dimensiones}")
        driver.find_element(By.XPATH, '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]/div[text()="All"]').click()
        time.sleep(2)

        try:
            WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                By.XPATH, '//span[text()="Formato"]/parent::div')))
        except TimeoutException:
            raise Exception(
                "Error, tardó demasiado en cargar la sublista de dimensiones a mostrar(cod. barras, cod. articulo)")

        for dimension in dimensiones:
            try:
                trace.write('I', f"Seleccionando: {dimension}.")
                if dimension == 'Código Tienda':
                    dimension = 'digo Tienda'
                elif dimension == 'Código de Barras':
                    dimension = 'digo de Barras'
                elif dimension == 'División':
                    dimension = 'Divisi'
                elif dimension == 'Categoría':
                    dimension = 'Categor'
                elif dimension == 'Artículo':
                    dimension = 'Art'
                elif dimension == 'Fecha':
                    for i in range(5): accion.send_keys(Keys.DOWN).perform();

                dim_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[contains(text(),"{dimension}")]/parent::div')))
                if dim_select.get_attribute('aria-selected') == 'false':
                    dim_select.click()
                    time.sleep(1)

            except TimeoutException:
                trace.write('E', f"Error: {dimension}, no se encontro en el apartado de Dimensiones a mostrar.")
                raise Exception(f'Error: {dimension}, no se encontro en el apartado de Dimensiones a mostrar')

        trace.write('I', "Seleccion de Dimensiones Correcto.")
        time.sleep(2)


        trace.write('I', f"Seleccionamos: Aplicar Filtro")
        try:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Aplicar todas las segmentaciones . Haga clic aquí para seguir"]')
            apply_filter.location_once_scrolled_into_view
        except:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Apply all slicers . Click here to follow"]')
            apply_filter.location_once_scrolled_into_view
        time.sleep(2)
        apply_filter.click()
        time.sleep(2)

        trace.write('I', "Apartado de Metricas.")
        trace.write('I', "Limpiando apartado de Formato.")
        list_metricas_xpath = driver.find_elements(By.XPATH,
                                                   '//div[@role="listbox" and @aria-label="Metricas"]//div[@class="slicerItemContainer" and @role="option"]')
        for metrica_path in list_metricas_xpath:
            if metrica_path.get_attribute('aria-selected') == 'true':
                metrica_path.click()
            time.sleep(1)

        trace.write('I', f"Seleccionando metricas: {metricas}")
        for metrica in metricas:
            trace.write('I', f"Seleccionando: {metrica}.")
            try:
                metric_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[text()="{metrica}"]/parent::div')))
                if metric_select.get_attribute('aria-selected') == 'false':
                    metric_select.click()
                    time.sleep(1)
            except TimeoutException:
                trace.write('E', f"Error: {metrica}, no se encontro en el apartado de Metricas.")
                raise Exception(f'Error: {metrica}, no se encontro en el apartado de Metricas')

        trace.write('I', "Seleccion de Metricas Correcto.")
        time.sleep(2)


        trace.write('I', "Apartado de Filtros.")
        trace.write('I', "Limpiando apartado de Formato.")
        try:
            formato_box = driver.find_element(By.XPATH, '//div[@aria-label="Formato" and @role="combobox"]')
            ActionChains(driver).move_to_element(formato_box).perform()
            time.sleep(2)
            driver.find_element(By.XPATH, '//h3[text()="Formato"]/parent::div/span').click()
            time.sleep(2)
            trace.write('I', "Formatos limpios.")
            formato_box.click()
        except Exception as e:
            trace.write('E', "Error al limpiar los filtros de Formato.")
            raise Exception('E', f"Error al limpiar los filtros Formato: {e}.")
        time.sleep(2)

        for formato in formatos:
            trace.write('I', f"Seleccionando: {formato}.")
            try:
                format_select = WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[text()="{formato}"]/parent::div')))

                if format_select.get_attribute('aria-selected') == 'false':
                    format_select.click()
                    time.sleep(1)

            except TimeoutException:
                trace.write('E', f'Error: {formato}, no se encontro en el apartado de Formato.')
                raise Exception(f'Error: {formato}, no se encontro en el apartado de Formato')

        trace.write('I', "Seleccion de Formatos Correcto.")
        time.sleep(2)

        trace.write('I', f"Seleccionamos: Aplicar Filtro")
        try:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Aplicar todas las segmentaciones . Haga clic aquí para seguir"]')
            apply_filter.location_once_scrolled_into_view
        except:
            apply_filter = driver.find_element(By.XPATH,
                                               '//div[@aria-label="Apply all slicers . Click here to follow"]')
            apply_filter.location_once_scrolled_into_view
        time.sleep(2)
        apply_filter.click()
        time.sleep(2)

        # espera que aparezca el efecto de "cargando.."
        try:
            WebDriverWait(driver, 15).until(
                EC.visibility_of_element_located((By.XPATH, '//div[@class="powerbi-spinner xsmall"]'))
            )
        except TimeoutException:
            pass

        trace.write('I', "Esperando que se genere el informe...")

        # espera que desaparezca el efecto de "cargando.."
        try:
            WebDriverWait(driver, 300).until(
                EC.invisibility_of_element
                ((By.XPATH, '//div[@class="powerbi-spinner xsmall"]'))
            )
            trace.write('I', 'Reporte listo')
        except TimeoutException:
            trace.write('E', 'Error: Reporte tardó mas de 5 minutos en generarse.')
            raise Exception(f'Error: Reporte tardó mas de 5 minutos en generarse.')

        trace.write('I', "Nos movemos al reporte y vamos a los tres puntos.")
        ActionChains(driver).move_to_element(driver.find_element(By.XPATH, '//div[@class="top-viewport"]')).perform()
        time.sleep(2)

        trace.write('I', "Seleccionamos: Más opciones")
        try:
            driver.find_element(By.XPATH, '//*[@aria-label="Más opciones"]').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH, '//*[@aria-label="More options"]').click()
        time.sleep(2)

        trace.write('I', "Seleccionamos: Exportar Datos")
        try:
            driver.find_element(By.XPATH, '//button[@aria-description="Exportar datos"]').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH, '//button[@aria-description="Export data"]').click()
        time.sleep(5)

        trace.write('I',"Seleccionamos: Datos con diseño actual(xlsx)")
        try:
            driver.find_element(By.XPATH,
                                '//*[contains(text(),"Datos con dise")]/parent::span/preceding-sibling::div/span').click()
        except NoSuchElementException:
            driver.find_element(By.XPATH,
                                '//*[text()="Data with current layout"]/parent::span/preceding-sibling::div/span').click()
        time.sleep(5)

        trace.write('I',"Exportando archivo.")
        try:
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Exportar"]')
        except NoSuchElementException:
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Export"]')
        btn_exportar.location_once_scrolled_into_view
        btn_exportar.click()
        time.sleep(10)

        trace.write('I', "Regresando al menu inicial.")
        driver.switch_to.default_content()
        driver.find_element(By.XPATH, '//button[@aria-label = "Inicio"]').click()

        fileHelper.wait_download(num_archivos, 'xlsx')

        fileHelper.rename_files(filename, paths.source, 'xlsx')

        fileHelper.send_to_gs(paths.source, filename, 'xlsx', params.bucket_src, False)

        trace.write('I', 'Proceso de navegacion  Finalizado.')

        pre_transform_sori00i(filename, date_from, subtype)

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

def pre_transform_sori00v_and_sori00b(filename, date_from, subtype):

    try:
        file_name = f'{filename}.csv'
        if subtype == 'sori00v':
            date_id = datetime.strptime(date_from, '%Y-%m-%d').strftime('%Y-%m-%d')
        else:
            date_id = (datetime.strptime(date_from, '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')

        name_columns, numeric_colums, columns_cero = obtener_campos(subtype)

        files_xlsx = [f for f in os.listdir(paths.source) if f.endswith('.xlsx')]
        for file in files_xlsx:
            df = pd.read_excel(paths.source + file)

            # Crea a nuevos encabezados de los originales sin acentos, sin caracters extraños
            columnas_sin_acentos = []
            for columna_df in df.columns:
                texto_normalizado = unicodedata.normalize('NFD', columna_df)
                texto_sin_acentos = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')

                columnas_sin_acentos.append(texto_sin_acentos)
            df.columns = columnas_sin_acentos

            df = df.fillna("0").astype(str)
            df = df[(df['Fecha'] != '0') & (df['Codigo de Barras'] != '0')]

            for columna in df.columns:
                # le da formato a la fecha, y si hay alguna que sea distinta a fechaini la setea a False
                if columna == 'Fecha':
                    df[columna] = df[columna].apply(lambda x: date_id if str(x).split(' ')[0] == date_id else False)

                    # si alguna fecha fué seteada a False quiere decir que es diferente a fechaini, levanta una esepción
                    assert df[columna].eq(
                        date_id).all(), f"Error, no todas las fechas contenidas en el archivo son iguales a {date_id}"

                df[columna] = df[columna].fillna('').apply(
                    lambda x: str(x).replace(',', '').replace('"', '').replace('=', '').replace('nan', '').strip())


            if sorted(name_columns) == sorted(df.columns):

                df.to_csv(paths.source + file_name, encoding='utf-8', index=False, float_format='%.5f')

            else:
                trace.write('E', 'Las columnas no coinciden con las columnas del archivo')

                trace.write('E', f'Las columnas del archivo son: {df.columns}')
                trace.write('E', f'Las columnas del json son: {name_columns}')
                raise Exception('Error en la pre-transformacion')

    except Exception as e:
            trace.write("I", f"Error en la pre-tranformacion {e} de: {subtype}")

def pre_transform_sori00i(filename, date_from, subtype):

    try:
        file_name = f'{filename}.csv'

        date_id = datetime.strptime(date_from, '%Y-%m-%d').strftime('%Y-%m-%d')

        name_columns, numeric_colums, columns_cero = obtener_campos(subtype)

        files_xlsx = [f for f in os.listdir(paths.source) if f.endswith('.xlsx')]
        for file in files_xlsx:
            df = pd.read_excel(paths.source + file)

            # Crea a nuevos encabezados de los originales sin acentos, sin caracters extraños
            columnas_sin_acentos = []
            for columna_df in df.columns:
                texto_normalizado = unicodedata.normalize('NFD', columna_df)
                texto_sin_acentos = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')

                columnas_sin_acentos.append(texto_sin_acentos)
            df.columns = columnas_sin_acentos

            df = df.fillna("0").astype(str)
            df = df[(df['Fecha'] != '0') & (df['Codigo de Barras'] != '0')]

            for columna in df.columns:

                if columna == 'Fecha':
                    df[columna] = df[columna].apply(lambda x: str(x).split(' ')[0])

                df[columna] = df[columna].fillna('').apply(
                    lambda x: str(x).replace(',', '').replace('"', '').replace('=', '').replace('nan', '').strip())


            df['rm_time_id'] = date_from

            if sorted(name_columns) == sorted(df.columns):

                df.to_csv(paths.source + file_name, encoding='utf-8', index=False, float_format='%.5f')

            else:
                trace.write('E', 'Las columnas no coinciden con las columnas del archivo')

                trace.write('E', f'Las columnas del archivo son: {df.columns}')
                trace.write('E', f'Las columnas del json son: {name_columns}')
                raise Exception('Error en la pre-transformacion')

    except Exception as e:
            trace.write("I", f"Error en la pre-tranformacion: {e} de: {subtype}")

def pre_transform_sori00q(filename, date_from, subtype):

    try:
        file_name = f'{filename}.csv'

        date_id = datetime.strptime(date_from, '%Y-%m-%d').strftime('%Y-%m-%d')

        name_columns, numeric_colums, columns_cero = obtener_campos(subtype)

        files_xlsx = [f for f in os.listdir(paths.source) if f.endswith('.xlsx')]
        for file in files_xlsx:
            df = pd.read_excel(paths.source + file)

            # Crea a nuevos encabezados de los originales sin acentos, sin caracters extraños
            columnas_sin_acentos = []
            for columna_df in df.columns:
                texto_normalizado = unicodedata.normalize('NFD', columna_df)
                texto_sin_acentos = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')

                columnas_sin_acentos.append(texto_sin_acentos)
            df.columns = columnas_sin_acentos

            df = df.fillna("0").astype(str)
            df = df[(df['Fecha'] != '0')]

            for columna in df.columns:

                if columna == 'Fecha':
                    df[columna] = df[columna].apply(lambda x: str(x).split(' ')[0])

                df[columna] = df[columna].fillna('').apply(
                    lambda x: str(x).replace(',', '').replace('"', '').replace('=', '').replace('nan', '').strip())


            df['rm_time_id'] = date_from

            if sorted(name_columns) == sorted(df.columns):

                df.to_csv(paths.source + file_name, encoding='utf-8', index=False, float_format='%.5f')

            else:
                trace.write('E', 'Las columnas no coinciden con las columnas del archivo')

                trace.write('E', f'Las columnas del archivo son: {df.columns}')
                trace.write('E', f'Las columnas del json son: {name_columns}')
                raise Exception('Error en la pre-transformacion')

    except Exception as e:
            trace.write("I", f"Error en la pre-tranformacion: {e} de: {subtype}")

def set_driver(params, trace, paths):
    trace.write('I', 'configurando driver.')
    chrome_options = Options()
    #if os.name != "nt":
    #    chrome_options.add_argument("--headless")
    #chrome_options.add_argument("--incognito")
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
    #chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36")
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
        #end_program(common.ResultType.EXCEPTION, 'Finalizado con Error Desconocido.', f'Exception: {e}')
    return driver


def main():
    global trace, params, paths, fileHelper, driver, robot_result

    params = common.load_json_params()
    trace = common.Trace(params)
    paths = common.Paths()
    robot_result = common.Result(params)
    fileHelper = common.FileHelper(trace, paths, params)

    #Siempre se ven asi
    error_code = 0
    robot_result = common.Result(params)

    """file_name = 'walmart_salida'
    #files_after = [f for f in os.listdir(paths.source)]
    # pre_transformar_archivo_sams00v_excel(files_after, filename, date_from)
    pre_transform_sori00q(file_name, '2025-09-09', 'sori00q')
    curr = common.Subtask('sori00q', '2025-09-09')
    curr.set_src([file_name + '.csv'], 'csv')
    robot_ft.transform_report(trace, params, paths, fileHelper, curr, common.ZipType.NONE)"""


    driver = set_driver(params, trace, paths)
    login()
    time.sleep(3)
    close_note()
    time.sleep(1)


    subtypes = get_subtypes_to_process()
    trace.write("I", f"subtypes a procesar: {subtypes}")


    last_date_flag = ''


    for subtype in subtypes:
        match subtype:
            case 'sori00v':
                process_subtype(download_report_sori00v_and_sori00b, subtype, last_date_flag, common.AllowDateRange.No)

            case 'sori00b':
                process_subtype(download_report_sori00v_and_sori00b, subtype, last_date_flag, common.AllowDateRange.No)

            case 'sori00i':
                process_subtype(download_report_sori00i, subtype, last_date_flag, common.AllowDateRange.No)

            case 'sori00q':
                process_subtype(download_report_sori00q, subtype, last_date_flag, common.AllowDateRange.No)


            case _:
                result_status = {"status": "E", "status_code": 910,
                                 "status_message": f"No esta definido el subtype {subtype} en el codigo del robot!"}
                pass

    end_program(error_code, 'Tarea Finalizada.')


if __name__ == "__main__":
    main()