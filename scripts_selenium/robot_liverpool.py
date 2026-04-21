

'''
--------------------------------------------------------------------------

ENVIRONMENT: 2031
PORTAL: LIVERPOOL
VERSION: V5
CREATION DATE: 2025/06/25
CHAIN: 10002
DEVELOPER: AXEL TREJO AMADOR
REVIEWED BY: AXEL TREJO AMADOR
CREATION DATE: 2025/06/22 00:00 pm
LAST UPDATE: 2025/07/16 09:00 am
UPDATE: 'LUGAR O APARTADO DONDE SE REALIZO EL AJUSTE'
--------------------------------------------------------------------------

OBSERVACIONES.
    - La maquina que se usa es la g100 unicamente para liverpol (de momento)
    - El portal esta bajando varios usuarios y son los siguientes:
        0000	Usuario P00000728 (Seccion 704)
        0001	Usuario P00115847 (Seccion 701)
        0002	Usuario P00000728 (Seccion 701)
        0003	Usuario P00000728 (Seccion 702)
        0004	Usuario P00000728 (Seccion 706)

    Todos los user bajan la misma informacion tanto de ventas como inventarios.

--------------------------------------------------------------------------
'''
import calendar
from bs4 import BeautifulSoup
import robot_ft
import common
import json
import os
import time
import requests
import base64
import codecs
import sys
import zipfile
import shutil
from datetime import date, timedelta, datetime
import csv
import unicodedata
import pandas as pd

# from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException, NoAlertPresentException
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
    driver.quit()
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
    sections = list(filter(lambda x: (x.subtype == subtype), params.subtypes_conf))[0].sections
    select_metric = list(filter(lambda x: (x.subtype == subtype), params.subtypes_conf))[0].select_metric

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
                    filename = f'da_{params.chain_id}_{params.supplier_id}_{params.drt_id}_{subtype}_{fact_time}_{date}_{date}'
                    f_download(subtype, fact_time, filename, date, date, select_metric, sections)
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
            trace.write('I',f"Procesando carpeta: {carpeta}")
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
            trace.write('I',f"La carpeta {carpeta} no existe.")


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

        try:
            trace.write('I','Ingresando claves de acceso.')
            driver.find_element(By.CSS_SELECTOR, 'input[id="logonuidfield"][name="j_username"]').send_keys(params.username)
            time.sleep(3)
            driver.find_element(By.CSS_SELECTOR, 'input[id="logonpassfield"][name="j_password"]').send_keys(codecs.decode(base64.b64decode(params.password)))
            time.sleep(3)
            driver.find_element(By.CSS_SELECTOR, 'input[class="urBtnStdNew"][name="uidPasswordLogon"]').click()
            time.sleep(3)

        except TimeoutException:
            driver.execute_script('window.stop()')

        except Exception as e:
            fileHelper.save_screen(driver)
            trace.write('E', 'Finalizado inicio de sesion con error!')
            end_program(common.ResultType.EXCEPTION, 'Finalizado con Error Desconocido.', f'Exception: {repr(e)}')

        # Validando ingreso correcto
        try:
            driver.find_element(By.CSS_SELECTOR, 'span[class="urTxtMsg"]')
            trace.write('I','Usuario y/o contrasenia no validos.')
            end_program(common.ResultType.USER_ERROR, 'Error en login!')
            driver.close()
            sys.exit(0)

        except NoSuchElementException:
            trace.write('I','Inicio de sesion exitoso.')

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

# region download

def waiting_selection(driver, name_icon, reference_number,subtype):
    flag = True
    WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'table[name="bloque"][class="SAPBEXCellspacing"]')))
    if (name_icon == 'Art') and ('00v' in subtype):
        element = driver.find_elements(By.XPATH,f'//a[contains(text(),"{name_icon}")]/parent::td/parent::tr/td[2]')
        element[1].click()
    elif (name_icon == 'Art') and ('00i' in subtype):
        trace.write('E', 'Seleccionando: Articulo.')
        element = driver.find_elements(By.XPATH,f'//a[contains(text(),"{name_icon}")]/parent::td/parent::tr/td[2]')
        element[2].click()
    else:

        driver.find_element(By.XPATH,f'//a[contains(text(),"{name_icon}")]/parent::td/parent::tr/td[2]').click()
    s_time = datetime.now()

    try:
        driver.find_element(By.XPATH,'//div[@id="sub-frame-error"]')
        #send_email(driver, 'Monitoreo Liverpool 2031', 'Error durante la seleccion en el bloque de navegacion.')
        raise Exception('Error durante la seleccion en el bloque de navegacion.')

    except NoSuchElementException:

        while True:

            if (datetime.now() - s_time).seconds < 300:
                elements_selected = driver.find_elements(By.CSS_SELECTOR,'a[title="Eliminar desglose"]')

                if len(elements_selected) == reference_number:
                    trace.write('I',name_icon + ' seleccionado.')
                    flag = False
                    break

                else:
                    time.sleep(2)

            else:
                raise Exception('El portal tardo demasiado en cargar el contenido')

        if flag:
            raise Exception('No se pudo seleccionar {}.'.format(name_icon))

def exporting_file(driver, path_sorce, subtype, sections):
    wait_element = WebDriverWait(driver, 10)
    actions = ActionChains(driver)

    wait_element.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'table[name="bloque"][class="SAPBEXCellspacing"]')))

    article = driver.find_element(By.XPATH,'//a[contains(text(),"Art")]')
    actions.move_to_element(article)
    actions.context_click(article)
    actions.perform()
    time.sleep(3)
    trace.write('I','Desplegando informacion en formato csv.')
    driver.switch_to.default_content()
    driver.switch_to.frame(driver.find_element(By.XPATH,'//iframe[@id="ivuFrm_page0ivu2"]'))

    if ('live00v' in subtype) or ('live00b' in subtype):
        driver.switch_to.frame(driver.find_element(By.XPATH,'//iframe[@title="Ventas Diarias"]'))
    elif ('live00i' in subtype) or ('live00p' in subtype):
        driver.switch_to.frame(driver.find_element(By.XPATH,'//iframe[@title="Ventas Mensuales U"]'))

    csv_export = driver.find_element(By.XPATH,'//nobr[contains(text(), "Exportar archivo como *.csv")]')
    actions = ActionChains(driver)
    actions.move_to_element(csv_export).perform()
    actions.click(driver.find_element(By.XPATH,'//nobr[contains(text(), "Exportar archivo como *.csv")]')).perform()
    trace.write('I','Extrayendo informacion')
    wait_element.until(EC.presence_of_element_located((By.XPATH, '//pre')))
    csv_source = BeautifulSoup(driver.page_source, 'html.parser').find('pre').text.encode('cp1252', 'ignore').decode('utf-8', 'ignore')

    if csv_source:
        rows = csv_source.split(chr(10))
        with open(path_sorce + f'{sections}_liverpool_data.csv', 'w', encoding='UTF-16') as f_input:
            csv_input = csv.writer(f_input, lineterminator=chr(10), quotechar='"', delimiter=',')
            for row in rows:
                items = row.split('","')
                csv_input.writerow(items)
            trace.write('I','Archivo csv preparado.')
    else:
        raise Exception('Error no se extrajo el texto del csv que se muestra en la pagina.')


def download_report_live00v(subtype, fact_time, filename, date_from, date_to,select_metric,sections):
    global robot_result, current_subtask
    trace.write('I', f'Comenzando descarga de {subtype}, fecha: {date_from}.')

    fileHelper.clear_paths([paths.temp, paths.source])
    # clearn_paths([paths.temp, paths.source])

    fecha = datetime.strptime(date_from, '%Y-%m-%d').strftime('%d/%m/%Y')
    driver.implicitly_wait(20)
    wait_element = WebDriverWait(driver, 30)
    actions = ActionChains(driver)
    num_archivos = 1
    step = ""

    try:
        trace.write('I','Iniciando navegacion en portal.')

        try:
            trace.write('I', 'Regresando al Inicio del Portal.')
            driver.switch_to.default_content()
            inicio = driver.find_element(By.XPATH, '//*[@id="navNode_1_0" and @class="prtlTopNav1stLvl-1"]')
            driver.execute_script('arguments[0].click()', inicio)
        except Exception as e:
            pass

        for seccion in sections:
            trace.write('I','Reportes.')
            reports = driver.find_element(By.XPATH,'//*[@id="navNodeAnchor_1_1" and @class="prtlTopNavLnk"]')
            driver.execute_script('arguments[0].click()', reports)
            time.sleep(5)
            driver.set_page_load_timeout(450)
            trace.write('I','Cambiando de Frame.')
            driver.switch_to.frame(driver.find_element(By.CSS_SELECTOR,'iframe[id="ivuFrm_page0ivu2"]'))

            start_date = (datetime.strptime(date_from, '%Y-%m-%d') - timedelta(days=6)).strftime('%d/%m/%Y')
            end_date = (datetime.strptime(date_from, '%Y-%m-%d') - timedelta(days=0)).strftime('%d/%m/%Y')

            trace.write('I','Ventas Diarias.')
            sale_day = driver.find_element(By.XPATH,'//*[contains(text(), "Ventas Diarias")]')
            driver.execute_script('arguments[0].click()', sale_day)
            time.sleep(5)
            trace.write('I','Cambiando de Frame.')
            new_frame = driver.find_element(By.CSS_SELECTOR,'iframe[id="isolatedWorkArea"][name="isolatedWorkArea"]')
            driver.switch_to.frame(new_frame)

            # Ingresando seccion
            trace.write('I',f'Seccion: ' + seccion)
            section = wait_element.until(EC.element_to_be_clickable((
                By.CSS_SELECTOR, 'input[name="VAR_VALUE_EXT_2"][id="VAR_VALUE_EXT_2"]')))
            section.send_keys(seccion)
            time.sleep(5)

            # Ingresando periodo inicial
            trace.write('I','Inicio de periodo: {}.'.format(start_date))
            try:
                driver.execute_script('arguments[0].setAttribute("value", ' + '"' + start_date + '")',
                                      driver.find_element(By.XPATH, '//input[@id="VAR_VALUE_LOW_EXT_14"]'))
            except:
                driver.execute_script('arguments[0].setAttribute("value", ' + '"' + start_date + '")',
                                      driver.find_element(By.XPATH, '//input[@id="VAR_VALUE_LOW_EXT_12"]'))
            time.sleep(5)

            # Ingresando periodo final
            trace.write('I','Fin de periodo: {}.'.format(end_date))
            try:
                driver.execute_script('arguments[0].setAttribute("value", ' + '"' + end_date + '")',
                                      driver.find_element(By.XPATH, '//input[@id="VAR_VALUE_HIGH_EXT_14"]'))
            except:
                driver.execute_script('arguments[0].setAttribute("value", ' + '"' + end_date + '")',
                                      driver.find_element(By.XPATH, '//input[@id="VAR_VALUE_HIGH_EXT_12"]'))
            time.sleep(5)

            # Oprimiendo boton ejecutar
            trace.write('I','Datos enviados, cargando formulario...')
            driver.execute_script('arguments[0].click()',
                                  driver.find_element(By.XPATH, '//nobr[contains(text(), "Ejecutar")]'))

            trace.write('I','Realizando proceso de ventas.')
            driver.set_page_load_timeout(250)

            # Quita el filtro de cuidado con la piel en el usuario 2
            try:
                driver.find_element(By.XPATH, '//a[@title="Eliminar filtro"]')[2].click()
            except:
                pass

            for numeric, metric in enumerate(select_metric):
                trace.write('I',metric)
                #trace.write('I',numeric+2)
                waiting_selection(driver, metric, numeric + 2,subtype)
            # Seleccionando Dia/Periodo


            # Descargando archivo
            trace.write('I','Preparando reporte para descargar.')
            exporting_file(driver, paths.source, subtype, seccion)
            time.sleep(5)

            trace.write('I', 'Regresando al inicio del Portal')
            try:
                driver.switch_to.default_content()
                driver.find_element(By.XPATH, '//a[contains(text(),"Inicio")]').click()
            except Exception as e:
                raise Exception ('Error, no se pudo regresar al inicio de Portal')

            time.sleep(10)

            fileHelper.wait_download(num_archivos, 'csv')

            num_archivos += 1

        trace.write('I', 'Proceso de navegacion  Finalizado.')

        pre_transform_live00v(filename, date_from, subtype)

        fileHelper.send_to_gs(paths.source, filename, 'csv', params.bucket_src, False)

        # produccion
        current_subtask.set_src([filename + '.csv'], 'csv')

        # local
        # current_subtask.set_src(['da_30007_00_1_tott00i_day_2025-03-26_2025-03-26.zip'], 'csv')

        # Si no es .zip es .NONE
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

def download_report_live00i(subtype, fact_time, filename, date_from, date_to,select_metric,sections):
    global robot_result, current_subtask
    trace.write('I', f'Comenzando descarga de {subtype}, fecha: {date_from}.')

    fileHelper.clear_paths([paths.temp, paths.source])
    # clearn_paths([paths.temp, paths.source])

    fecha = datetime.strptime(date_from, '%Y-%m-%d').strftime('%d/%m/%Y')
    driver.implicitly_wait(20)
    wait_element = WebDriverWait(driver, 30)
    actions = ActionChains(driver)
    num_archivos = 1
    step = ""

    try:

        trace.write('I','Iniciando navegacion en portal para Inventarios.')

        try:
            trace.write('I', 'Regresando al Inicio del Portal.')
            driver.switch_to.default_content()
            inicio = driver.find_element(By.XPATH, '//*[@id="navNode_1_0" and @class="prtlTopNav1stLvl-1"]')
            driver.execute_script('arguments[0].click()', inicio)
        except Exception as e:
            pass

        for seccion in sections:
            trace.write('I','Seleccioando: Reportes.')
            reports = driver.find_element(By.XPATH,'//*[@id="navNodeAnchor_1_1" and @class="prtlTopNavLnk"]')
            driver.execute_script('arguments[0].click()', reports)
            time.sleep(5)
            driver.set_page_load_timeout(450)

            trace.write('I','Cambiando de Frame.')
            driver.switch_to.frame(driver.find_element(By.CSS_SELECTOR,'iframe[id="ivuFrm_page0ivu2"]'))

            trace.write('I','Buscando Apartado de ventas mensuales.')
            driver.execute_script('arguments[0].click()',
                                  driver.find_element(By.XPATH, '//*[contains(text(), "Ventas Mensuales U")]'))
            time.sleep(5)

            # Cambiando de Frame
            trace.write('I','Cambiando de Frame')
            driver.switch_to.frame(driver.find_element(By.CSS_SELECTOR,
                'iframe[id="isolatedWorkArea"][name="isolatedWorkArea"]'))

            # Ingresando seccion
            trace.write('I','Seccion: ' + seccion)
            wait_element.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, 'input[name="VAR_VALUE_EXT_2"][id="VAR_VALUE_EXT_2"]'
                     ))).send_keys(seccion)
            time.sleep(5)

            # Oprimiendo boton ejecutar
            trace.write('I','Datos enviados, cargando formulario...')
            driver.execute_script('arguments[0].click()',
                                  driver.find_element(By.XPATH, '//nobr[contains(text(), "Ejecutar")]'))
            time.sleep(5)

            # Quita el filtro de cuidado con la piel en el usuario 2

            try:
                trace.write('I', 'Eliminando filtro para algunos usuario.')
                driver.find_element(By.XPATH, '//a[@title="Eliminar filtro"]')[1].click()
            except:
                trace.write('I', 'No se encontro el filtro para eliminar. ')
                pass

            #if '01' in params.supplier_id:
            try:
                driver.find_element(By.XPATH,'//a[@title="Eliminar filtro"]')[1].click()
            except:
                trace.write('I', 'No se encontro el filtro para eliminar. ')


            for numeric, metric in enumerate(select_metric):
                #trace.write('I',metric)
                #trace.write('I',numeric + 2)
                waiting_selection(driver, metric, numeric + 2, subtype)


            time.sleep(3)
            trace.write('I', 'Preparando reporte para descargar.')
            exporting_file(driver, paths.source, subtype, seccion)
            time.sleep(5)

            trace.write('I', 'Regresando al inicio del Portal')
            try:
                driver.switch_to.default_content()
                driver.find_element(By.XPATH, '//a[contains(text(),"Inicio")]').click()
            except Exception as e:
                raise Exception('Error, no se pudo regresar al inicio de Portal')

            time.sleep(10)

            fileHelper.wait_download(num_archivos, 'csv')

            num_archivos += 1

        trace.write('I', 'Proceso de navegacion  Finalizado.')

        #fileHelper.rename_files(filename, paths.source, 'csv')

        pre_transform_live00i(filename, date_from, subtype)

        fileHelper.send_to_gs(paths.source, filename, 'csv', params.bucket_src, False)



        # produccion
        current_subtask.set_src([filename + '.csv'], 'csv')

        # local
        # current_subtask.set_src(['da_30007_00_1_tott00i_day_2025-03-26_2025-03-26.zip'], 'csv')

        # Si no es .zip es .NONE
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

def pre_transform_live00v(filename, date_from,subtype):

    file_name_csv = f'{filename}.csv'
    file_path_out = os.path.join(paths.source, file_name_csv)
    df_array = []

    try:
        trace.write('I', f'Transformando Venta Diaria.')
        nombre_columnas = obtener_campos(subtype)

        files = [f for f in os.listdir(paths.source) if f.endswith('.csv') or f.endswith('.CSV') or f.endswith('.Csv')]


        for file in files:
            data = []
            if os.path.getsize(paths.source + file) > 100:
                with open(paths.source + file, 'r', encoding='utf-16') as f_input:
                    csv_input = csv.reader(f_input, delimiter=',', quotechar='"', lineterminator=chr(10))

                    for i, line in enumerate(csv_input):

                        if line:
                            line = [l.replace('$', '').replace(',', '').replace(chr(9), '').replace('"', '') for l in line]

                            if 'Resultado' not in line[11]:

                                if len(line) == 15:
                                    if i == 0:
                                        cadena = ''
                                        for columna in line:
                                            columna_original = unicodedata.normalize('NFD', columna)
                                            columna_sin_acento = ''.join(
                                                c for c in columna_original if unicodedata.category(c) != 'Mn')
                                            cadena = f'{cadena},{columna_sin_acento}'

                                        cadena = ','.join(cadena.split(',')[1:])
                                        data.append(cadena.split(','))

                                    else:
                                        cadena = ''
                                        for columna in line:
                                            columna = columna.replace(',', '').strip()
                                            cadena = f'{cadena},{columna}'

                                        cadena = ','.join(cadena.split(',')[1:])
                                        data.append(cadena.split(','))

                                else:
                                    raise Exception(f'Error en el registro {i + 1} del archivo {file}, se esperaban '
                                                    f'18 datos y se recibieron {len(line)}.')


            df = pd.DataFrame(data[1:], columns=data[0])
            df_array.append(df)

        df_final = pd.concat(df_array, ignore_index=True)
        columas_df = df_final.columns
        if (sorted(nombre_columnas) == sorted(columas_df)):
            archivos = os.listdir(paths.source)
            for archivo in archivos:
                #print(archivo)
                ruta_completa = os.path.join(paths.source, archivo)
                nombre_base, extension_actual = os.path.splitext(ruta_completa)
                # Crear el nuevo nombre de archivo con la nueva extension
                archivo_nuevo = nombre_base + '.txt'
                os.rename(ruta_completa, archivo_nuevo)

            df_final.to_csv(file_path_out, encoding='utf-8', index=False, header=True)

        else:
            trace.write('E', f'Las columas no coinciden.')
            trace.write('E', f'El archivo pudo ser descargaado previamente por alguien mas.')
            trace.write('E', f'Columnas del archivo: {columas_df}')
            trace.write('E', f'Columnas del json: {nombre_columnas}')
            raise Exception('E', f'Las columas no coinciden')


    except Exception as e:
        trace.write('E', f'Error en el Proceso de Pre-transformacion{e}')
        raise Exception('Error en la pre-transformacion')

def pre_transform_live00i(filename, date_from, subtype):
    file_name_csv = f'{filename}.csv'
    file_path_out = os.path.join(paths.source, file_name_csv)

    try:
        trace.write('I', f'Transformando Inventario Diaria.')
        nombre_columnas = obtener_campos(subtype)

        files = [f for f in os.listdir(paths.source) if f.endswith('.csv') or f.endswith('.CSV') or f.endswith('.Csv')]
        df_array = []

        for file in files:
            data = []
            if os.path.getsize(paths.source + file) > 100:
                with open(paths.source + file, 'r', encoding='utf-16') as f_input:
                    csv_input = csv.reader(f_input, delimiter=',', quotechar='"', lineterminator=chr(10))

                    for i, line in enumerate(csv_input):

                        if line:
                            line = [l.replace('$', '').replace(',', '').replace(chr(9), '').replace('"', '') for l in line]

                            if 'Resultado' not in line[3]:

                                if len(line) == 20:
                                    if i == 0:
                                        cadena = ''
                                        for columna in line:
                                            columna_original = unicodedata.normalize('NFD', columna)
                                            columna_sin_acento = ''.join(
                                                c for c in columna_original if unicodedata.category(c) != 'Mn')
                                            cadena = f'{cadena},{columna_sin_acento}'

                                        cadena = ','.join(cadena.split(',')[1:])
                                        cadena = f'{cadena},Flag A,Flag B'
                                        data.append(cadena.split(','))

                                    else:
                                        cadena = ''
                                        for columna in line:
                                            columna = columna.replace(',', '').strip()
                                            cadena = f'{cadena},{columna}'

                                        cadena = ','.join(cadena.split(',')[1:])
                                        if cadena.split(',')[5] == 'Activo':
                                            flag = 1
                                        else:
                                            flag = 0

                                        cadena = f'{cadena},{flag},{flag}'
                                        data.append(cadena.split(','))

                                else:
                                    raise Exception(f'Error en el registro {i + 1} del archivo {file}, se esperaban '
                                                    f'18 datos y se recibieron {len(line)}.')

            df = pd.DataFrame(data[1:], columns=data[0])
            df_array.append(df)

        df_final = pd.concat(df_array, ignore_index=True)
        columas_df = df_final.columns

        if (sorted(nombre_columnas) == sorted(columas_df)):
            archivos = os.listdir(paths.source)
            for archivo in archivos:
                #print(archivo)
                ruta_completa = os.path.join(paths.source, archivo)
                nombre_base, extension_actual = os.path.splitext(ruta_completa)
                # Crear el nuevo nombre de archivo con la nueva extension
                archivo_nuevo = nombre_base + '.txt'
                os.rename(ruta_completa, archivo_nuevo)

            df_final.to_csv(file_path_out, encoding='utf-8', index=False, header=True)

        else:
            trace.write('E', f'Las columas no coinciden.')
            trace.write('E', f'El archivo pudo ser descargaado previamente por alguien mas.')
            trace.write('E', f'Columnas del archivo: {columas_df}')
            trace.write('E', f'Columnas del json: {nombre_columnas}')
            raise Exception('E', f'Las columas no coinciden')

    except Exception as e:
        trace.write('E', f'Error en el Proceso de Pre-transformacion{e}')
        raise Exception('Error en la pre-transformacion')
# endregion download
def pre_transform_live00b(filename, date_from, subtype):
    file_name_csv = f'{filename}.csv'
    file_path_out = os.path.join(paths.source, file_name_csv)
    df_array = []

    try:
        trace.write('I', f'Transformando Venta Diaria.')
        nombre_columnas = obtener_campos(subtype)

        files = [f for f in os.listdir(paths.source) if f.endswith('.csv') or f.endswith('.CSV') or f.endswith('.Csv')]

        for file in files:
            data = []
            if os.path.getsize(paths.source + file) > 100:
                with open(paths.source + file, 'r', encoding='utf-16') as f_input:
                    csv_input = csv.reader(f_input, delimiter=',', quotechar='"', lineterminator=chr(10))

                    for i, line in enumerate(csv_input):

                        if line:
                            line = [l.replace('$', '').replace(',', '').replace(chr(9), '').replace('"', '') for l in
                                    line]

                            if 'Resultado' not in line[11]:

                                if len(line) == 15:
                                    if i == 0:
                                        cadena = ''
                                        for columna in line:
                                            columna_original = unicodedata.normalize('NFD', columna)
                                            columna_sin_acento = ''.join(
                                                c for c in columna_original if unicodedata.category(c) != 'Mn')
                                            cadena = f'{cadena},{columna_sin_acento}'

                                        cadena = ','.join(cadena.split(',')[1:])
                                        data.append(cadena.split(','))

                                    else:
                                        cadena = ''
                                        for columna in line:
                                            columna = columna.replace(',', '').strip()
                                            cadena = f'{cadena},{columna}'

                                        cadena = ','.join(cadena.split(',')[1:])
                                        data.append(cadena.split(','))

                                else:
                                    raise Exception(f'Error en el registro {i + 1} del archivo {file}, se esperaban '
                                                    f'18 datos y se recibieron {len(line)}.')

            df = pd.DataFrame(data[1:], columns=data[0])
            df_array.append(df)

        df_final = pd.concat(df_array, ignore_index=True)
        columas_df = df_final.columns
        if (sorted(nombre_columnas) == sorted(columas_df)):
            archivos = os.listdir(paths.source)
            for archivo in archivos:
                # print(archivo)
                ruta_completa = os.path.join(paths.source, archivo)
                nombre_base, extension_actual = os.path.splitext(ruta_completa)
                # Crear el nuevo nombre de archivo con la nueva extension
                archivo_nuevo = nombre_base + '.txt'
                os.rename(ruta_completa, archivo_nuevo)

            df_final.to_csv(file_path_out, encoding='utf-8', index=False, header=True)

        else:
            trace.write('E', f'Las columas no coinciden.')
            trace.write('E', f'El archivo pudo ser descargaado previamente por alguien mas.')
            trace.write('E', f'Columnas del archivo: {columas_df}')
            trace.write('E', f'Columnas del json: {nombre_columnas}')
            raise Exception('E', f'Las columas no coinciden')


    except Exception as e:
        trace.write('E', f'Error en el Proceso de Pre-transformacion{e}')
        raise Exception('Error en la pre-transformacion')

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

    serv = Service(r'C:\chromedriver.exe')
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

    # Siempre se ven asi
    error_code = 0
    robot_result = common.Result(params)

     #para provar el tranform
    """pre_transform_live00i('da_00004_00_1_live00i_mon_2025-06-20_2025-06-20', '2025-06-20', 'live00i')
    # file = f'{name_hecho}.csv' #Nombre dela rchivo descargado
    curr = common.Subtask('live00i', '2025-06-20')  # datos del json
    curr.set_src(['da_00004_00_1_live00i_mon_2025-06-20_2025-06-20.csv'], 'csv')  # tipo de archivo
    robot_ft.transform_report(trace, params, paths, fileHelper, curr, common.ZipType.NONE)"""

    driver = set_driver(params, trace, paths)
    login()
    time.sleep(3)
    close_note()
    time.sleep(1)

    subtypes = get_subtypes_to_process()
    trace.write("I", f"subtypes a procesar: {subtypes}")

    last_date_inoh = ''
    last_date_sale = ''

    for subtype in subtypes:
        match subtype:
            case 'live00v':
                process_subtype(download_report_live00v, subtype, last_date_sale,
                                common.AllowDateRange.No)
            case 'live00i':
                process_subtype(download_report_live00i, subtype, last_date_inoh,
                                common.AllowDateRange.No)
            case 'live00b':
                process_subtype(download_report_live00v, subtype, last_date_inoh,
                                common.AllowDateRange.No)
            case _:
                result_status = {"status": "E", "status_code": 910,
                                 "status_message": f"No esta definido el subtype {subtype} en el codigo del robot!"}
                pass

    end_program(error_code, 'Tarea Finalizada.')


if __name__ == "__main__":
    main()

