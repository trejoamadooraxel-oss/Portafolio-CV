"""
-------------------------------------------------------
SCRIPT DE AUTOMATIZACION ETL PARA SORIANA
CIUDAD DE MEXICO, MEXICO VALLEJO, 2026
TODOS LOS DERECHOS RECERVADOS A CREMERIA AMAERICANA
-------------------------------------------------------
AREA DEVELOPER: Desarrollador Bi
DEVELOPER CREATE: Axel Trejo Amador
DEVELOPER DATE: 09/06/2026
LAST DATE UPDATE:
UPDATE DESCRIPTION:
-------------------------------------------------------
Parameters Execution:

-in_direccion
ttps://retaillink.wal-mart.com/home
-in_usuario

-in_password
-in_fechain
07/06/2026
-in_descarga
sell_out
-in_variables
1
-------------------------------------------------------

"""
from builtins import print

from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains
from selenium import webdriver
from datetime import date, timedelta
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc
import platform
from selenium.webdriver.common.keys import Keys

import time
import calendar
import zipfile
import os
import argparse
import sys
import xlrd
import csv
import pandas as pd
import gzip
import shutil
import unicodedata
import openpyxl
import requests
from datetime import date, timedelta, datetime
#Correo
import imghdr
import smtplib
import ssl
from email.message import EmailMessage

import zipfile
from pathlib import Path

def send_email(driver, subject, task):
    # Envio de captura a correo electronico para monitorear al portal en tiempo real
    time.sleep(5)
    driver.save_screenshot('screen.png')
    user_mail = ''
    password_mail = ''
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = user_mail
    msg['To'] = user_mail
    msg.set_content(task)

    with open('screen.png', 'rb') as content:
        data_image = content.read()
        data_type = imghdr.what(content.name)
        data_name = content.name

    msg.add_attachment(data_image, maintype='image', subtype=data_type, filename=data_name)

    with smtplib.SMTP('smtp.office365.com', 587) as smtp:
        context = ssl.create_default_context()
        smtp.starttls(context=context)
        smtp.login(user_mail, password_mail)
        smtp.send_message(msg)
        print('Envio de correo exitoso.')


def open_new_tab_download_file(driver, element):
    # Funcion para evitar abrir nuevo tab al descargar archivos.
    # open element in same tab override javascript for that
    driver.execute_script('window.open = function(url) {window.location=url}')

    # click on element to download file
    driver.execute_script("arguments[0].click()", element)


def enable_download_in_headless_chrome(driver, path):
    # add missing support for chrome "send_command"  to selenium webdriver
    driver.command_executor._commands["send_command"] = ("POST",'/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': path}}
    command_result = driver.execute("send_command", params)


def limpiar(dir_temp, dir_fuentes):
    carpetas = [dir_temp, dir_fuentes]
    for c in carpetas:
        f = [name for name in os.listdir(c)]
        for file in f:
            os.remove(c + file)


def setDriver(address, path):
    print('Inicio Ejecucion')

    # 1. Inicializamos las opciones nativas de UC
    chrome_options = uc.ChromeOptions()

    # Detectar OS
    so = platform.system()

    if os.name != "nt":
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-component-update")
        chrome_options.add_argument("--no-first-run")
    else:
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--window-size=1336,768")
        chrome_options.add_argument("--disable-component-update")
        chrome_options.add_argument("--no-first-run")

    chrome_options.add_argument("-allow-running-insecure-content")

    # Ruta de Chrome según OS
    if so == "Darwin":  # Mac
        chrome_options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    elif so == "Windows":
        chrome_options.binary_location = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
    elif so == "Linux":
        chrome_options.binary_location = "/usr/bin/google-chrome"

    # 2. Configuración del directorio de descargas
    prefs = {
        "download.default_directory": path,
        "download.prompt_for_download": False,
        "directory_upgrade": True
    }
    chrome_options.add_experimental_option("prefs", prefs)

    print("Iniciando driver indetectable")

    try:
        if os.name == "nt":
            driver = uc.Chrome(
                options=chrome_options,
                use_subprocess=True
            )
        else:
            driver = uc.Chrome(
                options=chrome_options,
                use_subprocess=True
            )

        time.sleep(2)  # dale tiempo a inicializar
        print("Driver listo")
        time.sleep(5)      
        
    except Exception as e:
        print(f'Error al acceder al portal: {e}')
        sys.exit(0)

    return driver



def iniciar_sesion(driver, username_text, password_text, path_file):

    try:
        text_user = username_text
        text_pass = password_text.split('|')[0]
        text_token = password_text.split('|')[1]

        url = 'https://retaillink.login.wal-mart.com/api/login'
        password = text_pass
        token = text_token

        headers = {
            'content-type': 'application/json',
            'referer': 'https://retaillink.wal-mart.com/home',
            'x-bot-token': token

        }
        data = {
            "username": text_user,
            "password": password,
            "language": "en"
        }
        cookies = {
            "lang": "en"
        }

        print( f"Iniciando sesion.")
    except Exception as e:
        print( f"Error Iniciando sesion: {e}.\n")
        raise Exception(e)

    try:
        s = requests.session()
        s.headers.update(headers)

        driver.get('https://retaillink.wal-mart.com/home')
        time.sleep(5)

        s.cookies.set('lang', 'en')

        respuesta = s.post(url, json=data)

        j_data = respuesta.json()
        #print(j_data)
        direccion = j_data['payload']['redirectTo']
        for cook in s.cookies.items():
            driver.add_cookie({'name': cook[0], 'value': cook[1], 'domain': '.wal-mart.com'})

        if direccion == "/change-password":
            raise Exception('999_Finalizado con posible error de contraseña vencida.')

        driver.get(direccion)
        time.sleep(5)
        print("Acceso Correcto")

    except Exception as e:
        print(f"Error: {e}\n")











    
    except Exception as e:
        print(e)
        print('Finalizado Error de Acceso')
        print('Finalizado Error de Acceso zona segura')
        print('Finalizado con Error Desconocido')
        driver.close()
        sys.exit(0)

    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((
            By.XPATH, '//span[contains(text(), "Usuario o contrase") and contains(text(), "a incorrectos.")]')))
        print('Usuario o Pass Incorrectos.')
        print('Finalizado Error de Acceso')
        driver.quit()
        sys.exit(0)
    except:
       pass

    return driver

def rename_file_download(dir_fuentes,in_variables, in_descarga,in_fechaini   ):
    try:
        archivos = [name for name in os.listdir(dir_fuentes)]
        for index, archivo in enumerate(archivos):
            if in_variables == 'Walmart':
                nuevo_nombre = f'{in_variables}_{str(in_descarga).split('/')[0]}_{str(in_fechaini).replace('/', '')}_{index}_origen.{str(archivo).split('.')[-1]}'

            else:
                nuevo_nombre = f'{in_variables}_{in_descarga}_{str(in_fechaini).replace('/', '')}_{index}_Input.{str(archivo).split('.')[-1]}'

            archivo_original = Path(os.path.join(dir_fuentes, archivo))
            archivo_renombrado = archivo_original.with_name(nuevo_nombre)
            archivo_original.rename(archivo_renombrado)

            print(f'El archivo se renombro: {nuevo_nombre}')

    except Exception as e:
        print(f'Error. Al obtener el nuevo nombre para el archivo descargado de {in_variables}: {e}')


def busqueda_reportes(driver, dir_fuentes, in_fechaini, in_descarga, in_variables,dir_temp, dir_storage):

    months = {
        '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
    }

    str(in_descarga).split('/')[0]

    name_report = (f"CA_{str(in_descarga).split('/')[0]}_{str(in_fechaini.replace('/', ''))}")

    print("Busqueda de Reporte")

    if 'fill_rate' in in_descarga:

        print("Iniciando Descarga Fill Rate.")

        print('Refireccionamos a la pagina: Decision Support ')
        driver.get('https://retaillink2.wal-mart.com/decision_support/')

        if 'peticion' in in_descarga:
            try:
                time.sleep(5)
                primera_ventana = driver.window_handles[0]
                for ventana in driver.window_handles[1:]:
                    driver.switch_to.window(ventana)
                    driver.close()
                driver.switch_to.window(primera_ventana)
                driver.switch_to.default_content()

                print('Generamos la solicitud para crear el reporte de Fill Rate.')
                driver.switch_to.default_content()

                print('Seleccionamos: My Reports')
                driver.find_element(By.XPATH,'//a[@title="My Saved Reports"]').click()
                time.sleep(3)

                driver.switch_to.default_content()

                iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="ifrContent"]'))
                )
                driver.switch_to.frame(iframe_1)
                driver.find_element(By.XPATH,
                                    '//span[text()="Cremeria_Americana"]//parent::span/img[@src="images/tree/plus.gif"]').click()
                time.sleep(3)

                print('Seleccionamos el reporte con click derecho a: CA_FILL_RATE_FULL')
                report = driver.find_element(By.XPATH, f'//span[contains(text(),"CA_FILL_RATE_FULL")]/img')
                ActionChains(driver).context_click(report).perform()
                time.sleep(3)

                print('Seleccionamos: Modify.')
                driver.find_element(By.XPATH, '//a[text()="Modify"]').click()

                print('Cambiamos la ventana para crear el reporte.')
                todas_las_ventanas = driver.window_handles
                driver.switch_to.window(todas_las_ventanas[-1])

                driver.switch_to.default_content()
                iframe_2 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="menu"]')))
                driver.switch_to.frame(iframe_2)

                print('Seleccionamos la metrica: Submite')
                driver.find_element(By.XPATH, '//div[@id="step6txt"]').click()

                driver.switch_to.default_content()
                time.sleep(3)
                iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@name="content_6"]')))
                driver.switch_to.frame(iframe_3)
                time.sleep(3)

                input = driver.find_element(By.XPATH, '//input[@id="title"]')
                input.clear()
                time.sleep(1)
                input.send_keys(f'{name_report}')
                time.sleep(3)

                print('Ejecutamos la consulta.')
                print('Seleccionamos: Run Now.')
                driver.find_element(By.XPATH, '//input[@title="Run Now"]').click()
                time.sleep(3)

                print('Verificando si se genero la consulta.')
                iframe_4 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@name="SubmitWindow"]')))
                driver.switch_to.frame(iframe_4)
                time.sleep(3)

                try:
                    driver.find_element(By.XPATH, '//center[text()="Query Submitted"]')
                    print('Reporte Solicitado con Exito.')
                except NoSuchElementException:
                    try:
                        driver.find_element(By.XPATH, '//center[text()="Multiple submits of this request are not allowed."]')
                        print('El reporte ya se solicito con anterioridad.')
                    except Exception as e:
                        pass

                try:
                    driver.find_element(By.XPATH, '//center[text()="Query Submitted"]')
                    print('Reporte Solicitado con Exito.')
                except NoSuchElementException:

                    print('No se encontro la confirmacion de creacion.')
                    print('Volviendo a generar el reporte.')

                    driver.switch_to.default_content()
                    time.sleep(3)
                    iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                        By.XPATH, '//iframe[@name="content_6"]')))
                    driver.switch_to.frame(iframe_3)
                    time.sleep(2)

                    print('Ejecutamos la consulta.')
                    print('Seleccionamos: Run Now.')
                    driver.find_element(By.XPATH, '//input[@title="Run Now"]').click()
                    time.sleep(3)

                    print('Verificando si se genero la consulta.')
                    iframe_4 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                        By.XPATH, '//iframe[@name="SubmitWindow"]')))
                    driver.switch_to.frame(iframe_4)
                    time.sleep(2)

                    try:
                        driver.find_element(By.XPATH, '//center[text()="Query Submitted"]')
                        print('Reporte Solicitado con Exito, al segundo intento.')
                    except Exception as e:
                        print('No se pido generar la sonsulta al segundo intento')
                        print('Revisar el Script')
                        raise Exception('No se pido generar la sonsulta al segundo intento.')

                """driver.switch_to.default_content()
                time.sleep(2)
                iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="content_6"]')))
                driver.switch_to.frame(iframe_3)
                time.sleep(2)
    
                input = driver.find_element(By.XPATH, '//input[@id="title"]')
                input.send_keys(in_fechaini)
    
                # iframes = driver.find_elements(By.XPATH, '//iframe[@id="left')[0]
                iframe_4 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="left"]')))
                driver.switch_to.frame(iframe_4)
                time.sleep(2)
    
                driver.find_element(By.XPATH, '//a[text()="Time Range 1"]').click()
                """
                print('Exito: Finalizando la solicitud de Reporte.')
            except Exception as e:
                print(e)
                print('Error: Finalizado al Solicitar el reporte.')
                driver.close()
                sys.exit(0)

        if 'descarga' in in_descarga:
            try:
                primera_ventana = driver.window_handles[0]
                for ventana in driver.window_handles[1:]:
                    driver.switch_to.window(ventana)
                    driver.close()
                driver.switch_to.window(primera_ventana)
                driver.switch_to.default_content()

                print('Descargando el reporte de Fill Rate generado.')
                driver.switch_to.default_content()

                print('Seleccionamos: Status')
                driver.find_element(By.XPATH, '//a[@title="Report Status Page"]').click()
                time.sleep(2)

                driver.switch_to.default_content()

                iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="ifrContent"]'))
                )
                driver.switch_to.frame(iframe_1)

                iframe_2 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="JobTable"]'))
                )
                driver.switch_to.frame(iframe_2)
                time.sleep(5)

                try:
                    print(f'Buscando el archivo a descargar: {name_report}')
                    driver.find_element(By.XPATH, f'//span[text()=" {name_report} "]').click()
                    time.sleep(10)
                    todas_las_ventanas = driver.window_handles
                    driver.switch_to.window(todas_las_ventanas[-1])
                    driver.switch_to.default_content()

                    driver.find_element(By.XPATH, '//font[@color="navy" and text()=" Request Name :"]')

                    print(f'Descarga del archivo: {name_report}, Exitosa.')

                except NoSuchElementException:
                    raise Exception('No se localiza el Reporte Solicitado: ')

                time.sleep(5)
                esperar_descarga(dir_fuentes, 1)
                time.sleep(2)
                rename_file_download(dir_fuentes,in_variables, in_descarga,in_fechaini)

            except Exception as e:
                print(e)
                print('Error: Finalizado al Solicitar el reporte.')
                driver.close()
                sys.exit(0)

    if 'sell_out' in in_descarga:

        print("Iniciando Descarga Sell Out.")

        print('Refireccionamos a la pagina: Decision Support ')
        driver.get('https://retaillink2.wal-mart.com/decision_support/')

        if 'peticion' in in_descarga:
            try:
                time.sleep(5)
                primera_ventana = driver.window_handles[0]
                for ventana in driver.window_handles[1:]:
                    driver.switch_to.window(ventana)
                    driver.close()
                driver.switch_to.window(primera_ventana)
                driver.switch_to.default_content()

                print('Generamos la solicitud para crear el reporte de Sell_Out.')
                driver.switch_to.default_content()

                print('Seleccionamos: My Reports')
                driver.find_element(By.XPATH, '//a[@title="My Saved Reports"]').click()
                time.sleep(3)

                driver.switch_to.default_content()

                iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="ifrContent"]'))
                )
                driver.switch_to.frame(iframe_1)
                driver.find_element(By.XPATH,
                                    '//span[text()="Cremeria_Americana"]//parent::span/img[@src="images/tree/plus.gif"]').click()
                time.sleep(3)

                print('Seleccionamos el reporte con click derecho a: CA_SELL_OUT_FULL')
                report = driver.find_element(By.XPATH, f'//span[contains(text(),"CA_SELL_OUT_FULL")]/img')
                ActionChains(driver).context_click(report).perform()
                time.sleep(3)

                print('Seleccionamos: Modify.')
                driver.find_element(By.XPATH, '//a[text()="Modify"]').click()

                print('Cambiamos la ventana para crear el reporte.')
                todas_las_ventanas = driver.window_handles
                driver.switch_to.window(todas_las_ventanas[-1])

                driver.switch_to.default_content()
                iframe_2 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="menu"]')))
                driver.switch_to.frame(iframe_2)

                print('Seleccionamos la metrica: Time')
                driver.find_element(By.XPATH, '//div[@id="step4txt"]').click()

                driver.switch_to.default_content()
                time.sleep(3)
                iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@name="content_4"]')))
                driver.switch_to.frame(iframe_3)
                time.sleep(3)

                iframe_4 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="left"]')))
                driver.switch_to.frame(iframe_4)
                time.sleep(2)

                print('Seleccionamos: Time Range 1')
                driver.find_element(By.XPATH, '//a[text()="Time Range 1"]').click()
                time.sleep(3)

                driver.switch_to.default_content()

                iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@name="content_4"]')))
                driver.switch_to.frame(iframe_3)

                iframe_4 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="mid"]')))
                driver.switch_to.frame(iframe_4)

                iframe_5 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="F1000010_frame"]')))
                driver.switch_to.frame(iframe_5)

                print('Seleccionamos: Pos Date. (mm/dd/yyyy).')
                WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//a[@id="204F1000010"]//parent::div//a'))).click()

                time.sleep(3)

                print('Seleccionamos: Time Range 1 Is Between. ')
                WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//a[contains(text(),"Time Range 1 Is Between")]'))).click()
                time.sleep(3)

                driver.switch_to.default_content()

                iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@name="content_4"]')))
                driver.switch_to.frame(iframe_3)

                iframe_6 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="right"]')))
                driver.switch_to.frame(iframe_6)

                iframe_7 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="filter_values"]')))
                driver.switch_to.frame(iframe_7)

                driver.find_element(By.XPATH, '//select[@id="filllist"]/option').click()
                time.sleep(2)

                driver.switch_to.default_content()

                iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@name="content_4"]')))
                driver.switch_to.frame(iframe_3)

                iframe_6 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="right"]')))
                driver.switch_to.frame(iframe_6)

                print('Seleccionamos: Modify.')
                driver.find_element(By.XPATH, '//input[@id="Modify"]').click()
                time.sleep(2)

                date_search = str(datetime.strptime(in_fechaini, '%d/%m/%Y').strftime('%m/%d/%Y'))

                print(f'Ingresmos la Fecha a buscar: {date_search}')
                # Creamos la condicional
                # date_search_menos =  (datetime.strptime(in_fechaini, '%d/%m/%Y') - timedelta(days=n)).strftime('%m/%d/%Y')

                fecha_desde = (driver.find_element(By.XPATH, '//input[@id="firstValue"]'))
                fecha_hasta = (driver.find_element(By.XPATH, '//input[@id="lastValue"]'))

                fecha_desde.clear()
                fecha_desde.send_keys(date_search)
                fecha_hasta.clear()
                fecha_hasta.send_keys(date_search)
                time.sleep(2)

                print('Seleccionamos: Modify.')
                driver.find_element(By.XPATH, '//input[@id="btnAnd"]').click()
                time.sleep(2)

                driver.switch_to.default_content()
                iframe_2 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="menu"]')))
                driver.switch_to.frame(iframe_2)

                print('Seleccionamos la metrica: Submit')
                driver.find_element(By.XPATH, '//div[@id="step6txt"]').click()


                driver.switch_to.default_content()
                time.sleep(3)
                iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@name="content_6"]')))
                driver.switch_to.frame(iframe_3)
                time.sleep(3)

                input = driver.find_element(By.XPATH, '//input[@id="title"]')
                input.clear()
                time.sleep(1)
                input.send_keys(f'{name_report}')
                time.sleep(3)

                print('Ejecutamos la consulta.')
                print('Seleccionamos: Run Now.')
                driver.find_element(By.XPATH, '//input[@title="Run Now"]').click()
                time.sleep(3)

                print('Verificando si se genero la consulta.')
                iframe_4 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@name="SubmitWindow"]')))
                driver.switch_to.frame(iframe_4)
                time.sleep(3)

                try:
                    driver.find_element(By.XPATH, '//center[text()="Query Submitted"]')
                    print('Reporte Solicitado con Exito.')
                except NoSuchElementException:
                    try:
                        driver.find_element(By.XPATH,
                                            '//center[text()="Multiple submits of this request are not allowed."]')
                        print('El reporte ya se solicito con anterioridad.')
                    except Exception as e:
                        pass

                try:
                    driver.find_element(By.XPATH, '//center[text()="Query Submitted"]')
                    print('Reporte Solicitado con Exito.')
                except NoSuchElementException:

                    print('No se encontro la confirmacion de creacion.')
                    print('Volviendo a generar el reporte.')

                    driver.switch_to.default_content()
                    time.sleep(3)
                    iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                        By.XPATH, '//iframe[@name="content_6"]')))
                    driver.switch_to.frame(iframe_3)
                    time.sleep(2)

                    print('Ejecutamos la consulta.')
                    print('Seleccionamos: Run Now.')
                    driver.find_element(By.XPATH, '//input[@title="Run Now"]').click()
                    time.sleep(3)

                    print('Verificando si se genero la consulta.')
                    iframe_4 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                        By.XPATH, '//iframe[@name="SubmitWindow"]')))
                    driver.switch_to.frame(iframe_4)
                    time.sleep(2)

                    try:
                        driver.find_element(By.XPATH, '//center[text()="Query Submitted"]')
                        print('Reporte Solicitado con Exito, al segundo intento.')
                    except Exception as e:
                        print('No se pido generar la sonsulta al segundo intento')
                        print('Revisar el Script')
                        raise Exception('No se pido generar la sonsulta al segundo intento.')

                """driver.switch_to.default_content()
                time.sleep(2)
                iframe_3 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="content_6"]')))
                driver.switch_to.frame(iframe_3)
                time.sleep(2)

                input = driver.find_element(By.XPATH, '//input[@id="title"]')
                input.send_keys(in_fechaini)

                # iframes = driver.find_elements(By.XPATH, '//iframe[@id="left')[0]
                iframe_4 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="left"]')))
                driver.switch_to.frame(iframe_4)
                time.sleep(2)

                driver.find_element(By.XPATH, '//a[text()="Time Range 1"]').click()
                """
                print('Exito: Finalizando la solicitud de Reporte.')
            except Exception as e:
                print(e)
                print('Error: Finalizado al Solicitar el reporte.')
                driver.close()
                sys.exit(0)

        if 'descarga' in in_descarga:
            try:
                primera_ventana = driver.window_handles[0]
                for ventana in driver.window_handles[1:]:
                    driver.switch_to.window(ventana)
                    driver.close()
                driver.switch_to.window(primera_ventana)
                driver.switch_to.default_content()

                print('Descargando el reporte de Fill Rate generado.')
                driver.switch_to.default_content()

                print('Seleccionamos: Status')
                driver.find_element(By.XPATH, '//a[@title="Report Status Page"]').click()
                time.sleep(2)

                driver.switch_to.default_content()

                iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="ifrContent"]'))
                )
                driver.switch_to.frame(iframe_1)

                iframe_2 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                    By.XPATH, '//iframe[@id="JobTable"]'))
                )
                driver.switch_to.frame(iframe_2)
                time.sleep(5)

                try:
                    print(f'Buscando el archivo a descargar: {name_report}')
                    driver.find_element(By.XPATH, f'//span[text()=" {name_report} "]').click()
                    time.sleep(10)
                    todas_las_ventanas = driver.window_handles
                    driver.switch_to.window(todas_las_ventanas[-1])
                    driver.switch_to.default_content()

                    driver.find_element(By.XPATH, '//font[@color="navy" and text()=" Request Name :"]')

                    print(f'Descarga del archivo: {name_report}, Exitosa.')

                except NoSuchElementException:
                    raise Exception('No se localiza el Reporte Solicitado: ')

                time.sleep(5)
                esperar_descarga(dir_fuentes, 1)
                time.sleep(2)
                rename_file_download(dir_fuentes, in_variables, in_descarga, in_fechaini)

            except Exception as e:
                print(e)
                print('Error: Finalizado al Solicitar el reporte.')
                driver.close()
                sys.exit(0)



def comprimir_archivos(dir_fuentes, dir_temp, dir_storage,in_variables, in_descarga,in_fechaini):
    carpeta_A = Path(dir_fuentes)
    carpeta_B = Path(dir_temp)
    carpeta_destino = Path(dir_storage)

    fecha_consulta = str(in_fechaini).replace('/', '')

    # 2. Definir la ruta del archivo ZIP final
    archivo_zip_final = carpeta_destino / f"{in_variables}_{str(in_descarga).split('/')[0]}_{fecha_consulta}.zip"

    # Asegurar que la tercera carpeta exista
    carpeta_destino.mkdir(parents=True, exist_ok=True)

    # 3. Proceso de compresión unificado
    with zipfile.ZipFile(archivo_zip_final, mode="w", compression=zipfile.ZIP_DEFLATED) as zip_file:

        # Comprimir elementos de la Carpeta A
        for archivo in carpeta_A.iterdir():
            if archivo.is_file():
                zip_file.write(archivo, arcname=archivo.name)

        # Comprimir elementos de la Carpeta B
        for archivo in carpeta_B.iterdir():
            if archivo.is_file():
                zip_file.write(archivo, arcname=archivo.name)

    print(f"Archivo guardado en: {archivo_zip_final}")


def transformar_archivo(dir_fuentes, dir_temp, dir_storage,in_fechaini, in_descarga, in_variables):

    if 'fill_rate' in in_descarga:

        try:

            name_file = f'{in_variables}_{str(in_descarga).split('/')[0]}_{str(in_fechaini).replace('/', '')}_tranform.csv'
            file_path = os.path.join(dir_temp, name_file)

            archivos = [name for name in os.listdir(dir_fuentes)]

            for archivo in archivos:

                if '.xls' in archivo:

                    df = pd.read_excel(f'{dir_fuentes}/{archivo}', dtype=str)
                    df = df.fillna(0)

                    df = df[(df['Unnamed: 0'] != 0) & (df['Unnamed: 3'] != 0)]
                    df.columns = df.iloc[0]
                    # 2. Mantener solo las filas desde la posición 1 en adelante y restablecer el índice
                    df_nuevo = df[1:].reset_index(drop=True)

                    columnas_espanol = [
                        "Num Orden de Compra",
                        "Num Producto",
                        "Flags",
                        "UPC",
                        "Desc #1 de Producto",
                        "Desc #2 de Producto",
                        "Fecha Cancelacion Orden de Compra",
                        "Fecha Creacion Orden de Compra",
                        "Gramaje por Unidad",
                        "Cant Unidades por paquete",
                        "Cantidad de Paquetes Historicos Ordenados al Almacen",
                        "Cantidad de Paquetes Historicos Recibidos en Almacen",
                        "Costo Historico de lo Ordenado al Almacen"
                    ]

                    df_nuevo.columns = columnas_espanol

                    for columna in df_nuevo.columns:
                        df_nuevo[columna] = df_nuevo[columna].apply(
                            lambda x: str(x).replace('$', '').replace(',', '').replace('%', '').replace('            ',
                                                                                                        ' ').strip())

                        if 'Fecha' in columna:
                            df_nuevo[columna] = df_nuevo[columna].apply(
                                lambda x: str(datetime.strptime(x, '%m/%d/%Y').strftime('%Y-%m-%d').strip()))

                        if "UPC" == columna:
                            df_nuevo[columna] = df_nuevo[columna].astype(int)

                    df_nuevo['CA Fecha Consulta'] = datetime.today().strftime('%Y-%m-%d')
                    df_nuevo['CA Hora Consulta'] = datetime.today().strftime('%H:%M:%S')

                    df_nuevo.to_csv(file_path, encoding='utf-8', index=False, header=True)

        except Exception as e:
            print(e)
            print(f'Error de Transformacion de {in_descarga} para {in_variables}')
            sys.exit(0)

        comprimir_archivos(dir_fuentes, dir_temp, dir_storage,in_variables, in_descarga, in_fechaini )

def esperar_descarga(dir_fuentes, can_arch):
    print("Esperando descarga")
    t1 = datetime.now()
    archivos = [name for name in os.listdir(dir_fuentes) if name.endswith(".txt") or name.endswith(".xlsx") or name.endswith(".csv") or name.endswith(".xls") or name.endswith(".zip")]
    #de 600 aumenté a 1200
    while len(archivos) < can_arch and (datetime.now()-t1).seconds <= 1200:
        time.sleep(5)
        archivos = [name for name in os.listdir(dir_fuentes) if name.endswith(".txt") or name.endswith(".xlsx") or name.endswith(".csv") or name.endswith(".xls") or name.endswith(".zip")]
    if any(File.endswith(".txt") or File.endswith(".csv") or File.endswith(".xlsx")  or File.endswith(".xls")  or File.endswith(".zip") for File in os.listdir(dir_fuentes)):
        print("Reporte Descargado.")
        #driver.close()
    else:
        print('Error al descargar archivo.')
        #driver.close()
        #sys.exit(0)


def borrar_archivo(dir_temp):
    archivo = [name for name in os.listdir(dir_temp) if name.endswith(".csv")]
    for arc in archivo:
        file = arc.split('.csv')[0]
        if os.path.getsize(dir_temp + file + '.csv') == 0:
            os.remove(dir_temp + file + '.csv')


def csv_from_excel(dir_fuentes, inf, outf):
    df = pd.read_excel(dir_fuentes + inf + '.xlsx')
    df.to_csv(dir_fuentes + outf + '1.csv', encoding='latin-1')

    outfile = open(dir_fuentes + outf + '.csv', 'w', encoding='latin-1')
    f = open(dir_fuentes + outf + '1.csv', 'r', encoding='latin-1')
    for line in f:
        outfile.write(','.join(line.split(',')[1:]))
    outfile.close()
    f.close()
    os.remove(dir_fuentes + outf + '1.csv')


def csv_from_xls(dir_fuentes, inf, outf):
    df = pd.read_excel(dir_fuentes + inf + '.xls')
    df.to_csv(dir_fuentes + outf + '1.csv', encoding='latin-1')

    outfile = open(dir_fuentes + outf + '.csv', 'w', encoding='latin-1')
    f = open(dir_fuentes + outf + '1.csv', 'r', encoding='latin-1')
    for line in f:
        outfile.write(','.join(line.split(',')[1:]))
    outfile.close()
    f.close()
    os.remove(dir_fuentes + outf + '1.csv')
    



def parser_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-in_usuario", help="Usuario para iniciar sesion en el portal", required=True)
    parser.add_argument("-in_password", help="Contrasenia para iniciar sesion en el portal", required=True)
    parser.add_argument("-in_direccion", help="Url del portal", required=True)
    parser.add_argument("-in_fechaini", help="Fecha inicial para buscar en el portal", required=True)
    parser.add_argument("-in_fechafin", help="Fecha inicial para buscar en el portal", required=False)
    parser.add_argument("-in_descarga", help="Nombre del archivo salida", required=True)## XXXXXXX_XXXXXXX_cityclq_dl # l = 0000 # 2 = 0001 #
    parser.add_argument("-in_variables", help="Tipo de archivo a descargar 'csv' o 'xls'", required=True)
    args = parser.parse_args()
    return args

def main():
    if os.name == "nt":
        path_images = os.path.dirname(os.path.abspath(__file__)) + '\\images\\'  # Se guardan las imagenes en caso de requerir image recognition
        dir_temp = os.path.dirname(os.path.abspath(__file__)) + '\\_Temp\\'  # Aqui se realizan las transformaciones necesarias al archivo
        dir_fuentes = os.path.dirname(os.path.abspath(__file__)) + '\\_Fuentes\\'  # Aqui se descargan los archivos
        dir_storage = os.path.dirname(os.path.abspath(__file__)) + '\\Storage\\'  # Aqui se almacenan una vez comprimidos.

    else:
        path_images = os.path.dirname(os.path.abspath(__file__)) + '/images/'  # Se guardan las imagenes en caso de requerir image recognition
        dir_temp = os.path.dirname(os.path.abspath(__file__)) + '/_Temp/'  # Aqui se realizan las transformaciones necesarias al archivo
        dir_fuentes = os.path.dirname(os.path.abspath(__file__)) + '/_Fuentes/'  # Aqui se descargan los archivos
        dir_storage = os.path.dirname(os.path.abspath(__file__)) + '\\Storage\\'  # Aqui se almacenan una vez comprimidos.

    args = parser_args()

    limpiar(dir_temp, dir_fuentes)
    driver = setDriver(args.in_direccion,dir_fuentes)
    driver = iniciar_sesion(driver, args.in_usuario, args.in_password, args.in_descarga)
    busqueda_reportes(driver, dir_fuentes, args.in_fechaini, args.in_descarga, args.in_variables, dir_temp, dir_storage)
    if 'descarga' in str(args.in_descarga).split('/')[-1]:
        transformar_archivo(dir_fuentes, dir_temp,dir_storage ,args.in_fechaini, args.in_descarga, args.in_variables)

    print('Tarea Finalizada Ok')



if __name__ == "__main__":
    main()

