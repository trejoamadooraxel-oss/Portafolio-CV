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
        time.sleep(10)
        print("Acceso Correcto")

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

def comprimir_archivos(dir_fuentes, dir_temp, dir_storage,in_variables, in_descarga,in_fechaini):
    carpeta_A = Path(dir_fuentes)
    carpeta_B = Path(dir_temp)
    carpeta_destino = Path(dir_storage)

    fecha_consulta = str(in_fechaini).replace('/', '')

    # 2. Definir la ruta del archivo ZIP final
    archivo_zip_final = carpeta_destino / f"{in_variables}_{in_descarga}_{fecha_consulta}.zip"

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

def busqueda_reportes(driver, dir_fuentes, in_fechaini, in_descarga, in_variables,dir_temp, dir_storage):
    print("Busqueda de Reporte")


    primera_ventana = driver.window_handles[0]
    for ventana in driver.window_handles[1:]:
        driver.switch_to.window(ventana)
        driver.close()
    driver.switch_to.window(primera_ventana)
    driver.switch_to.default_content()

    if 'varios' in in_descarga:

        print('Redireccionando a: Club Within A Club Report for')
        driver.get(
            'https://retaillink2.wal-mart.com/swas/buyer_recap.aspx?groupId=1&division=18&area=merch&ch=H3&ukey=W4930')
        time.sleep(8)

        types_download = ['Sales By Member Type', 'Short Shipped PO', 'Club Aged Inventory', 'Defective Returns',
                         'Total Member Returns']


        for index, type_download in enumerate(types_download):

            num_archivos = index

            if type_download == 'Sales By Member Type':
                descarga = 'Sell_out'
            elif type_download == 'Short Shipped PO':
                descarga = 'Fill_rate'
            elif type_download == 'Club Aged Inventory':
                descarga = 'Inventories'
            elif type_download == 'Defective Returns':
                descarga = 'Devoluciones History'
            elif type_download == 'Total Member Returns':
                descarga = 'Devoluciones'

            print(f"Iniciando Descarga de Informacion de: {type_download}.")

            print('Desplegamos la lista del apartado: Club Whitin A Club Report.')
            driver.find_element(By.XPATH, '//select[@name="buyerReport_level"]').click()
            time.sleep(5)

            print(f'Seleccionamos la opcion de: {type_download}')
            driver.find_element(By.XPATH, f'//option[text()="{type_download}"]').click()
            time.sleep(5)

            print('Extrayendo columnas:')
            columnas = driver.find_elements(By.XPATH, '//td[@scope="col"]/b')


            if descarga == 'Devoluciones History':
                columnas = columnas[:len(columnas) // 2]

            list_columnas = []
            for columna in columnas:
                texto = driver.execute_script("return arguments[0].innerText;", columna)
                texto_limpio = texto.strip().replace("\n", " ")
                list_columnas.append(texto_limpio)

            print('Extrayendo columnas de datos:')
            datos = driver.find_elements(By.XPATH, f'//tr[@class="normal" and @colspan]/td[@nowrap]')

            list_columnas_datos = []
            cadena = []
            bandera = 0
            for dato in datos:
                texto = dato.text
                cadena.append(texto)
                bandera += 1

                if bandera == (len(list_columnas)):
                    list_columnas_datos.append(cadena)
                    cadena = []
                    bandera = 0

            print(f'Creando el archivo con la infromacion extraida de: {descarga}')
            df_sell_out = pd.DataFrame(list_columnas_datos, columns=list_columnas)
            nuevo_nombre = f'{in_variables}_{descarga}_{str(in_fechaini).replace('/', '')}_origen.csv'
            print(nuevo_nombre)
            file_path = os.path.join(dir_fuentes, nuevo_nombre)
            df_sell_out.to_csv(file_path, encoding='utf-8', index=False, header=True)

            time.sleep(5)
            esperar_descarga(dir_fuentes, num_archivos)

def transformar_archivo(dir_fuentes, dir_temp, in_fechaini, in_descarga, in_variables, dir_storage):

    try:

        archivos = [name for name in os.listdir(dir_fuentes)]

        for archivo in archivos:

            name_file = f'{in_variables}_{str(archivo).split('_')[1]}_{str(in_fechaini).replace('/', '')}_tranform.csv'
            file_path = os.path.join(dir_temp, name_file)
            print(archivo)

            df = pd.read_csv(f'{dir_fuentes}/{archivo}', dtype=str)
            df = df.fillna(0)

            columnas_sin_acentos = []
            for columna_df in df.columns:
                columna_df = columna_df.replace('%', 'Porc')
                texto_normalizado = unicodedata.normalize('NFD', columna_df)
                texto_sin_acentos = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')
                columnas_sin_acentos.append(texto_sin_acentos)
            df.columns = columnas_sin_acentos

            for columna in df.columns:
                df[columna] = df[columna].apply(
                    lambda x: str(x).replace('$', '').replace(',', '').replace('%', '').replace('            ',
                                                                                                ' ').strip())

                if (columna == 'Codigo de barras'):
                    df[f'CA {columna}'] = df[columna].apply(lambda x: int(str(x).split('-')[0].strip()))
                    df[f'CA Producto'] = df[columna].apply(
                        lambda x: str(x).split('-')[1].replace('            ', ' ').strip())

                if columna == 'Material':
                    df[f'CA Codigo {columna}'] = df[columna].apply(lambda x: int(str(x).split('-')[0].strip()))
                    df[f'CA Decripcion {columna}'] = df[columna].apply(
                        lambda x: str(x).replace('            ', ' ').split('-')[1].strip())

                if (columna == 'Tienda'):
                    df[f'CA Num {columna}'] = df[columna].apply(lambda x: int(str(x).split('-')[0].strip()))
                    df[f'CA Nombre {columna}'] = df[columna].apply(
                        lambda x: str(x).replace('            ', ' ').split('-')[1].strip())

            df['CA Fecha Consulta'] = datetime.today().strftime('%Y-%m-%d')
            df['CA Hora Consulta'] = datetime.today().strftime('%H:%M:%S')

            df.to_csv(file_path, encoding='utf-8', index=False, header=True)

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
    transformar_archivo(dir_fuentes, dir_temp, args.in_fechaini, args.in_descarga, args.in_variables, dir_storage)

    print('Tarea Finalizada Ok')



if __name__ == "__main__":
    main()

