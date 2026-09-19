"""
------------------------------------------------------------------------------------------------------------------------
https://mex-rmscm02084.realmetrics.io/retool/v1/workflows/d6486d75-07ca-46e4-9afe-4190bfbd6516/startTrigger?workflowApiKey=retool_wk_52d4a54100d345efab1c6705c99ad014
-in_url
https://mex-rmscm02084.realmetrics.io/retool/v1/workflows/d8a15d90-a9d3-4659-9dce-5a863aca01ff/startTrigger?workflowApiKey=retool_wk_b317635496974b4d888080d92ff4c1ab
-in_drt_id
4959

-in_url
https://pro.02052.realmetrics.tech/retool/v1/workflows/4d3fd86f-3c3e-4925-9485-76aef4e460fd/startTrigger?workflowApiKey=retool_wk_1bdd985533e543c891d30fbd737d3ff8
-in_drt_id
3583
------------------------------------------------------------------------------------------------------------------------
Notas:
"""

from datetime import date, timedelta, datetime
#from dateutil.relativedelta import relativedelta
from multiprocessing import Pool
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException, NoAlertPresentException
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.support.ui import Select
from zipfile import ZipFile
from email import message_from_bytes


import argparse
import calendar
import csv
import gzip
import itertools
#import numpy as np
import os
import re
#import pandas as pd
import sys
import shutil
import time
from enum import Enum
import json
#import openpyxl
#from openpyxl.styles import numbers
import subprocess
import requests
import base64
import codecs
import suscriber

#importar librerias para el metodo de code_email
from bs4 import BeautifulSoup
import imaplib
import msal #librería de python para implementación de Oauth2
import email

conf_client = {
    "authority": "https://microsoftonline.com",
    "client_id": "TU_CLIENT_ID_AQUI",
    "scope": ['https://office365.com'],
    "secret": "TU_SECRET_DE_AZURE_AQUI",
}

class FactException(Exception):
    '''Es una clase heredada de una clase construida dentro de Python llamada FactException.
    Usa dos parametros code y message
    super().__init__(self.message) llama al constructor de la clase base (Exception) y le pasa el mensaje.
    Esto se hace para asegurar que la clse está inicializado adecuadamente.'''
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(self.message)


class Config:
    '''Es una clase que usa distintos parametros de configuración.'''
    def __init__(self, host, username, password, token, type, language, drt_id, exec_num, chain_id, supplier_id, tasklist,
                 subtask_api,
                 bucket_gen, bucket_src, bucket_out, log_api, task_api, project, location,
                 dataset_dst, dataset_src, dataset_cat, robot_version, portal, user_num, phone_num):
        self.drt_id = drt_id
        self.chain_id = chain_id
        self.supplier_id = supplier_id
        self.exec_num = exec_num
        self.host = host
        self.username = username
        self.password = password
        self.token = token
        self.type = type  # nos dice si es SI o DL
        self.language = language
        self.tasklist = tasklist
        self.subtask_api = subtask_api
        self.log_api = log_api
        self.task_api = task_api
        self.bucket_gen = bucket_gen
        self.bucket_src = bucket_src
        self.bucket_out = bucket_out
        self.project = project
        self.location = location
        self.dataset_dst = dataset_dst
        self.dataset_src = dataset_src
        self.dataset_cat = dataset_cat
        self.robot_version = robot_version
        self.portal = portal
        self.user_num = user_num
        self.phone_num = phone_num
        #self.extra_params = extra_params
        # self.report = report


class Paths:
    def __init__(self):
        self.images = ''
        self.data = ''
        self.temp = ''
        self.backup = ''
        self.files_transformed = ''
        self.files_origin = ''
        self.base = ''


class ResultType(Enum):
    ''' Se definió una enumeración utilizando la clase Enum de pthyon.
    Se le llamó ResultType y contiene valores asignados.
    Se usaron para que sea más fácil de leer el código.'''
    OK = 0
    USER_ERROR = 999
    EXCEPTION = 910
    DOWNLOAD_ERROR = 977
    EMPTY_FILE = 901
    CAPTCHA_RESOLVE = 800
    DATA_NOT_AVAILABLE = 900
    COLUMNS_COUNT = 902


class Exec:
    '''Genera y ejecuta un comando en BQ usando los parametros que se le pasan'''
    @staticmethod
    def execute_bq_script(conf, paths, fact, days, date):

        dates = ','.join(map(lambda x: "'" + x + "'", days))
        datecode = datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')
        filename = f'bif_aut_day_mex{conf.chain_id}_{fact}_v00_{conf.supplier_id}_{datecode}.gz'

        # da_00007_00_123_15_walmarb_day_20231215
        cmd = f'bq query --project_id="{conf.project}" --use_legacy_sql=false --use_cache=false '
        # bq query --project_id="rmscm02084" --use_legacy_sql=false --use_cache=false '
        cmd += f' --parameter="p_project::{conf.project}" '
        #--parameter="p_project::rmscm02084"
        cmd += f' --parameter="p_dest_dataset::{conf.dataset_dst}" '
        # --parameter = "p_dest_dataset::prod_ds_rm_scm_02084"
        cmd += f' --parameter="p_sour_dataset::{conf.dataset_src}" '
        # --parameter = "p_sour_dataset::prod_ds_rm_scm_02084_src"
        cmd += f' --parameter="p_cat_dataset::{conf.dataset_cat}" '
        # --parameter="p_cat_dataset::prod_ds_rm_scm_02084_cat"
        cmd += f' --parameter="p_bucket::{conf.bucket_gen}" '
        # --parameter = "p_bucket::ds_rm_scm_02084"
        cmd += f' --parameter="p_filename::{filename}" '

        cmd += f' --parameter="p_chain_code::{conf.chain_id}" '
        cmd += f' --parameter="p_supl::{conf.supplier_id}" '
        cmd += f' --parameter="p_date::{dates}" '
        cmd += f' --flagfile="{paths.base}/{fact}_00.sql" '
        Trace.write(conf, 'I', f'cmd: {cmd}')
        result = Exec.execute_command(conf, cmd)
        # if (result):
        #    Trace.write(f'bq script result: {result}')
        return 0

    @staticmethod
    def upload_files(conf, paths):
        try:
            Trace.write(conf, 'I', "Subiendo archivos de origen...")
            cmd = f'gsutil -q cp {paths.backup}/*.zip gs://{conf.bucket_src}'
            Exec.execute_command(conf, cmd)
        except Exception as e:
            Trace.write(conf, 'E', f'SourceFiles. Excepcion: {e}')
        try:
            Trace.write(conf, 'I', "Subiendo archivos generados...")
            cmd = f'gsutil -q cp {paths.files_transformed}/*.gz gs://{conf.bucket_gen}'
            Exec.execute_command(conf, cmd)
        except Exception as e:
            Trace.write(conf, 'E', f'GenFiles. Excepcion: {e}')
        return ""

    ''' Este método toma dos parámetros, conf y cmd. 
        Utiliza la clase subprocess.Popen para ejecutar el comando especificado (cmd) en la terminal. 
        La salida estándar y el error estándar del comando se capturan utilizando communicate().

        Si el código de retorno del proceso (p.returncode) no es igual a 0, lo que indica un error, 
        se genera una excepción con el mensaje de error estándar (stderr) como argumento.'''
    @staticmethod
    def execute_command(conf, cmd):
        # cmd = cmd.split(' ')
        # Trace.write(conf, 'I', "Ejecutando comando....")
        # try:
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout, stderr = p.communicate()
        # Trace.write(conf, 'I', "Comando ejecutado!")
        # Trace.write(conf, 'I', f"stdout: { stdout }. stderr: {stderr}, return code: {p.returncode}")
        if p.returncode != 0:
            raise Exception(stderr)
        return 0
        # except Exception as e:
        #    Trace.write(f"Exception exc: {e}")
        #    return 1

    '''El método se encarga de subir archivos a Google Cloud Storage utilizando el comando gsutil y captura excepciones
     que puedan ocurrir durante el proceso.
     Además, se utilizan mensajes de registro a través de la clase Trace. '''
    @staticmethod
    def upload_files(conf, paths):
        try:
            Trace.write(conf, 'I', "Subiendo archivos de origen...")
            cmd = f'gsutil -q cp {paths.backup}/*.zip gs://{conf.bucket_src}'
            # print(cmd)
            Exec.execute_command(conf, cmd)
        except Exception as e:
            Trace.write(conf, 'E', f'SourceFiles. Excepcion: {e}')
        try:
            Trace.write(conf, 'I', "Subiendo archivos generados...")
            cmd = f'gsutil -q cp {paths.files_transformed}/*.gz gs://{conf.bucket_gen}'
            Exec.execute_command(conf, cmd)
        except Exception as e:
            Trace.write(conf, 'E', f'GenFiles. Excepcion: {e}')
        return ""

    '''El método se encarga de subir archivos a Google Cloud Storage utilizando el comando gsutil y captura excepciones
         que puedan ocurrir durante el proceso.
         Además, se utilizan mensajes de registro a través de la clase Trace. '''
    @staticmethod
    def upload_files_out(conf, paths):
        try:
            # Trace.write(conf, 'I', "Subiendo archivos de salida...")
            cmd = f'gsutil -q cp {paths.images}/* gs://{conf.bucket_out}'
            Exec.execute_command(conf, cmd)
        except Exception as e:
            Trace.write(conf, 'E', f'OutFiles. Excepcion: {e}')
        return ""


def fix_nulls(s):
    '''Remplaza los nulos por nada'''
    for line in s:
        yield line.replace('\0', '')


class Trace:
    '''Esta clase se usa para la traza.
        Se define la clase Trace con el método estático write.
        Se crea un diccionario con los parametros y después hace POST request. '''
    @staticmethod
    def write(conf, type, message, public_user='N'):
        parameters = {"drt_id": conf.drt_id, "type": type, "message": message, "public_user": public_user,
                      "execute_number": conf.exec_num}
        response = requests.post(conf.log_api, json=parameters)
        print(message)


class FileHelper:
    '''Función que hace una captura de pantalla y la guarda en la carpeta images'''
    @staticmethod
    def save_screen(driver, paths, conf):
        driver.save_screenshot(
            f'{paths.images}da_{conf.chain_id}_{conf.supplier_id}_{conf.drt_id}_{conf.exec_num}.png')

    '''Función que comprime los archivos en uno'''
    @staticmethod
    def zip_all_to_one_file(from_path, to_path, file_id):
        shutil.make_archive(f'{to_path}{file_id}', 'zip', from_path)

    '''Función que comprime los archivos de salida'''
    @staticmethod
    def gz_files_by_extension(from_path, to_path, ext):
        archivo = [name for name in os.listdir(from_path) if name.endswith(f".{ext}")]
        for arc in archivo:
            file = arc.split(f'.{ext}')[0]
            with open(from_path + file + f'.{ext}', 'rb') as f_in, gzip.open(f'{to_path}{file}.{ext}.gz',
                                                                             'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

            os.rename(f'{to_path}{file}.{ext}.gz', f'{to_path}{file}.gz')

    '''Función que limpia directorios'''
    @staticmethod
    def clear_paths(paths):
        for c in paths:
            f = [name for name in os.listdir(c)]
            for file in f:
                os.remove(c + file)

    '''Función que borra los archivos de salida si estan vacios'''
    @staticmethod
    def clear_path_empty_files(path, ext):
        archivo = [name for name in os.listdir(path) if name.endswith(f".{ext}")]
        for arc in archivo:
            file = arc.split(f'.{ext}')[0]
            if os.path.getsize(path + file + f'.{ext}') == 0:
                os.remove(path + file + f'.{ext}')
                raise Exception('Archivo de salida {} vacio.'.format(arc))


def wait_download(driver, conf, documents_number, paths, ext):
    '''Espera que se terminen de descargar el número de archivos indicados en documents_number'''
    start = datetime.now()
    # Esperando descarga
    while True:
        empty_files_count = 0
        time.sleep(10)
        Trace.write(conf, 'I', f'Buscando archivo descargado. -> {datetime.now()}')
        if (datetime.now() - start).seconds < 500:
            files = [f for f in os.listdir(paths.files_origin) if
                     f.endswith(f'.{ext}')]

            Trace.write(conf, 'I', f'Archivos descargados: {len(files)}')
            if len(files) == documents_number:
                for file in files:
                    if os.path.getsize(paths.files_origin + file) <= 50:
                        empty_files_count += 1
                if empty_files_count == 0:
                    Trace.write(conf, 'I', 'Archivos Descargados.')
                    break
        else:
            Trace.write(conf, 'I', 'No se descargo el archivo.')
            raise FactException(ResultType.DOWNLOAD_ERROR.value, 'Error de descarga.')


def enable_download_in_headless_chrome(driver, path):
    # add missing support for chrome "send_command"  to selenium webdriver
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': path}}
    command_result = driver.execute("send_command", params)


def set_driver(conf, paths):
    '''Inicializa el chromeDriver con todas las opciones que lo necesitamos.'''
    Trace.write(conf, 'I', 'Inicio Ejecución del proceso.')
    chrome_options = Options()
    if os.name != "nt":
        #chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--lang=en-US")
    #chrome_options.addArguments("--disable-notifications ")
    #chrome_options.addArguments("disable-gpu");
    chrome_options.add_argument(
        '--disable-dev-shm-usage')  # utilizado para evitar errores en el docker por falta de memoria
    chrome_options.add_argument("-allow-running-insecure-content")
    prefs = {"download.default_directory": paths.files_origin,
             "download.prompt_for_download": False,
             "download.directory_upgrade": True,
             "download.extensions_to_open": 'exe',
             "safebrowsing.enabled": True,
             "profile.default_content_settings.popups": 0,  # Desactiva la ventana emergente de permisos
             "profile.default_content_setting_values.notifications": 1,
             "profile.default_content_setting_values.geolocation": 2,
             "profile.default_content_setting_values.automatic_downloads": 1
             }

    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36")

    Trace.write(conf, 'I', 'Iniciando driver.')

    serv = Service(r'C:\\chromedriver.exe')
    if os.name == "nt":
        driver = webdriver.Chrome(service=serv, options=chrome_options)
    else:
        driver = webdriver.Chrome(options=chrome_options)

    try:
        Trace.write(conf, 'I', 'Version: 1.0. Driver Listo')
        time.sleep(10)

    except Exception as e:
        end_program(driver, conf, paths, ResultType.EXCEPTION, 'Finalizado con Error Desconocido.', e)
    return driver


##--------------CONFIGURACION PARA OBTENER EL CODIGO POR EMAIL-------------------------------

def generate_auth_string(user, token):
    """generate an Oauth2 standard token adn return it"""
    return f"user={user}\x01auth=Bearer {token}\x01\x01"


def generate_msal_token():
    """generate Oauth2 token using msal library and return it

    genera un token de autorización de microsoft según Oauth2 usando la librería
    msal:
    1)crea una instancia de la aplicacion python_imap registrada en Azure active directory
    1.5) busca un token en caché
    2)si no halla un token solicita uno nuevo
    3)retorna el token si todo sale bien, si no, False
    """
    # 1
    app = msal.ConfidentialClientApplication(conf_client['client_id'], authority=conf_client['authority'],
                                             client_credential=conf_client['secret'])
    # 1.5
    result = app.acquire_token_silent(conf_client['scope'], account=None)
    # 2
    if not result:
        print("No suitable token in cache.  Get new one.")
        result = app.acquire_token_for_client(scopes=conf_client['scope'])
    # 3
    if "access_token" in result:
        print("Token obtenido satisfactoriamente")
    else:
        print(f"""
                {result.get("error")}\n
                {result.get("error_description")}\n
                {result.get("correlation_id")}
                """)
        return False

    return result['access_token']


def obtencion_codigo(email):
    try:
        print('Obteniendo Codigo de acceso')
        mail = imaplib.IMAP4_SSL('outlook.office365.com')

        # generando token
        msal_access_token = generate_msal_token()
        assert msal_access_token, "Error, falló obtención del token de autenticación."
        # mail.starttls()
        mail.authenticate("XOAUTH2", lambda x: generate_auth_string(
            email,
            msal_access_token).encode("utf-8"))
        mail.select('Inbox')
        type, data = mail.search(None, '(Subject "Verify your identity")')
        ids = data[0].split()
        a=0

        for num in reversed(ids):
            #con "RFC822" obtienes todo el contenido del correo
            typ, data = mail.fetch(num, '(RFC822)')
            raw_email = data[0][1]
            #print(type(raw_email))
            # convierte el byte_id literal en una cadena eliminando b''
            email_message = message_from_bytes(raw_email)            #print(email_message)

            for part in email_message.walk():

                #Muestra el tipo de contenido que vamos a leer
                #print(part.get_content_type())
                if part.get_content_type() == 'text/plain':
                    contenido_plano = (part.get_payload())
                else:
                    pass

            print('Obteniendo Codigo verificacion.')

            # - Contenido plano
            code = str(contenido_plano).split('Your verification code is:')[1].split('NIQ Activate,')[0].replace('\r','').replace('\n','')
            #print(code)
            a+=1

            if a == 1:
                break

        return code

    except Exception as e:
        print(e)


##--------------TIPOS DE INICIO DE SESION-------------------------------

def iniciar_sesion_email(driver, conf, paths):
    '''Función de inicio de sesión en el portal'''

    url = conf.host
    user = conf.username
    passw = conf.password
    phone_number = conf.phone_num

    try:
        driver.get(url)
        time.sleep(5)
        Trace.write(conf, 'I', 'Introduciendo Usuario')
        driver.find_element(By.XPATH, '//input[@id="username"]').send_keys('mvaldesf@cuervo.com.mx')
        time.sleep(2)
        Trace.write(conf, 'I', 'Introduciendo Password')
        time.sleep(2)
        driver.find_element(By.XPATH, '//input[@id="password"]').send_keys('Mex34$VñMex34$Vñ')
        time.sleep(2)
        driver.find_element(By.XPATH, "//button[@type='submit' and @name='action' and @value='default']").click()
        time.sleep(3)

        driver.find_element(By.XPATH, "//button[contains(text(),'ry another method')]").click()
        time.sleep(2)
        driver.find_element(By.XPATH, "//span[contains(text(),'Email')]").click()
        time.sleep(30)

        #email = conf.extra_params['email_code']['emai_params']
        codigo = obtencion_codigo('cstc2104_01@realmetrics.io')

        driver.find_element(By.XPATH, '//input[@id="code"]').send_keys(codigo.strip())
        time.sleep(2)
        driver.find_element(By.XPATH, '//button[text()="Continue"]').click()
        time.sleep(2)
        try:
            driver.find_element(By.XPATH, '//span[@id="error-element-code"]')
            raise Exception(Trace.write('E', f'El codigo no es valido.'))
        except:
           pass

        try:
            driver.find_element(By.XPATH, '//button[text()="Remind me later"]').click()
        except:
            pass
    except Exception as e:
        FileHelper.save_screen(driver, paths, conf)
        Trace.write(conf, 'E', f'Finalizado Error de Acceso zona segura. Excepcion: {e}')
        end_program(driver, conf, paths, ResultType.USER_ERROR, 'Finalizado con Error Desconocido.', e)

    Trace.write(conf, 'I', f'Acceso correcto.')
    time.sleep(10)
    return driver


def iniciar_sesion_phone(driver, conf, paths):
    '''Función de inicio de sesión en el portal'''

    url = conf.host
    user = conf.username
    passw = conf.password
    phone_number = conf.phone_num


    v_project = conf.project
    v_portal = conf.portal + conf.user_num
    v_user = conf.user_num
    v_trace = 1
    sms_json = suscriber.get_Code(v_project, v_portal, v_user, v_trace)

    # --Inicio de sesión normal

    try:
        driver.get(url)
        time.sleep(5)
        Trace.write(conf, 'I', 'Introduciendo Usuario')
        driver.find_element(By.XPATH, '//input[@id="username"]').send_keys(user)
        time.sleep(2)
        Trace.write(conf, 'I', 'Introduciendo Password')
        time.sleep(2)
        driver.find_element(By.XPATH, '//input[@id="password"]').send_keys(passw)
        time.sleep(2)
        driver.find_element(By.XPATH, "//button[@type='submit' and @name='action' and @value='default']").click()
        time.sleep(3)

        try:
            driver.find_element(By.XPATH, '//span[@id="error-element-password"]')
            raise Exception(Trace.write('E', f'Finalizado Error de Acceso zona segura.'))
        except:
            pass

        try:
            driver.find_element(By.XPATH, '//span[text()="XXXXXXXXX9258"]')
            time.sleep(2)
        except:
            driver.find_element(By.XPATH, "//button[contains(text(),'ry another method')]").click()
            time.sleep(2)
            driver.find_element(By.XPATH, "//span[contains(text(),'SMS')]").click()

        time.sleep(15)

        v_project = conf.project
        v_portal = conf.portal + conf.user_num
        v_user = conf.user_num
        v_trace = 1

        sms_json = suscriber.get_Code(v_project, v_portal, v_user, v_trace)
        if (sms_json):
            print(sms_json)
            codigo = sms_json.data.code.replace('.', '')
            print(f'Codigo: {codigo}')
        else:
            print('E', f'Error')


        driver.find_element(By.XPATH, '//input[@id="code"]').send_keys(codigo)
        time.sleep(2)
        driver.find_element(By.XPATH, '//button[text()="Continue"]').click()
        time.sleep(2)
        try:
            driver.find_element(By.XPATH, '//span[@id="error-element-code"]')
            raise Exception(Trace.write('E', f'El codigo no es valido.'))
        except:
           pass

        try:
            driver.find_element(By.XPATH, '//button[text()="Remind me later"]').click()
        except:
            pass
    except Exception as e:
        FileHelper.save_screen(driver, paths, conf)
        Trace.write(conf, 'E', f'Finalizado Error de Acceso zona segura. Excepcion: {e}')
        end_program(driver, conf, paths, ResultType.USER_ERROR, 'Finalizado con Error Desconocido.', e)

    Trace.write(conf, 'I', f'Acceso correcto.')
    time.sleep(10)
    return driver


def res_captcha(driver):
    '''Función que resuelve el captcha.'''
    driver.find_element(By.XPATH, '//div[@class="g-recaptcha"]')
    driver.switch_to.frame(driver.find_element(By.TAG_NAME, 'iframe'))
    time.sleep(3)
    ActionChains(driver).click(driver.find_element(By.TAG_NAME, 'body')).key_down(Keys.ENTER).perform()
    driver.switch_to.default_content()
    time.sleep(3)

    try:
        driver.switch_to.frame(driver.find_elements(By.TAG_NAME, "iframe")[2])
        ActionChains(driver).click(
            driver.find_elements(By.XPATH, "//div[@class='button-holder help-button-holder']")[0]).key_down(
            Keys.ENTER).perform()
        # driver.find_elements_by_xpath("//div[@class='button-holder help-button-holder']")[0].click()
    except Exception as e:
        Trace.write(e)
    return True

##--------------VERSIONES DEL CALENDARIO-------------------------------
def calendar_months(mes, version):

    month = str(mes).zfill(2)
    months_v0 = {
    "01": "Enero",
    "02": "Febrero",
    "03": "Marzo",
    "04": "Abril",
    "05": "Mayo",
    "06": "Junio",
    "07": "Julio",
    "08": "Agosto",
    "09": "Septiembre",
    "10": "Octubre",
    "11": "Noviembre",
    "12": "Diciembre"
    }

    months_v1 = {
    "01": "Ene",
    "02": "Feb",
    "03": "Mar",
    "04": "Abr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Ago",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dic"
    }

    months_v3 = {
      "01": "Jan",
      "02": "Feb",
      "03": "Mar",
      "04": "Apr",
      "05": "May",
      "06": "Jun",
      "07": "Jul",
      "08": "Aug",
      "09": "Sep",
      "10": "Oct",
      "11": "Nov",
      "12": "Dec"
    }

    months_vs = {
      "01": "Enero",
      "02": "Febrero",
      "03": "Marzo",
      "04": "Abril",
      "05": "Mayo",
      "06": "Junio",
      "07": "Julio",
      "08": "Agost",
      "09": "Sept",
      "10": "Oct",
      "11": "Nov",
      "12": "Dic"
    }

    if version == 'calendar_v0':
        months = months_v0
    elif version == 'calendar_v1':
        months = months_v1
    elif version == 'calendar_v3':
        months = months_v3
    elif version == 'calendar_vs':
        months = months_vs
    else:
        return "Versión no válida"

    month_day = months.get(month, "Mes no válido")

    return month_day


# Función que navega en el portal y descarga los archivos para trabajar
def descarga_dl(driver, conf, date, paths, fact, dias):

    driver.implicitly_wait(20)
    wait = WebDriverWait(driver, 120)
    actions = ActionChains(driver)
    fecha = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=dias)).strftime('%Y-%m-%d')
    flag = False

    try:
        #Prueba de metricas y de descarga
        if fact == 'sams00v':

            Trace.write(conf, 'I', 'Proceso de Descarga Ventas.')
            Trace.write(conf, 'I', 'Seleccionamos: Data Exporter')
            driver.find_element(By.XPATH, "//span[text()='Data Exporter']").click()
            time.sleep(2)
            Trace.write(conf, 'I', 'Seleccionamos: Limpiar')
            driver.find_element(By.XPATH, '//button[contains(text(),"Limpiar")]').click()
            time.sleep(2)
            driver.find_element(By.XPATH, '//button[contains(text(),"Limpiar")]').click()
            time.sleep(2)
            Trace.write(conf, 'I', 'Seleccionamos: Supply Data Type')
            WebDriverWait(driver, 120).until(EC.visibility_of_element_located(
                (By.XPATH, '//div[@data-testid="select-data_exporter_product_dropdown"]'))).click()
            time.sleep(4)
            Trace.write(conf, 'I', 'Seleccionamos: Supply Chain')
            # WebDriverWait(driver, 120).until(EC.visibility_of_elements_located((By.XPATH, '//span[text()="Supply Chain"]')))
            elements = driver.find_elements(By.XPATH, '//span[text()="Supply Chain"]')
            elements[1].click()

            # Trace.write(conf, 'I', 'Verificamos Supply Data Type')
            time.sleep(2)

            supply = driver.find_element(By.XPATH,
                                         '//div[@data-testid="select-data_exporter_product_dropdown"]/div').text

            if supply == 'Supply Chain':
                Trace.write(conf, 'I', 'Seleccion correcta de Supply Chain ')
            else:
                raise FactException(ResultType.Exception.value, 'Error en el Supply Chain')

            Trace.write(conf, 'I', 'Seleccionamos: Multi Periodo')
            driver.find_element(By.XPATH, "//button[@aria-label='Multi Periodo']").click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Perido')
            driver.find_element(By.XPATH, "//button[@id='basic-button']").click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Current')
            driver.find_element(By.XPATH, '//div[@data-testid="current-period-selection"]').click()

            Trace.write(conf, 'I', 'Seleccionamos: Periodo Personalizado')
            driver.find_element(By.XPATH, '//li[text()="Periodo Personalizado"]').click()
            time.sleep(2)

            Trace.write(conf, 'I', f'La fecha a buscar es: {fecha}')
            date_to_search = fecha.split('-')
            dia = date_to_search[2]
            mes = date_to_search[1]
            anio = date_to_search[0]
            mes_search_int = int(mes) - 1
            dia_search_int = int(dia) - 0  # SE LE RESTAN LOS DIAS, DEPENDIENDO EL HECHO (Ventas o Barrido)

            # Trace.write(conf, 'I', 'Eligiendo la version del calendario')
            mes_calendar = calendar_months(mes, 'calendar_v0')
            mes_string = (mes_calendar).lower()

            meses = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
                     "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
            WebDriverWait(driver, 60).until(
                EC.visibility_of_all_elements_located((By.XPATH, '//div[@class="sc-dwnOUR loOxNp MuiFormControl-root sc-knEsKG PXrWX MuiTextField-root"]/div/div'))
            )
            selectores_date = driver.find_elements(By.XPATH,
                                                   '//div[@class="sc-dwnOUR loOxNp MuiFormControl-root sc-knEsKG PXrWX MuiTextField-root"]/div/div')

            Trace.write(conf, 'I', 'Seleccionando Fecha Final')

            selectores_date[1].click()
            time.sleep(3)

            Trace.write(conf, 'I', f'Seleccionamos Anio:{anio}')
            driver.find_element(By.XPATH, '//div[@role="presentation" and @aria-live="polite"]/div').click()
            time.sleep(3)
            driver.find_element(By.XPATH, f'//div/div/button[text()="{anio}"]').click()
            time.sleep(3)

            mes_año_portal = driver.find_element(By.XPATH,
                                                 '//div[@role="presentation" and @aria-live="polite"]/div').text
            mes_portal = str(mes_año_portal).split(' ')[0]
            año_portal = str(mes_año_portal).split(' ')[1]

            Trace.write(conf, 'I', f'Seleccionamos Mes:{meses[mes_search_int]}')

            # Trace.write(conf, 'I', mes_portal)
            # Trace.write(conf, 'I', año_portal)

            mes_portal_int = None
            for i in range(0, len(meses), 1):
                if meses[i] == mes_portal:
                    mes_portal_int = i
                    break
            mes_año_portal = driver.find_element(By.XPATH,
                                                 '//div[@role="presentation" and @aria-live="polite"]/div').text

            # Trace.write(conf, 'I', mes_año_portal)
            # Trace.write(conf, 'I', f'MES PORTAL: {mes_portal}')
            # Trace.write(conf, 'I', f'MES SEARCH: {mes_search_int}')

            if mes_search_int > mes_portal_int:
                Trace.write(conf, 'I', 'Incrementa hasta el mes correspondiente')
                while mes_search_int >= mes_portal_int:
                    mes_search_int += 1
                    mes_año_portal = driver.find_element(By.XPATH,
                                                         f'//div[@role="presentation" and @aria-live="polite"]/div').text

                    if mes_año_portal == mes_string + ' ' + anio:
                        Trace.write(conf, 'I', f'selecciona: {mes_string} {anio}')
                        break
                    driver.find_element(By.XPATH, '//button[@aria-label="Next month"]').click()

            if mes_search_int < mes_portal_int:
                Trace.write(conf, 'I', 'Decrementa hasta el mes correspondiente')
                while mes_portal_int < mes_portal_int:
                    mes_search_int -= 1
                    mes_año_portal = driver.find_element(By.XPATH,
                                                         f'//div[@role="presentation" and @aria-live="polite"]/div').text

                    if mes_año_portal == mes_string + ' ' + anio:
                        Trace.write(conf, 'I', f'Seleccion correcta de {mes_string} {anio}')
                        break
                    driver.find_element(By.XPATH, '//button[@aria-label="Previous month"]').click()

            if mes_search_int == mes_portal_int:
                # Trace.write(conf, 'I', 'Es igual')
                mes_año_portal = driver.find_element(By.XPATH,
                                                     '//div[@role="presentation" and @aria-live="polite"]/div').text

                if mes_año_portal == mes_string + ' ' + anio:
                    Trace.write(conf, 'I', f'Seleccion correcta de {mes_string} {anio}')

            time.sleep(3)
            Trace.write(conf, 'I', f'Seleccionamos Dia: {dia_search_int}')
            driver.find_element(By.XPATH, f'//button[text()="{dia_search_int}"]').click()
            time.sleep(2)

            # ---------------------------------------Introduciendo Fechas Inicio---------------------------------------------
            Trace.write(conf, 'I', 'Seleccionando Fecha Fin')
            selectores_date[0].click()
            time.sleep(3)

            Trace.write(conf, 'I', f'Seleccionamos Anio: {anio}')
            driver.find_element(By.XPATH, '//div[@role="presentation" and @aria-live="polite"]/div').click()
            time.sleep(3)
            driver.find_element(By.XPATH, f'//div/div/button[text()={anio}]').click()
            time.sleep(3)

            mes_año_portal = driver.find_element(By.XPATH,
                                                 '//div[@role="presentation" and @aria-live="polite"]/div').text

            mes_portal = str(mes_año_portal).split(' ')[0]
            año_portal = str(mes_año_portal).split(' ')[1]
            Trace.write(conf, 'I', f'Seleccionamos Mes:{meses[mes_search_int]}')
            # Trace.write(conf, 'I', mes_portal)
            # Trace.write(conf, 'I', año_portal)

            for i in range(0, len(meses), 1):
                if meses[i] == mes_portal:
                    break

            mes_año_portal = driver.find_element(By.XPATH,
                                                 '//div[@role="presentation" and @aria-live="polite"]/div').text
            # Trace.write(conf, 'I', mes_año_portal)
            # Trace.write(conf, 'I', f'MES PORTAL: {mes_portal_int}')
            # Trace.write(conf, 'I', f'MES SEARCH: {mes_search_int}')

            if mes_search_int > mes_portal_int:
                Trace.write(conf, 'I', 'Incrementa hasta el mes correspondiente')
                while mes_search_int >= mes_portal_int:
                    mes_search_int += 1
                    mes_año_portal = driver.find_element(By.XPATH,
                                                         f'//div[@role="presentation" and @aria-live="polite"]/div').text

                    if mes_año_portal == mes_string + ' ' + anio:
                        Trace.write(conf, 'I', f'selecciona: {mes_string} {anio}')
                        break
                    driver.find_element(By.XPATH, '//button[@aria-label="Next month"]').click()

            if mes_search_int < mes_portal_int:
                Trace.write(conf, 'I', 'Decrementa hasta el mes correspondiente')
                while mes_portal_int < mes_portal_int:
                    mes_search_int -= 1
                    mes_año_portal = driver.find_element(By.XPATH,
                                                         f'//div[@role="presentation" and @aria-live="polite"]/div').text

                    if mes_año_portal == mes_string + ' ' + anio:
                        Trace.write(conf, 'I', f'selecciona: {mes_string} {anio}')
                        break
                    driver.find_element(By.XPATH, '//button[@aria-label="Previous month"]').click()

            if mes_search_int == mes_portal_int:
                # Trace.write(conf, 'I', 'Es igual')
                mes_año_portal = driver.find_element(By.XPATH,
                                                     '//div[@role="presentation" and @aria-live="polite"]/div').text

                if mes_año_portal == mes_string + ' ' + anio:
                    Trace.write(conf, 'I', f'Seleccion correcta de {mes_string} {anio}')

            time.sleep(3)
            Trace.write(conf, 'I', f'Seleccionamos Dia: {dia_search_int}')
            driver.find_element(By.XPATH, f'//button[text()="{dia_search_int}"]').click()
            time.sleep(3)

            Trace.write(conf, 'I', 'Seleccionamos: Aplicar')
            driver.find_element(By.XPATH, f'//button[contains(text(),"plicar")]').click()
            time.sleep(3)

            Trace.write(conf, 'I', 'Seleccionamos: Desplegar por: DAY')
            desplejar_por = driver.find_element(By.XPATH, f'//input[@value="WEEKLY"]')
            value = desplejar_por.get_attribute('value')

            # Trace.write(conf, 'I', value)
            if value != 'DAILY':
                driver.find_element(By.XPATH, f'//input[@value="WEEKLY"]/parent::div').click()
                time.sleep(2)
                driver.find_element(By.XPATH, f'//li[@data-value="DAILY"]').click()
                time.sleep(2)

            # --------------------------------Seleccionando Productos ----------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Seleccion de Productos')

            status_prod = driver.find_element(By.XPATH,
                                              f'//p[contains(text(),"Selección de Productos")]/parent::div/parent::div/parent::div')
            actions0 = ActionChains(driver)
            actions0.move_to_element(status_prod).perform()
            if status_prod.get_attribute('aria-expanded') == 'false':
                status_prod.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Numero de Item')
            status_items = driver.find_element(By.XPATH, f'//div[@data-testid="sku"]/div')
            status_items.click()
            time.sleep(3)
            try:
                driver.find_element(By.XPATH, f'//span[text()="Unselect All"]/preceding-sibling::span').click()
                time.sleep(2)
                status_items.click()
                driver.find_element(By.XPATH, f'//span[text()="Select All"]/preceding-sibling::span').click()
            except:
                try:
                    status_items.click()
                    driver.find_element(By.XPATH, f'//span[text()="Select All"]/preceding-sibling::span').click()
                    time.sleep(2)
                except:
                    raise Exception('Cambio la seleccion de Items')

            # actions = ActionChains(driver)
            # actions.send_keys(Keys.ESCAPE).perform()
            time.sleep(2)

            # --------------------------------Seleccionando Productos ----------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Seleccion de Club')
            status_prod = driver.find_element(By.XPATH,
                                              f'//p[contains(text(),"Selección de Club")]/parent::div/parent::div/parent::div')
            actions0 = ActionChains(driver)
            actions0.move_to_element(status_prod).perform()
            if status_prod.get_attribute('aria-expanded') == 'false':
                status_prod.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Club')
            status_club = driver.find_element(By.XPATH, f'//div[@data-testid="store"]/div/div/input')
            status_club.click()
            time.sleep(3)
            try:
                driver.find_element(By.XPATH, f'//span[text()="Unselect All"]/preceding-sibling::span').click()
                status_club.click()
                time.sleep(2)
                driver.find_element(By.XPATH, f'//span[text()="Select All"]/preceding-sibling::span').click()
            except:
                try:
                    status_club.click()
                    driver.find_element(By.XPATH, f'//span[text()="Select All"]/preceding-sibling::span').click()
                    time.sleep(2)
                except:
                    raise Exception('Cambio la seleccion de Club')

            # --------------------------------Seleccionando Dimensiones ----------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Dimenciones y Metricas')
            status_filters = driver.find_element(By.XPATH,
                                                f'//div[contains(text(),"Dimensiones y Métricas")]/parent::div')
            if status_filters.get_attribute('aria-expanded') == 'false':
                status_filters.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Agrupar Dimensiones')
            metrica_dime = driver.find_element(By.XPATH, f'//label[text() = "Dimensión"]/parent::div')
            actions2 = ActionChains(driver)
            actions2.move_to_element(metrica_dime).perform()
            time.sleep(2)

            Trace.write(conf, 'I', 'Introduccionedo Dimensiones a buscar')
            dimensiones = ['Tribe', 'Club', 'Item']  # Axel 2108
            ##El arreglo  contiene las maetricas a a selecicionar del apartado de dimensiones.
            # dimensiones = ['Tribe', 'Club', 'UPC', 'Item']#Frank 2037
            # print(dimensiones)

            for i in dimensiones:
                metrica_dime.click()
                time.sleep(2)
                input_dime = driver.find_element(By.XPATH, f'//input[@placeholder="Buscar"]')
                time.sleep(2)
                # Trace.write(conf, 'I', f'Buscando {i}')
                input_dime.clear()
                time.sleep(2)
                input_dime.send_keys(i)

                time.sleep(2)

                if i == 'Club':
                    Trace.write(conf, 'I', f'Seleccionamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                elif i == 'Item':
                    Trace.write(conf, 'I', f'Seleccionamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                elif i == 'Tribe':
                    # Trace.write(conf, 'I', f'Eliminamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                elif i == 'UPC':
                    Trace.write(conf, 'I', f'Seleccionamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                elif i == 'Fabricante':
                    Trace.write(conf, 'I', f'Seleccionamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                time.sleep(2)
                actions = ActionChains(driver)
                actions.send_keys(Keys.ESCAPE).perform()
                time.sleep(2)

            time.sleep(2)
            Trace.write(conf, 'I', 'Verificamos dimensiones seleccionadas')
            dimensiones_value = driver.find_element(By.XPATH, f'//label[text() = "Dimensión"]/parent::div/div/input')
            # Trace.write(conf, 'I', dimensiones_value.get_attribute('value'))
            con_palabras = ','
            for i, dimension in enumerate(dimensiones):
                palabra = dimension
                if i != 0:
                    con_palabras = f'{con_palabras}, {palabra}'
                metrica_sin_tribe = con_palabras.replace(",, ", '')

            if dimensiones_value.get_attribute('value') == metrica_sin_tribe:
                Trace.write(conf, 'I', 'Las dimensiones esta correctamente seleccionadas')
            else:
                raise FactException(ResultType.EXCEPTION.value, 'Error en Dimesiones')

            time.sleep(2)

            # --------------------------------Seleccionando Metricas ----------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Seleccion Metricas')
            status_metricas = driver.find_element(By.XPATH,
                                                  f'//p[contains(text(),"Selección de Métricas")]/parent::div/parent::div/parent::div')
            actions0 = ActionChains(driver)
            actions0.move_to_element(status_prod).perform()
            if status_metricas.get_attribute('aria-expanded') == 'false':
                status_metricas.click()
            time.sleep(2)

            metrica_metr = driver.find_element(By.XPATH, f'//label[text() = "Métricas"]/parent::div')
            action3 = ActionChains(driver)
            action3.move_to_element(metrica_metr).perform()
            metrica_metr.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Ingresando Metricas a buscar')
            metricas = ['Sales (Mex$)', 'Unit Sales', 'Store OH EoP Quantity', 'Store OH EoP Sell Mex$']  # AXEL
            ##El arreglo  contiene las maetricas a a selecicionar del apartado de dimensiones.
            # metricas = ['Sales (Mex$)', 'Unit Sales']  # Frank
            # print(metricas)

            input_metr = driver.find_element(By.XPATH, f'//input[@placeholder="Buscar"]')

            for i in metricas:
                input_metr.clear()
                time.sleep(2)
                Trace.write(conf, 'I', f'Seleccionamos: {i}')
                input_metr.send_keys(i)
                time.sleep(2)

                metrica = driver.find_element(By.XPATH, f'//span[text() = "{i}"]/parent::div/parent::li')

                if metrica.get_attribute('aria-selected') == 'false':
                    metrica.click()
                time.sleep(2)

            actions.send_keys(Keys.ESCAPE).perform()
            time.sleep(2)

            # ----------------------------Proceso de Descarga ----------------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Ejecutar')
            driver.find_element(By.XPATH, f'//p[contains(text(),"Ejecutar informe")]').click()
            time.sleep(2)
            time.sleep(10)

            Trace.write(conf, 'I', 'Seleccionamos: Exportamos')
            driver.find_element(By.XPATH, f'//span[@aria-label="Exportar"]').click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Expor a CSV')
            driver.find_element(By.XPATH, f'//li[@aria-label="Exportar a CSV"]').click()
            time.sleep(2)

            name_archivo = driver.find_element(By.XPATH,
                                               '//input[@class="sc-dmctIk sc-hAQmFe ifqOwr dkjbCA MuiInputBase-input MuiOutlinedInput-input MuiInputBase-inputSizeSmall"]').get_attribute(
                "value")
            Trace.write(conf, 'I', f'Generando archivo: {name_archivo}')
            time.sleep(2)

            try:
                driver.find_element(By.XPATH, '//*[contains(text(), "Notifíqueme por Email")]').click()
            except:
                pass

            Trace.write(conf, 'I', 'Aceptamos Alerta a exportar')
            driver.find_element(By.XPATH, f'//button[text()="OK"]').click()
            time.sleep(5)

            # VA ENTRAR AL RELOJ A SBUSCAR EL ARCHIVO CON EL NOMBRE QUE SE GENERO Y EXTRAIMOS ARRIBA
            try:
                WebDriverWait(driver, 10).until(EC.visibility_of_element_located((
                    By.XPATH, '//*[contains(text(), "Su solicitud de descarga")]')))

                driver.find_element(By.XPATH, '//button[contains(text(), "OK")]').click()
                time.sleep(2)

                Trace.write(conf, 'I', 'Exportar Actividades.')
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(1)
                driver.switch_to.default_content()
                time.sleep(2)
                driver.find_element(By.XPATH,
                                    '//button[@data-testid="data_exporter_status_panel_export_activities_button"]').click()

                # try:
                Trace.write(conf, 'I', 'Descargando archivo.')
                WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//*[contains(text(), "{name_archivo}")]')))

                # enable_download_in_headless_chrome(driver, paths.files_origin)
                driver.find_element(By.XPATH,
                                    f'//*[contains(text(), "{name_archivo}")]//parent::div//parent::div/div[@data-testid="data-export-download"]').click()


            except:
                Trace.write(conf, 'I', 'La descarga fue directa.')


            actions = ActionChains(driver)
            actions.send_keys(Keys.ESCAPE).perform()
            time.sleep(2)

            wait_download(driver, conf, 2, paths, 'csv')

            flag = True

        if fact == 'sams00b':

            Trace.write(conf, 'I', 'Proceso de Descarga Barrido.')
            Trace.write(conf, 'I', 'Seleccionamos: Data Exporter')
            driver.find_element(By.XPATH, "//span[text()='Data Exporter']").click()
            time.sleep(2)
            Trace.write(conf, 'I', 'Seleccionamos: Limpiar')
            driver.find_element(By.XPATH, '//button[contains(text(),"Limpiar")]').click()
            time.sleep(2)
            driver.find_element(By.XPATH, '//button[contains(text(),"Limpiar")]').click()
            time.sleep(2)
            Trace.write(conf, 'I', 'Seleccionamos: Supply Data Type')
            WebDriverWait(driver, 120).until(EC.visibility_of_element_located(
                (By.XPATH, '//div[@data-testid="select-data_exporter_product_dropdown"]'))).click()
            time.sleep(4)
            Trace.write(conf, 'I', 'Seleccionamos: Supply Chain')
            # WebDriverWait(driver, 120).until(EC.visibility_of_elements_located((By.XPATH, '//span[text()="Supply Chain"]')))
            elements = driver.find_elements(By.XPATH, '//span[text()="Supply Chain"]')
            elements[1].click()

            #Trace.write(conf, 'I', 'Verificamos Supply Data Type')
            time.sleep(2)

            supply = driver.find_element(By.XPATH, '//div[@data-testid="select-data_exporter_product_dropdown"]/div').text

            if supply == 'Supply Chain':
                Trace.write(conf, 'I', 'Seleccion correcta de Supply Chain ')
            else:
                raise FactException(ResultType.Exception.value, 'Error en el Supply Chain')

            Trace.write(conf, 'I', 'Seleccionamos: Multi Periodo')
            driver.find_element(By.XPATH, "//button[@aria-label='Multi Periodo']").click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Perido')
            driver.find_element(By.XPATH, "//button[@id='basic-button']").click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Current')
            driver.find_element(By.XPATH, '//div[@data-testid="current-period-selection"]').click()

            Trace.write(conf, 'I', 'Seleccionamos: Periodo Personalizado')
            driver.find_element(By.XPATH, '//li[text()="Periodo Personalizado"]').click()
            time.sleep(2)

            Trace.write(conf, 'I', f'La fecha a buscar es: {fecha}')
            date_to_search = fecha.split('-')
            dia = date_to_search[2]
            mes = date_to_search[1]
            anio = date_to_search[0]
            mes_search_int = int(mes) - 1
            dia_search_int = int(dia) - 0  # SE LE RESTAN LOS DIAS, DEPENDIENDO EL HECHO (Ventas o Barrido)

            #Trace.write(conf, 'I', 'Eligiendo la version del calendario')
            mes_calendar = calendar_months(mes, 'calendar_v0')
            mes_string = (mes_calendar).lower()

            meses = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
                     "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

            selectores_date = driver.find_elements(By.XPATH,
                                                   '//div[@class="sc-dwnOUR loOxNp MuiFormControl-root sc-knEsKG PXrWX MuiTextField-root"]/div/div')

            Trace.write(conf, 'I', 'Seleccionando Fecha Final')
            selectores_date[1].click()
            time.sleep(3)

            Trace.write(conf, 'I', f'Seleccionamos Anio:{anio}')
            driver.find_element(By.XPATH, '//div[@role="presentation" and @aria-live="polite"]/div').click()
            time.sleep(3)
            driver.find_element(By.XPATH, f'//div/div/button[text()="{anio}"]').click()
            time.sleep(3)

            mes_año_portal = driver.find_element(By.XPATH, '//div[@role="presentation" and @aria-live="polite"]/div').text
            mes_portal = str(mes_año_portal).split(' ')[0]
            año_portal = str(mes_año_portal).split(' ')[1]

            Trace.write(conf, 'I', f'Seleccionamos Mes:{meses[mes_search_int]}')

            #Trace.write(conf, 'I', mes_portal)
            #Trace.write(conf, 'I', año_portal)

            mes_portal_int = None
            for i in range(0, len(meses), 1):
                if meses[i] == mes_portal:
                    mes_portal_int = i
                    break
            mes_año_portal = driver.find_element(By.XPATH,
                                                 '//div[@role="presentation" and @aria-live="polite"]/div').text

            #Trace.write(conf, 'I', mes_año_portal)
            #Trace.write(conf, 'I', f'MES PORTAL: {mes_portal}')
            #Trace.write(conf, 'I', f'MES SEARCH: {mes_search_int}')

            if mes_search_int > mes_portal_int:
                Trace.write(conf, 'I', 'Incrementa hasta el mes correspondiente')
                while mes_search_int >= mes_portal_int:
                    mes_search_int += 1
                    mes_año_portal = driver.find_element(By.XPATH,
                                                         f'//div[@role="presentation" and @aria-live="polite"]/div').text

                    if mes_año_portal == mes_string + ' ' + anio:
                        Trace.write(conf, 'I', f'selecciona: {mes_string} {anio}')
                        break
                    driver.find_element(By.XPATH, '//button[@aria-label="Next month"]').click()

            if mes_search_int < mes_portal_int:
                Trace.write(conf, 'I', 'Decrementa hasta el mes correspondiente')
                while mes_portal_int < mes_portal_int:
                    mes_search_int -= 1
                    mes_año_portal = driver.find_element(By.XPATH,
                                                         f'//div[@role="presentation" and @aria-live="polite"]/div').text

                    if mes_año_portal == mes_string + ' ' + anio:
                        Trace.write(conf, 'I', f'Seleccion correcta de {mes_string} {anio}')
                        break
                    driver.find_element(By.XPATH, '//button[@aria-label="Previous month"]').click()

            if mes_search_int == mes_search_int:
                #Trace.write(conf, 'I', 'Es igual')
                mes_año_portal = driver.find_element(By.XPATH,
                                                     '//div[@role="presentation" and @aria-live="polite"]/div').text

                if mes_año_portal == mes_string + ' ' + anio:
                    Trace.write(conf, 'I', f'Seleccion correcta de {mes_string} {anio}')

            time.sleep(3)
            Trace.write(conf, 'I', f'Seleccionamos Dia: {dia_search_int}')
            driver.find_element(By.XPATH, f'//button[text()="{dia_search_int}"]').click()
            time.sleep(2)

            # ---------------------------------------Introduciendo Fechas Inicio---------------------------------------------
            Trace.write(conf, 'I', 'Seleccionando Fecha Fin')
            selectores_date[0].click()
            time.sleep(3)

            Trace.write(conf, 'I', f'Seleccionamos Anio: {anio}')
            driver.find_element(By.XPATH, '//div[@role="presentation" and @aria-live="polite"]/div').click()
            time.sleep(3)
            driver.find_element(By.XPATH, f'//div/div/button[text()={anio}]').click()
            time.sleep(3)

            mes_año_portal = driver.find_element(By.XPATH,
                                                 '//div[@role="presentation" and @aria-live="polite"]/div').text

            mes_portal = str(mes_año_portal).split(' ')[0]
            año_portal = str(mes_año_portal).split(' ')[1]
            Trace.write(conf, 'I', f'Seleccionamos Mes:{meses[mes_search_int]}')
            #Trace.write(conf, 'I', mes_portal)
            #Trace.write(conf, 'I', año_portal)

            for i in range(0, len(meses), 1):
                if meses[i] == mes_portal:
                    break

            mes_año_portal = driver.find_element(By.XPATH,
                                                 '//div[@role="presentation" and @aria-live="polite"]/div').text
            #Trace.write(conf, 'I', mes_año_portal)
            #Trace.write(conf, 'I', f'MES PORTAL: {mes_portal_int}')
            #Trace.write(conf, 'I', f'MES SEARCH: {mes_search_int}')

            if mes_search_int > mes_portal_int:
                Trace.write(conf, 'I', 'Incrementa hasta el mes correspondiente')
                while mes_search_int >= mes_portal_int:
                    mes_search_int += 1
                    mes_año_portal = driver.find_element(By.XPATH,
                                                         f'//div[@role="presentation" and @aria-live="polite"]/div').text

                    if mes_año_portal == mes_string + ' ' + anio:
                        Trace.write(conf, 'I', f'selecciona: {mes_string} {anio}')
                        break
                    driver.find_element(By.XPATH, '//button[@aria-label="Next month"]').click()

            if mes_search_int < mes_portal_int:
                Trace.write(conf, 'I', 'Decrementa hasta el mes correspondiente')
                while mes_portal_int < mes_portal_int:
                    mes_search_int -= 1
                    mes_año_portal = driver.find_element(By.XPATH,
                                                         f'//div[@role="presentation" and @aria-live="polite"]/div').text

                    if mes_año_portal == mes_string + ' ' + anio:
                        Trace.write(conf, 'I', f'selecciona: {mes_string} {anio}')
                        break
                    driver.find_element(By.XPATH, '//button[@aria-label="Previous month"]').click()

            if mes_search_int == mes_search_int:
                #Trace.write(conf, 'I', 'Es igual')
                mes_año_portal = driver.find_element(By.XPATH,
                                                     '//div[@role="presentation" and @aria-live="polite"]/div').text

                if mes_año_portal == mes_string + ' ' + anio:
                    Trace.write(conf, 'I', f'Seleccion correcta de {mes_string} {anio}')

            time.sleep(3)
            Trace.write(conf, 'I', f'Seleccionamos Dia: {dia_search_int}')
            driver.find_element(By.XPATH, f'//button[text()="{dia_search_int}"]').click()
            time.sleep(3)

            Trace.write(conf, 'I', 'Seleccionamos: Aplicar')
            driver.find_element(By.XPATH, f'//button[contains(text(),"plicar")]').click()
            time.sleep(3)

            Trace.write(conf, 'I', 'Seleccionamos: Desplegar por: DAY')
            desplejar_por = driver.find_element(By.XPATH, f'//input[@value="WEEKLY"]')
            value = desplejar_por.get_attribute('value')

            #Trace.write(conf, 'I', value)
            if value != 'DAILY':
                driver.find_element(By.XPATH, f'//input[@value="WEEKLY"]/parent::div').click()
                time.sleep(2)
                driver.find_element(By.XPATH, f'//li[@data-value="DAILY"]').click()
                time.sleep(2)

            # --------------------------------Seleccionando Productos ----------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Seleccion de Productos')

            status_prod = driver.find_element(By.XPATH,
                                               f'//p[contains(text(),"Selección de Productos")]/parent::div/parent::div/parent::div')
            actions0 = ActionChains(driver)
            actions0.move_to_element(status_prod).perform()
            if status_prod.get_attribute('aria-expanded') == 'false':
                status_prod.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Numero de Item')
            status_items = driver.find_element(By.XPATH, f'//div[@data-testid="sku"]/div')
            status_items.click()
            time.sleep(3)
            try:
                driver.find_element(By.XPATH, f'//span[text()="Unselect All"]/preceding-sibling::span').click()
                time.sleep(2)
                status_items.click()
                driver.find_element(By.XPATH, f'//span[text()="Select All"]/preceding-sibling::span').click()
            except:
                try:
                    status_items.click()
                    driver.find_element(By.XPATH, f'//span[text()="Select All"]/preceding-sibling::span').click()
                    time.sleep(2)
                except:
                    raise Exception('Cambio la seleccion de Items')

            # actions = ActionChains(driver)
            # actions.send_keys(Keys.ESCAPE).perform()
            time.sleep(2)

            # --------------------------------Seleccionando Productos ----------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Seleccion de Club')
            status_prod = driver.find_element(By.XPATH,
                                               f'//p[contains(text(),"Selección de Club")]/parent::div/parent::div/parent::div')
            actions0 = ActionChains(driver)
            actions0.move_to_element(status_prod).perform()
            if status_prod.get_attribute('aria-expanded') == 'false':
                status_prod.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Club')
            status_club = driver.find_element(By.XPATH, f'//div[@data-testid="store"]/div/div/input')
            status_club.click()
            time.sleep(3)
            try:
                driver.find_element(By.XPATH, f'//span[text()="Unselect All"]/preceding-sibling::span').click()
                status_club.click()
                time.sleep(2)
                driver.find_element(By.XPATH, f'//span[text()="Select All"]/preceding-sibling::span').click()
            except:
                try:
                    status_club.click()
                    driver.find_element(By.XPATH, f'//span[text()="Select All"]/preceding-sibling::span').click()
                    time.sleep(2)
                except:
                    raise Exception('Cambio la seleccion de Club')



            # --------------------------------Seleccionando Dimensiones ----------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Dimenciones y Metricas')
            status_filters = driver.find_element(By.XPATH,
                                                 f'//div[contains(text(),"Dimensiones y Métricas")]/parent::div')
            if status_filters.get_attribute('aria-expanded') == 'false':
                status_filters.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Agrupar Dimensiones')
            metrica_dime = driver.find_element(By.XPATH, f'//label[text() = "Dimensión"]/parent::div')
            actions2 = ActionChains(driver)
            actions2.move_to_element(metrica_dime).perform()
            time.sleep(2)

            Trace.write(conf, 'I', 'Introduccionedo Dimensiones a buscar')
            dimensiones = ['Tribe', 'Club', 'Item']  # Axel 2108
            ##El arreglo  contiene las maetricas a a selecicionar del apartado de dimensiones.
            # dimensiones = ['Tribe', 'Club', 'UPC', 'Item']#Frank 2037
            #print(dimensiones)

            for i in dimensiones:
                metrica_dime.click()
                time.sleep(2)
                input_dime = driver.find_element(By.XPATH, f'//input[@placeholder="Buscar"]')
                time.sleep(2)
                #Trace.write(conf, 'I', f'Buscando {i}')
                input_dime.clear()
                time.sleep(2)
                input_dime.send_keys(i)

                time.sleep(2)

                if i == 'Club':
                    Trace.write(conf, 'I', f'Seleccionamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                elif i == 'Item':
                    Trace.write(conf, 'I', f'Seleccionamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                elif i == 'Tribe':
                    #Trace.write(conf, 'I', f'Eliminamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                elif i == 'UPC':
                    Trace.write(conf, 'I', f'Seleccionamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                elif i == 'Fabricante':
                    Trace.write(conf, 'I', f'Seleccionamos: {i}')
                    dimension = driver.find_elements(By.XPATH, f'//span[text()="{i}"]')
                    dimension[0].click()

                time.sleep(2)
                actions = ActionChains(driver)
                actions.send_keys(Keys.ESCAPE).perform()
                time.sleep(2)

            time.sleep(2)
            Trace.write(conf, 'I', 'Verificamos dimensiones seleccionadas')
            dimensiones_value = driver.find_element(By.XPATH, f'//label[text() = "Dimensión"]/parent::div/div/input')
            #Trace.write(conf, 'I', dimensiones_value.get_attribute('value'))
            con_palabras = ','
            for i, dimension in enumerate(dimensiones):
                palabra = dimension
                if i != 0:
                    con_palabras = f'{con_palabras}, {palabra}'
                metrica_sin_tribe=con_palabras.replace(",, ", '')

            if dimensiones_value.get_attribute('value') == metrica_sin_tribe:
                Trace.write(conf, 'I', 'Las dimensiones esta correctamente seleccionadas')
            else:
                raise FactException(ResultType.EXCEPTION.value, 'Error en Dimesiones')

            time.sleep(2)

            # --------------------------------Seleccionando Metricas ----------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Seleccion Metricas')
            status_metricas = driver.find_element(By.XPATH,
                                               f'//p[contains(text(),"Selección de Métricas")]/parent::div/parent::div/parent::div')
            actions0 = ActionChains(driver)
            actions0.move_to_element(status_prod).perform()
            if status_metricas.get_attribute('aria-expanded') == 'false':
                status_metricas.click()
            time.sleep(2)


            metrica_metr = driver.find_element(By.XPATH, f'//label[text() = "Métricas"]/parent::div')
            action3 = ActionChains(driver)
            action3.move_to_element(metrica_metr).perform()
            metrica_metr.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Ingresando Metricas a buscar')
            metricas = ['Sales (Mex$)', 'Unit Sales', 'Store OH EoP Quantity', 'Store OH EoP Sell Mex$']  # AXEL
            ##El arreglo  contiene las maetricas a a selecicionar del apartado de dimensiones.
            # metricas = ['Sales (Mex$)', 'Unit Sales']  # Frank
            #print(metricas)

            input_metr = driver.find_element(By.XPATH, f'//input[@placeholder="Buscar"]')

            for i in metricas:
                input_metr.clear()
                time.sleep(2)
                Trace.write(conf, 'I', f'Seleccionamos: {i}')
                input_metr.send_keys(i)
                time.sleep(2)

                metrica = driver.find_element(By.XPATH, f'//span[text() = "{i}"]/parent::div/parent::li')

                if metrica.get_attribute('aria-selected') == 'false':
                    metrica.click()
                time.sleep(2)

            actions.send_keys(Keys.ESCAPE).perform()
            time.sleep(2)

            # ----------------------------Proceso de Descarga ----------------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Ejecutar')
            driver.find_element(By.XPATH, f'//p[contains(text(),"Ejecutar informe")]').click()
            time.sleep(2)
            time.sleep(10)

            Trace.write(conf, 'I', 'Seleccionamos: Exportamos')
            driver.find_element(By.XPATH, f'//span[@aria-label="Exportar"]').click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Expor a CSV')
            driver.find_element(By.XPATH, f'//li[@aria-label="Exportar a CSV"]').click()
            time.sleep(2)

            name_archivo = driver.find_element(By.XPATH,
                                               '//input[@class="sc-dmctIk sc-hAQmFe ifqOwr dkjbCA MuiInputBase-input MuiOutlinedInput-input MuiInputBase-inputSizeSmall"]').get_attribute(
                "value")
            Trace.write(conf, 'I', f'Generando archivo: {name_archivo}')
            time.sleep(2)

            try:
                driver.find_element(By.XPATH, '//*[contains(text(), "Notifíqueme por Email")]').click()
            except:
                pass

            Trace.write(conf, 'I', 'Aceptamos Alerta a exportar')
            driver.find_element(By.XPATH, f'//button[text()="OK"]').click()
            time.sleep(5)

            #VA ENTRAR AL RELOJ A SBUSCAR EL ARCHIVO CON EL NOMBRE QUE SE GENERO Y EXTRAIMOS ARRIBA
            try:
                WebDriverWait(driver, 10).until(EC.visibility_of_element_located((
                    By.XPATH, '//*[contains(text(), "Su solicitud de descarga")]')))

                driver.find_element(By.XPATH, '//button[contains(text(), "OK")]').click()
                time.sleep(2)

                Trace.write(conf, 'I', 'Exportar Actividades.')
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(1)
                driver.switch_to.default_content()
                time.sleep(2)
                driver.find_element(By.XPATH,
                                    '//button[@data-testid="data_exporter_status_panel_export_activities_button"]').click()

                # try:
                Trace.write(conf, 'I', 'Descargando archivo.')
                WebDriverWait(driver, 300).until(EC.visibility_of_element_located((
                    By.XPATH, f'//*[contains(text(), "{name_archivo}")]')))

                #enable_download_in_headless_chrome(driver, paths.files_origin)
                driver.find_element(By.XPATH,
                                    f'//*[contains(text(), "{name_archivo}")]//parent::div//parent::div/div[@data-testid="data-export-download"]').click()

            except:
                Trace.write(conf, 'I', 'La descarga fue directa.')


            actions = ActionChains(driver)
            actions.send_keys(Keys.ESCAPE).perform()
            time.sleep(2)

            wait_download(driver, conf, 2, paths, 'csv')

            flag = True

        #Seleccion de metricas
        if fact == 'samseco':

            Trace.write(conf, 'I', 'Proceso')
            Trace.write(conf, 'I', 'Seleccionamos: Insights')
            driver.find_element(By.XPATH, "//span[text()='Insights']").click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Last Mile')
            report = driver.find_element(By.XPATH, "//h2[text()='Basket Insights']")
            actions = ActionChains(driver)
            actions.move_to_element(report).perform()
            driver.find_element(By.XPATH, "//h2[text()='Last Mile']").click()
            time.sleep(2)

            iframe = driver.find_element(By.XPATH,
                                         '//div[@class="PowerBIReports__StyledReportContainer-sc-1hozppo-0 jnZxbX"]/iframe').click()

            driver.switch_to.frame(iframe)

            Trace.write(conf, 'I', 'Seleccionamos: Create Date Select by')

            time.sleep(2)

            element_time = driver.find_elements(By.XPATH,
                                                '//div[@class="slicer-dropdown-menu"]/div[text() = "All Time"]')

            element_time[0].click()

            Trace.write(conf, 'I', 'Seleccionamos: Month to Date')

            driver.find_element(By.XPATH, '//span[text()="Month to Date"]').click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Custom')

            customs = driver.find_elements(By.XPATH,
                                           '//div[text()="Custom"]/parent::div/parent::div/parent::div/parent::div/parent::div//*[@class="fill ui-role-button-fill sub-selectable"]')
            customs[0].click()

            actions.move_to_element(customs[1]).perform()

            # ----------------------------Configuracion de Fechas --------------------------------------------------------
            Trace.write(conf, 'I', 'Seleccionamos: Fechas')

            date_search = date_from.split('-')
            dia = date_search[2]
            mes = date_search[1]
            año = date_search[0]

            dia_search_int = int(dia)
            mes_search_int = int(mes)
            año_search_int = int(año)
            result = True

            Trace.write(conf, 'I', 'Eligiendo la version del calendario')
            mes_calendar = calendar_months(mes, 'calendar_v3')
            time.sleep(2)

            mes_string = mes_calendar.lower()

            element_date = driver.find_elements(By.XPATH, '//button[@class="calendar-button enter-button"]')
            time.sleep(2)

            # --------------------------------Introduccion de Fechas FIN----------------------------------------------------------

            Trace.write(conf, 'I', 'Seleccionamos: Fechas Fin')

            element_date[1].click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Año')
            mes_año_element = driver.find_element(By.XPATH, '//button[@aria-label="Month Picker"]').click()
            time.sleep(2)

            año_element = ''

            while result == True:
                año_element = driver.find_element(By.XPATH, '//button[@aria-label="Date Picker"]').text
                Trace.write(conf, 'I', año_element)

                if año_search_int == int(año_element):
                    Trace.write(conf, 'I', 'El mes es igual')
                    result == False
                if año_search_int < int(año_element):
                    Trace.write(conf, 'I', 'Decrementa el año')
                    driver.find_element(By.XPATH, '//button[@aria-label="Previous year"]').click()

                if año_search_int > int(año_element):
                    Trace.write(conf, 'I', 'Incrementa el año')
                    driver.find_element(By.XPATH, '//button[@aria-label="Next year"]').click()

                time.sleep(2)

            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: el mes')
            driver.find_element(By, f'//button[contains(text(),"{mes_string}")]').click()
            time.sleep(2)

            Trace.write(conf, 'I', f'Seleccionamos: dia{dia_search_int}')

            dia_element = driver.find_elements(By.XPATH,
                                               f'//div[@class="calendar-table"]/div/div/button[text()=" {dia_search_int} "]')

            if len(dia_element) == 2:
                dia_element[1].click()
            elif len(dia_element) == 1:
                dia_element[0].click()
            time.sleep(3)

            # ----------------------------------Introduccion de Fechas FIN------------------------------------------

            Trace.write(conf, 'I', 'Seleccionamos: Fechas Inicio')
            actions.move_to_element(customs[1]).perfom()

            element_date[0].click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Año')

            mes_año_element = driver.find_element(By.XPATH, '//button[@aria-label="Month Picker"]').click()
            time.sleep(2)

            año_element = ''
            result = True
            while result == True:
                año_element = driver.find_element(By.XPATH, '//button[@aria-label="Date Picker"]').text
                Trace.write(conf, 'I', año_element)
                if año_search_int == int(año_element):
                    Trace.write(conf, 'I', 'El mes es igual')
                    result == False
                if año_search_int < int(año_element):
                    Trace.write(conf, 'I', 'Decrementa el año')
                    driver.find_element(By.XPATH, '//button[@aria-label="Previous year"]').click()
                if año_search_int > int(año_element):
                    Trace.write(conf, 'I', 'Incrementa el año')
                    driver.find_element(By.XPATH, '//button[@aria-label="Next year"]').click()
                time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: el mes')
            driver.find_element(By, f'//button[contains(text(),"{mes_string}")]').click()
            time.sleep(2)

            Trace.write(conf, 'I', f'Seleccionamos: dia{dia_search_int}')

            dia_element = driver.find_elements(By.XPATH,
                                               f'//div[@class="calendar-table"]/div/div/button[text()=" {dia_search_int} "]')

            if len(dia_element) == 2:
                dia_element[1].click()
            elif len(dia_element) == 1:
                dia_element[0].click()
            time.sleep(3)

            elements = driver.fins_elements(By.CLASS_NAME, 'cicle')
            invisibility_promises = [
                WebDriverWait(driver, 10).until(EC.invisibility_of_element(element))
                for element in elements]
            Trace.write(conf, 'I', 'Seleccionamos: Exclude Net Paid Order')
            parametros = driver.find_elements(By.XPATH, '//div[@class="slicerItemContainer"]')
            bol = getattr(parametros[0], 'aria-selected')
            Trace.write(conf, 'I', bol)
            if bol == 'false':
                parametros[0].click()
            Trace.write(conf, 'I', 'Esperamos Reporte')
            time.sleep(15)

            Trace.write(conf, 'I', 'Descargando Reporte')
            report_view = driver.find_element(By.XPATH, '//div[@class="tableExContainer"]')
            actions.move_to_element(report_view).perform()
            report_view.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Más opciones')
            opciones = driver.find_element(By.XPATH, '//*[@aria-label="More options"]')
            actions.move_to_element(opciones).perform()
            opciones.click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Export data')
            driver.find_element(By.XPATH, '//button[@aria-label="Export data"]').click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Summarized data')
            driver.find_element(By.XPATH, '//span[text()="Summarized data"]').click()
            time.sleep(2)

            Trace.write(conf, 'I', 'Seleccionamos: Export')
            driver.find_element(By.XPATH, '//button[@aria-label="Export"]').click()
            time.sleep(2)
            

    except Exception as e:
        FileHelper.save_screen(driver, paths, conf)
        Trace.write(conf, 'E', f'Error: Finalizado en Area de Descarga. Excepcion: {e}')
        raise FactException(ResultType.EXCEPTION.value, 'Finalizado con Error Desconocido')

    return flag


# Función que transforma los archivos descargados a sus .csv correspondientes
def transformar_archivo(conf, date, paths, file_id, fact,dias):
    robot_v = conf.robot_version
    time_id = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=dias)).strftime('%Y-%m-%d')
    chain = conf.chain_id
    supplier_id = conf.supplier_id
    file_name = f'{file_id}.csv'
    days = set()

    try:
        Trace.write(conf, 'I', 'Iniciando proceso de transformación.')

        if fact == 'sams00v':

            files_csv = [f for f in os.listdir(paths.files_origin) if f.endswith('.csv')]
            for file_csv in files_csv:

                if 'Filter Selections' in file_csv:
                    pass

                else:
                    df = pd.read_csv(paths.files_origin + file_csv, encoding='utf-8', dtype=str)

                    df['Club Code'] = df['Club Code'].apply(lambda x: x.replace(',', '').strip())
                    df['Club'] = df['Club'].apply(lambda x: x.replace(',', '').strip())
                    df['Item Code'] = df['Item Code'].apply(lambda x: x.replace(',', '').strip())
                    df['UPC Code'] = df['UPC Code'].apply(lambda x: x.replace(',', '').strip())
                    df['Item'] = df['Item'].apply(lambda x: x.replace(',', '').strip())
                    df['DIARIO'] = df['DIARIO'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))
                    df['Period Start Date'] = df['Period Start Date'].apply(
                        lambda x: datetime.strptime(x, '%Y/%m/%d').strftime('%Y-%m-%d'))
                    df['Period End Date'] = df['Period End Date'].apply(
                        lambda x: datetime.strptime(x, '%Y/%m/%d').strftime('%Y-%m-%d'))
                    df['Sales (Mex$) / Actual'] = df['Sales (Mex$) / Actual'].apply(lambda x: x.replace(',', '').strip())
                    df['Unit Sales / Actual'] = df['Unit Sales / Actual'].apply(lambda x: x.replace(',', '').strip())
                    df['Store OH EoP Quantity / Actual'] = df['Store OH EoP Quantity / Actual'].apply(
                        lambda x: x.replace(',', '').strip())
                    df['Store OH EoP Sell Mex$ / Actual'] = df['Store OH EoP Sell Mex$ / Actual'].apply(
                        lambda x: x.replace(',', '').strip())

                    assert df['DIARIO'].eq(
                        time_id).all(), f"Error, no todas las fechas contenidas en el archivo son iguales a {time_id} \n "
                    days.add(df['DIARIO'].iloc[0])

                    df['File_id'] = file_id
                    df['Robot_v'] = robot_v
                    df['Time_id'] = df['DIARIO']
                    df['Chain'] = chain
                    df['Supplier_id'] = supplier_id

                    df[['File_id', 'Chain', 'Supplier_id', 'Time_id',
                        'Club Code', 'Club', 'Item Code', 'UPC Code', 'Item', 'DIARIO',
                        'Period Start Date', 'Period End Date', 'Sales (Mex$) / Actual',
                        'Unit Sales / Actual', 'Store OH EoP Quantity / Actual',
                        'Store OH EoP Sell Mex$ / Actual']] \
                        .to_csv(paths.temp + file_name, encoding='utf-8', header=False, index=False)

                    Trace.write(conf, 'I', f'Archivo {file_name} Generado.')

        if fact == 'sams00b':

            files_csv = [f for f in os.listdir(paths.files_origin) if f.endswith('.csv')]
            for file_csv in files_csv:

                if 'Filter Selections' in file_csv:
                    pass

                else:
                    df = pd.read_csv(paths.files_origin + file_csv, encoding='utf-8', dtype=str)

                    df['Club Code'] = df['Club Code'].apply(lambda x: x.replace(',', '').strip())
                    df['Club'] = df['Club'].apply(lambda x: x.replace(',', '').strip())
                    df['Item Code'] = df['Item Code'].apply(lambda x: x.replace(',', '').strip())
                    df['UPC Code'] = df['UPC Code'].apply(lambda x: x.replace(',', '').strip())
                    df['Item'] = df['Item'].apply(lambda x: x.replace(',', '').strip())
                    df['DIARIO'] = df['DIARIO'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))
                    df['Period Start Date'] = df['Period Start Date'].apply(
                        lambda x: datetime.strptime(x, '%Y/%m/%d').strftime('%Y-%m-%d'))
                    df['Period End Date'] = df['Period End Date'].apply(
                        lambda x: datetime.strptime(x, '%Y/%m/%d').strftime('%Y-%m-%d'))
                    df['Sales (Mex$) / Actual'] = df['Sales (Mex$) / Actual'].apply(lambda x: x.replace(',', '').strip())
                    df['Unit Sales / Actual'] = df['Unit Sales / Actual'].apply(lambda x: x.replace(',', '').strip())
                    df['Store OH EoP Quantity / Actual'] = df['Store OH EoP Quantity / Actual'].apply(
                        lambda x: x.replace(',', '').strip())
                    df['Store OH EoP Sell Mex$ / Actual'] = df['Store OH EoP Sell Mex$ / Actual'].apply(
                        lambda x: x.replace(',', '').strip())

                    assert df['DIARIO'].eq(
                        time_id).all(), f"Error, no todas las fechas contenidas en el archivo son iguales a {time_id} \n "
                    days.add(df['DIARIO'].iloc[0])

                    df['File_id'] = file_id
                    df['Robot_v'] = robot_v
                    df['Time_id'] = df['DIARIO']
                    df['Chain'] = chain
                    df['Supplier_id'] = supplier_id

                    df[['File_id', 'Chain', 'Supplier_id', 'Time_id',
                        'Club Code', 'Club', 'Item Code', 'UPC Code', 'Item', 'DIARIO',
                        'Period Start Date', 'Period End Date', 'Sales (Mex$) / Actual',
                        'Unit Sales / Actual', 'Store OH EoP Quantity / Actual',
                        'Store OH EoP Sell Mex$ / Actual']] \
                        .to_csv(paths.temp + file_name, encoding='utf-8', header=False, index=False)

                    Trace.write(conf, 'I', f'Archivo {file_name} Generado.')

        if fact == 'samseco':

            files_csv = [f for f in os.listdir(paths.files_origin) if f.endswith('.csv')]
            for file_csv in files_csv:
                df = pd.read_csv(paths.files_origin + file_csv, encoding='utf-8', dtype=str)
                print(df.columns)

                df['Club Code'] = df['Club Code'].apply(lambda x: x.replace(',', '').strip())
                df['Club'] = df['Club'].apply(lambda x: x.replace(',', '').strip())
                df['Item Code'] = df['Item Code'].apply(lambda x: x.replace(',', '').strip())
                df['UPC Code'] = df['UPC Code'].apply(lambda x: x.replace(',', '').strip())
                df['Item'] = df['Item'].apply(lambda x: x.replace(',', '').strip())
                df['DIARIO'] = df['DIARIO'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))
                df['Period Start Date'] = df['Period Start Date'].apply(
                    lambda x: datetime.strptime(x, '%Y/%m/%d').strftime('%Y-%m-%d'))
                df['Period End Date'] = df['Period End Date'].apply(
                    lambda x: datetime.strptime(x, '%Y/%m/%d').strftime('%Y-%m-%d'))
                df['Sales (Mex$) / Actual'] = df['Sales (Mex$) / Actual'].apply(lambda x: x.replace(',', '').strip())
                df['Unit Sales / Actual'] = df['Unit Sales / Actual'].apply(lambda x: x.replace(',', '').strip())
                df['Store OH EoP Quantity / Actual'] = df['Store OH EoP Quantity / Actual'].apply(
                    lambda x: x.replace(',', '').strip())
                df['Store OH EoP Sell Mex$ / Actual'] = df['Store OH EoP Sell Mex$ / Actual'].apply(
                    lambda x: x.replace(',', '').strip())

                assert df['DIARIO'].eq(
                    time_id).all(), f"Error, no todas las fechas contenidas en el archivo son iguales a {time_id} \n "

                df['File_id'] = file_id
                df['Robot_v'] = robot_v
                df['Time_id'] = df['Period End Date']
                df['Chain'] = chain
                df['Supplier_id'] = supplier_id

                df[['File_id', 'Chain', 'Supplier_id', 'Time_id',
                    'Club Code', 'Club', 'Item Code', 'UPC Code', 'Item', 'DIARIO',
                    'Period Start Date', 'Period End Date', 'Sales (Mex$) / Actual',
                    'Unit Sales / Actual', 'Store OH EoP Quantity / Actual',
                    'Store OH EoP Sell Mex$ / Actual']] \
                    .to_csv(paths.temp + file_name, encoding='utf-8', header=False, index=False)

                Trace.write(conf, 'I', f'Archivo {file_name} Generado.')

        Trace.write(conf, 'I', 'Proceso de Transformacion Finalizado.')
        FileHelper.gz_files_by_extension(paths.temp, paths.files_transformed, 'csv')
        FileHelper.clear_path_empty_files(paths.temp, 'csv')

    except FactException as e:
        raise FactException(e.code, e.message)
    except Exception as e:
        Trace.write(conf, 'E', f'Error: Finalizado en Area Transformacion. Excepcion: {e}')
        raise FactException(ResultType.EXCEPTION.value, 'Finalizado con Error Desconocido')

    return days


def proceso_dl(driver, date, conf, paths, fact, dias):
    error_code = ResultType.OK
    datecode = datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')
    job = f'bif_aut_day_mex{conf.chain_id}_{fact}_v00_{conf.supplier_id}_{datecode}'
    Trace.write(conf, 'I', f'Genrando reporte: {fact}.')

    try:
        FileHelper.clear_paths([paths.temp, paths.files_origin])
        downloaded = descarga_dl(driver, conf, date, paths, fact, dias)
        if downloaded:
            days = transformar_archivo(conf, date, paths, job, fact, dias)
            FileHelper.zip_all_to_one_file(paths.files_origin, paths.backup, job)
            Exec.upload_files(conf, paths)
            Exec.execute_bq_script(conf, paths, fact, days, date)
            confirm_plan(conf, date, fact)
            Trace.write(conf, 'I', f'Tarea {fact} Finalizada Ok')
        else:
            error_code = ResultType.DATA_NOT_AVAILABLE
        '''Cuando solo se necesite transformar se comenta todo lo de adentro del try y se quita el comentario de la
        siguiente linea'''
        # transformar_archivo(conf, date, paths, job, fact)
    except FactException as e:
        error_code = ResultType.EXCEPTION
        Trace.write(conf, 'E', f'{fact}. Excepcion: {e}')

    return error_code


def confirm_plan(conf, day, subtype):
    if subtype == 'garisb' and conf.type.strip() == 'DA':
        day = (datetime.strptime(day, '%Y-%m-%d') + timedelta(days=4)).strftime('%Y-%m-%d')

    try:
        Trace.write(conf, 'I', f'Confirmando plan day: {day}, subtype: {subtype}')
        subtasks = list(filter(lambda x: (x['date'] == day and x['subtype'].upper() == subtype.upper()), conf.tasklist))
        for subtask in subtasks:
            parameters = {"drt_id": conf.drt_id, "data": '{"day": ""}', "subtype": subtype, "date": day[0:10],
                          "status": "F"}
            response = requests.post(conf.subtask_api, json=parameters)
    except Exception as e:
        Trace.write(conf, 'E', f'Error: Notificando estado del plan. Excepcion: {e}')

# Función que crea y mapea los argumentos que necesita el programa
def parser_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-in_url", help="Url para obtener los parametros de ejecucion", required=True)
    parser.add_argument("-in_drt_id", help="id de la tarea", required=True)
    args = parser.parse_args()
    return args


def end_program(driver, conf, paths, code, message, exception=""):
    if conf.type == 'DA':
        Exec.upload_files_out(conf, paths)
    driver.quit()
    if exception == "":
        Trace.write(conf, 'I', message)
    else:
        Trace.write(conf, 'E', f'{message}. Exception: {exception}')
    # para que pueda recibir un enum o un int
    try:
        code_val = code.value
    except Exception as e:
        code_val = code

    for elem in conf.tasklist:
        if elem['status'] != 'F':
            code_val = 910

    if code_val != 0:
        status = 'E'
    else:
        status = 'F'
    # print(status)
    parameters = {"drt_id": conf.drt_id, "status": status, "status_code": code_val, "status_message": message}
    response = requests.post(conf.task_api, json=parameters)
    sys.exit(0)


def main():
    paths = Paths()
    if os.name == "nt":
        paths.data = os.path.dirname(os.path.abspath(__file__)) + '\data\\'  # Aqui se guardan los archivos .est y .log
        paths.images = os.path.dirname(
            os.path.abspath(__file__)) + '\_Images\\'  # Se guardan las imagenes en caso de requerir image recognition
        paths.files_transformed = os.path.dirname(
            os.path.abspath(__file__)) + '\_Upload_gz\\'  # Archivo final a subir a BQ comprimido .gz
        paths.temp = os.path.dirname(
            os.path.abspath(__file__)) + '\_Temp\\'  # Aqui se realizan las transformaciones necesarias al archivo
        paths.backup = os.path.dirname(os.path.abspath(
            __file__)) + '\_Backup\\'  # Se guarda una copia del archivo original descargado si se requiere
        paths.files_origin = os.path.dirname(
            os.path.abspath(__file__)) + '\_Fuentes\\'  # Aqui se descargan los archivos
        paths.base = os.path.dirname(os.path.abspath(__file__))
    else:
        paths.data = os.path.dirname(os.path.abspath(__file__)) + '/data/'  # Aqui se guardan los archivos .est y .log
        paths.images = os.path.dirname(
            os.path.abspath(__file__)) + '/_Images/'  # Se guardan las imagenes en caso de requerir image recognition
        paths.files_transformed = os.path.dirname(
            os.path.abspath(__file__)) + '/_Upload_gz/'  # Archivo final a subir a BQ comprimido .gz
        paths.temp = os.path.dirname(
            os.path.abspath(__file__)) + '/_Temp/'  # Aqui se realizan las transformaciones necesarias al archivo
        paths.backup = os.path.dirname(os.path.abspath(
            __file__)) + '/_Backup/'  # Se guarda una copia del archivo original descargado si se requiere
        paths.files_origin = os.path.dirname(os.path.abspath(__file__)) + '/_Fuentes/'  # Aqui se descargan los archivos
        paths.base = os.path.dirname(os.path.abspath(__file__))

    args = parser_args()
    parameters = {"drt_id": args.in_drt_id}
    response = requests.post(args.in_url, json=parameters)
    # Este whit open obtendra un json para probar el robot de forma local y no alterar tanto la estrucutuara del mismo
    with open(os.path.dirname(__file__) +'/json_retool_python_sams.json', 'r') as json_file:
        data = json.load(json_file)
        print(data)
    conf = Config(**data)

    # Json Para produccion
    #conf = Config(**response.json())

    FileHelper.clear_paths([paths.temp, paths.files_origin, paths.backup, paths.files_transformed, paths.images])
    driver = set_driver(conf, paths)

    #driver = iniciar_sesion_phone(driver, conf, paths)
    driver =  iniciar_sesion_email(driver, conf, paths)
    days_sams00v = set()
    days_sams00b = set()
    days_samseco = set()


    list_errors = []
    conf.tasklist.sort(key=lambda item: item['date'])
    for elem in conf.tasklist:
        if elem['status'] != 'F' and elem['subtype'].upper() == 'SAMS00V':
            days_sams00v.add(elem['date'])
        elif elem['status'] != 'F' and elem['subtype'].upper() == 'SAMS00B':
            days_sams00b.add(elem['date'])
        elif elem['status'] != 'F' and elem['subtype'].upper() == 'SAMSECO':
            days_samseco.add(elem['date'])

    if conf.type.strip() == 'DA':
        for date in days_sams00v:
            e_code = proceso_dl(driver, date, conf, paths, 'sams00v', 0)
            list_errors.append(e_code.value)
        for date in days_sams00b:
            e_code = proceso_dl(driver, date, conf, paths, 'sams00b', 2)
            list_errors.append(e_code.value)
        for date in days_samseco:
            e_code = proceso_dl(driver, date, conf, paths, 'samseco', 2)
            list_errors.append(e_code.value)

    response = requests.post(args.in_url, json=parameters)
    conf = Config(**response.json())

    for l in list_errors:
        if l != 0:
            error_code = l
            break

    end_program(driver, conf, paths, error_code, 'Tarea Finalizada.')


if __name__ == "__main__":
    main()