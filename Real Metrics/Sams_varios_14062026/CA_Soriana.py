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
https://socios.soriana.com/index/index.html#
-in_usuario
nubia.nez@cremeria-americana.com.mx
-in_password
Cremeria1*
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
from datetime import date, timedelta, datetime
#Correo
import imghdr
import smtplib
import ssl
from email.message import EmailMessage

def send_email(driver, subject, task):
    # Envio de captura a correo electronico para monitorear al portal en tiempo real
    time.sleep(5)
    driver.save_screenshot('screen.png')
    user_mail = 'jgarcia@realmetrics.io'
    password_mail = 'Quv60631'
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
        print("Accediendo a {}".format(address))
        print('Accediendo al portal')

        driver.get(address)

        # 5. Comportamiento humano post-carga:
        # Los bots interactúan al milisegundo de entrar. Mueve el mouse o espera un poco de forma orgánica.
        time.sleep(10)

    except Exception as e:
        print(f'Error al acceder al portal: {e}')
        sys.exit(0)

    return driver


def cerrar_aviso_informacion(driver):
    """
    Cierra el anuncio de política o aviso de información si aparece.
    Si no aparece, continúa sin errores.
    """


    print( "Validando si existen anuncios.")

    try:
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, '//*[text()="Comunicados"]')))
        bandera = 1
    except TimeoutException:
        print( "No se encontraron Anuncios.")
        bandera = 0


    if bandera == 1:
        try:
            print( "Cerrando aviso de Comunicados.")
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
                        print("Cerrando Ultimo Comunicado.")
                        WebDriverWait(driver, 30).until(EC.visibility_of_element_located((
                            By.XPATH, '//bdi[contains(text(),"Terminar")]'))
                        )
                        WebDriverWait(driver, 30).until(
                            EC.element_to_be_clickable((By.XPATH, '//bdi[contains(text(),"Terminar")]'))).click()
                        time.sleep(5)
                        print( "Confirmando cerrar Comunicados.")
                        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                            By.XPATH, '//bdi[contains(text(),"S")]')))
                        WebDriverWait(driver, 30).until(
                            EC.element_to_be_clickable((By.XPATH, '//bdi[contains(text(),"S")]'))).click()
                        time.sleep(2)
                        break
                    except Exception as q:
                        print('E', f"Error al cerrar los Comunicados {q}.")
                        raise Exception(f"Error al cerrar los Comunicados {q}.")

        except Exception as e:
            raise Exception (f'Error al cerrar los comunicados: {e}.')



def iniciar_sesion(driver, username_text, password_text, path_file):

    try:

        WebDriverWait(driver, 100).until(EC.visibility_of_element_located((
            By.XPATH, '//input[@placeholder="Correo"]')))

        driver.find_element(By.XPATH, '//input[@placeholder="Correo"]').send_keys(username_text)
        time.sleep(2)
        driver.find_element(By.XPATH, '//input[contains(@placeholder, "Contrase")]').send_keys(password_text)
        time.sleep(2)
        driver.find_element(By.XPATH, '//button[@id="__button1"]').click()
        time.sleep(10)

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


def aplicar_cambios(driver):
    print(f"Seleccionamos: Aplicar Filtro")
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


def busqueda_reportes(driver, dir_fuentes, in_fechaini, in_descarga, in_variables):

    months = {
        '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
    }
    proveedor = '9506'

    fecha = in_fechaini.split('/')
    anio = fecha[2]
    mes = fecha[1]
    if 'inventories' in in_descarga:
        dia = '1'

    dia = str(int(fecha[0]))

    cerrar_aviso_informacion(driver)
    
    print("Busqueda de Reporte")

    if 'fill_rate' in in_descarga:

        print("Iniciando Descarga Fill Rate.")

        try:
            print("Entrar Gestion de Proveedores.")

            driver.switch_to.default_content()

            print( f'Apartado Comercial.')
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//span[text()="Indicadores comerciales"]'))).click()
            time.sleep(2)

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//iframe[@id="__xmlview1--map_iframe"]'))
            )
            driver.switch_to.frame(iframe_1)
            time.sleep(5)
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//button[@id="basic-button" and text()="Consultas Ejecutivas"]'))).click()
            time.sleep(5)
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//li[text()="Nivel de Servicio"]'))).click()
            time.sleep(120)

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            iframe_2 = driver.find_elements(By.XPATH, '//iframe')
            driver.switch_to.frame(iframe_2[4])
            time.sleep(5)

            filtros = [ 'División',
                       'Categoría',
                       'Material',
                       'Código de Barras',
                       'Formato',
                       'Tienda',
                       'Pedido',
                       'Dimensión',
                       'Métricas',
                       'Fecha',
                       ]
            '''
            
            '''
            for filtro in filtros:
                print(f'Seleccionamos Filtro de {filtro}:')
                time.sleep(5)

                WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                    By.XPATH, f'//div[@aria-label="{filtro}"]'))).click()

                if filtro == 'Formato' or filtro == 'Dimensión' or filtro =='Métricas':
                    WebDriverWait(driver, 120).until(EC.visibility_of_element_located((
                        By.XPATH,
                        f'//div[@aria-multiselectable="true" and @aria-label="{filtro}" ]//div[@class="row"]')))

                    print('Seleccionamos cada una casillas.')

                    bandera = 0
                    while True:

                        list_formatos = driver.find_elements(By.XPATH,
                                                         f'//div[@aria-multiselectable="true" and @aria-label="{filtro}" ]//div[@class="row"]/div')

                        if bandera > len(list_formatos):
                            break

                        for index, format in enumerate(list_formatos):

                            acciones = ActionChains(driver)

                            valor_aria = format.get_attribute("aria-selected")
                            if valor_aria == 'true':
                                bandera += 1
                            else:

                                acciones.key_down(Keys.CONTROL)
                                acciones.click(format)
                                acciones.key_up(Keys.CONTROL)
                                acciones.perform()
                            time.sleep(1)

                            #La lista_format  va a traer 9 elementos, se va a romper al septimo y va a bajar
                            if index == 7:
                                print('Seleccionamos casillas faltantes.')

                                for i in range(7):
                                    acciones.key_down(Keys.DOWN)
                                    acciones.key_up(Keys.DOWN)
                                    acciones.perform()
                                break

                        time.sleep(30)

                elif filtro == 'Fecha':
                    print(f'Seleccionamos {months[mes]} {anio}.')
                    WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                        By.XPATH,
                        f'//div[@aria-multiselectable="true" and @aria-label="{filtro}" ]//span[text()="{months[mes]} {anio}"]'))).click()

                else:
                    print('Precionamos "Seleccionar todo".')
                    WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                        By.XPATH,f'//div[@aria-multiselectable="true" and @aria-label="{filtro}" ]//div[@title="Seleccionar todo"]'))).click()

            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//div[@aria-label="{filtro}"]'))).click()


            time.sleep(45)

            print(f'Descargamos Informacion.')
            ActionChains(driver).move_to_element(
                driver.find_element(By.XPATH, '//div[@class="top-viewport"]')  # div de tabla
            ).perform()
            time.sleep(2)

            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//*[@aria-label="Más opciones"]'))).click()

            time.sleep(2)

            print(f'Exportar Datos.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//button[@aria-description="Exportar datos"]'))).click()

            time.sleep(2)

            print(f'Selecciona Datos con diseño actual(xlsx).')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//*[text()="Datos con diseño actual"]/parent::span/preceding-sibling::div/span'))).click()

            time.sleep(5)

            print(f'Exportando archivo.')
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Exportar"]')
            btn_exportar.location_once_scrolled_into_view
            btn_exportar.click()
            time.sleep(5)
            esperar_descarga( dir_fuentes, 1)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            driver.close()
            sys.exit(0)

    if 'sell_out' in in_descarga:

        print("Iniciando Descarga Sell Out (Ventas).")

        try:
            print("Entrar Gestion de Proveedores.")

            driver.switch_to.default_content()

            print(f'Apartado Comercial.')

            print(f'Seleccionamos: Indicadores comerciales.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, '//span[text()="Indicadores comerciales"]'))).click()
            time.sleep(2)

            print(f'Elegimos Proveedor: {proveedor}.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, '//input[@placeholder="Proveedor"]'))).click()
            time.sleep(5)

            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//div[text()="{proveedor}"]/parent::div'))).click()
            time.sleep(7)

            assert proveedor in driver.find_element(By.XPATH, '//input[@placeholder="Proveedor"]').get_attribute(
                'value'), "Error Seleccionando usuario dentro del portal."

            print(f'Seleccion de proveedor con exito.')

            iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//iframe[@id="__xmlview1--map_iframe"]'))
            )
            driver.switch_to.frame(iframe_1)

            print(f'Seleccionamos: Generacion de reportes.')

            print(f'Seleccionamos: Asistente de generacion  de reportes.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, '//button[contains(text(),"Generaci") and contains(text(),"n de reportes")]'))).click()
            time.sleep(4)
            driver.find_element(By.XPATH,
                                '//*[contains(text(),"Asistente de generaci") and contains(text(),"n  de reportes")]').click()
            driver.switch_to.frame(driver.find_element(By.XPATH, '//div[@class="Embed-container"]/iframe'))
            time.sleep(2)

            print(f'Seleccionamos Mes Anio: {months[mes]} {anio}')
            WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, '//div[@aria-label="Meses"]')))
            driver.find_element(By.XPATH, '//div[@aria-label="Meses"]').click()
            time.sleep(3)
            try:
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[text()="{months[mes]} {anio}"]')))
                mes_dimension = driver.find_element(By.XPATH, f'//span[text()="{months[mes]} {anio}"]/parent::div')
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
                            raise Exception(f'El mes {months[mes]} no se encontro en la lista')
                        accion.key_down(Keys.DOWN)
                        accion.perform()
            except:
                raise Exception(f'El mes {months[mes]} no se encontro en la lista')

            aplicar_cambios(driver)

            try:
                time.sleep(15)
                fecha_inicio = datetime.strptime(in_fechaini, '%d/%m/%Y').strftime('01/%m/%Y')
                print(f'Rango de fechas a buscar: {fecha_inicio} - {fecha_inicio}.')
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, '//input[contains(@aria-label,"echa de fin")]/parent::div/button'))
                ).click()
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, f'//button[contains(@aria-description, ", {dia}")]'))
                ).click()
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, '//input[contains(@aria-label,"echa de inicio")]/parent::div/button'))
                ).click()
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, f'//button[contains(@aria-description, ", {dia}")]'))
                ).click()
            except Exception as e:
                print('Error en la seleccion de Fechas. Bottones', e)
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

            print(f"Verificando fecha seleccionada.")
            if fecha_inicio_seleccionada == fecha_final_seleccionada:
                print("Fechas seleccionadas correctamente.")
            else:
                raise Exception("Fechas seleccionadas NO correctas.")

            aplicar_cambios(driver)

            time.sleep(15)

            print("Apartado de Dimensiones a mostrar.")
            print("Limpiando apartado de Dimensiones a mostrar.")
            try:
                dimension_box = driver.find_element(By.XPATH,
                                                    '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]')
                ActionChains(driver).move_to_element(dimension_box).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//span[@aria-label="Borrar selecciones"]').click()
                time.sleep(2)
                print("Dimensiones a mostrar limpios.")
            except Exception as e:
                print("Error al limpiar los filtros de Dimensiones a mostrar.")
                raise Exception(f"Error al limpiar los filtros Dimensiones a mostrar: {e}.")

            print("Seleccionamos todas las Dimensiones.")
            try:
                driver.find_element(By.XPATH, '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]').click()
                time.sleep(5)
                dimensiones = driver.find_elements(By.XPATH,
                                                   '//div[@aria-label="Dimensiones a mostrar"]//div[@class="row"]/div')
                for dimencion in dimensiones:
                    valor_aria = dimencion.get_attribute("aria-selected")
                    if valor_aria == 'false':
                        time.sleep(1)
                        acciones = ActionChains(driver)
                        dimencion.click()
                        acciones.key_down(Keys.DOWN)
                        acciones.key_up(Keys.DOWN)
                        acciones.perform()

            except Exception as e:
                raise Exception('Error en la Seleccion de Dimensiones.')



            print("Seleccionamos todas las Metricas.")
            metricas = ['Venta (Pesos)', 'Venta (Unidades)']
            print(f"Seleccionamos las Metricas: {metricas}.")
            try:
                metricas_select = driver.find_elements(By.XPATH, '//div[@aria-label="Metricas"]//div[@class="row"]/div')

                for metric_select in metricas_select:
                    try:

                        if str(metric_select.text) in metricas:
                            if metric_select.get_attribute('aria-checked') == 'false':
                                metric_select.click()
                                time.sleep(1)

                        if str(metric_select.text) not in metricas:
                            if metric_select.get_attribute('aria-checked') == 'true':
                                metric_select.click()
                                time.sleep(1)

                    except TimeoutException:
                        print(f"Error: {metricas}, no se encontro en el apartado de Metricas.")
                        raise Exception(f'Error: {metricas}, no se encontro en el apartado de Metricas')
            except Exception as e:
                raise Exception('Error en la Seleccion de Metricas.')

            aplicar_cambios(driver)
            time.sleep(15)

            print("Apartado de Filtros.")
            print("Limpiando apartado de Division.")
            try:
                div_box = driver.find_element(By.XPATH, '//div[@aria-label="División" and @role="combobox"]')
                ActionChains(driver).move_to_element(div_box).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//h3[text()="División"]/parent::div/span').click()
                time.sleep(2)
                print("División limpios.")
                div_box.click()
            except Exception as e:
                print("Error al limpiar los filtros de División.")
                raise Exception('E', f"Error al limpiar los filtros División: {e}.")
            time.sleep(2)

            print(f"Seleccionando todos los División.")
            try:
                time.sleep(5)
                divisiones = driver.find_elements(By.XPATH,
                                                  '//div[@aria-label="División"]//div[@class="row"]/div')

                for division in divisiones:
                    valor_aria = division.get_attribute("aria-selected")
                    if valor_aria == 'false':
                        acciones = ActionChains(driver)
                        division.click()
                        acciones.key_down(Keys.DOWN)
                        acciones.key_up(Keys.DOWN)
                        acciones.perform()
                        time.sleep(1)
            except Exception as e:
                raise Exception('Error en la Seleccion de Dimensiones.')
            time.sleep(15)

            print("Limpiando apartado de Categoría.")
            try:
                categoria = driver.find_element(By.XPATH, '//div[@aria-label="Categoría" and @role="combobox"]')
                ActionChains(driver).move_to_element(categoria).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//h3[text()="Categoría"]/parent::div/span').click()
                time.sleep(2)
                print("Categoría limpios.")
                categoria.click()
            except Exception as e:
                print("Error al limpiar los filtros de Categoría.")
                raise Exception('E', f"Error al limpiar los filtros Categoría: {e}.")
            time.sleep(2)

            print(f"Seleccionando todas los Categoría.")
            try:
                time.sleep(5)
                driver.find_element(By.XPATH,
                                     '//div[@aria-label="Categoría"]//span[text()="Seleccionar todo"]').click()

            except Exception as e:
                raise Exception('Error en la Seleccion de Categoría.')
            time.sleep(15)

            print("Limpiando apartado de Formato.")
            try:
                formato_box = driver.find_element(By.XPATH, '//div[@aria-label="Formato" and @role="combobox"]')
                ActionChains(driver).move_to_element(formato_box).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//h3[text()="Formato"]/parent::div/span').click()
                time.sleep(2)
                print("Formatos limpios.")
                formato_box.click()
            except Exception as e:
                print("Error al limpiar los filtros de Formato.")
                raise Exception('E', f"Error al limpiar los filtros Formato: {e}.")
            time.sleep(2)

            print(f"Seleccionando todos los Formatos.")
            try:

                formatos = driver.find_elements(By.XPATH,
                                                '//div[@aria-label="Formato"]//div[@class="row"]/div')
                for formato in formatos:
                    valor_aria = formato.get_attribute("aria-selected")
                    if valor_aria == 'false':
                        acciones = ActionChains(driver)
                        formato.click()
                        acciones.key_down(Keys.DOWN)
                        acciones.key_up(Keys.DOWN)
                        acciones.perform()
                        time.sleep(1)
            except Exception as e:
                raise Exception('Error en la Seleccion de Dimensiones.')
            time.sleep(15)

            '''
            print("Limpiando apartado de Tienda.")
            try:
                tienda_box = driver.find_element(By.XPATH,
                                                 '//div[@aria-label="No. Nombre Tienda" and @role="combobox"]')
                ActionChains(driver).move_to_element(tienda_box).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//h3[text()="Tienda"]/parent::div/span').click()
                time.sleep(2)
                print("Tiendas limpios.")
                tienda_box.click()
            except Exception as e:
                print("Error al limpiar los filtros de Tienda.")
                raise Exception('E', f"Error al limpiar los filtros Tienda: {e}.")
            time.sleep(2)

            print(f"Seleccionando todos los Tienda.")

            try:
                time.sleep(5)
                bandera = 0
                while True:
                    acciones = ActionChains(driver)
                    tiendas = driver.find_elements(By.XPATH,
                                                   '//div[@aria-label="No. Nombre Tienda"]//div[@class="row"]/div')
                    if bandera == len(tiendas):
                        break

                    for index, tienda in enumerate(tiendas):
                        valor_aria = tienda.get_attribute("aria-selected")

                        if valor_aria == 'true':
                            bandera += 1
                        else:
                            tienda.click()

                        acciones.key_down(Keys.DOWN)
                        acciones.key_up(Keys.DOWN)
                        acciones.perform()
                        time.sleep(1)


            except Exception as e:
                raise Exception('Error en la Seleccion de Tienda.')
            time.sleep(15)
            '''

            print(f'Descargamos Informacion.')
            ActionChains(driver).move_to_element(
                driver.find_element(By.XPATH, '//div[@class="top-viewport"]')  # div de tabla
            ).perform()
            time.sleep(2)

            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//*[@aria-label="Más opciones"]'))).click()

            time.sleep(2)

            print(f'Exportar Datos.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//button[@aria-description="Exportar datos"]'))).click()

            time.sleep(2)

            print(f'Selecciona Datos con diseño actual(xlsx).')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//*[text()="Datos con diseño actual"]/parent::span/preceding-sibling::div/span'))).click()

            time.sleep(5)

            print(f'Exportando archivo.')
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Exportar"]')
            btn_exportar.location_once_scrolled_into_view
            btn_exportar.click()
            time.sleep(5)
            esperar_descarga(dir_fuentes, 1)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            driver.close()
            sys.exit(0)

    if 'inventories' in in_descarga:

        print("Iniciando Descarga Invetories (Inventarios).")

        try:
            print("Entrar Gestion de Proveedores.")

            driver.switch_to.default_content()

            print(f'Apartado Comercial.')

            print(f'Seleccionamos: Indicadores comerciales.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, '//span[text()="Indicadores comerciales"]'))).click()
            time.sleep(2)

            print(f'Elegimos Proveedor: {proveedor}.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, '//input[@placeholder="Proveedor"]'))).click()
            time.sleep(5)

            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//div[text()="{proveedor}"]/parent::div'))).click()
            time.sleep(7)

            assert proveedor in driver.find_element(By.XPATH, '//input[@placeholder="Proveedor"]').get_attribute(
                'value'), "Error Seleccionando usuario dentro del portal."

            print(f'Seleccion de proveedor con exito.')

            iframe_1 = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((
                By.XPATH, '//iframe[@id="__xmlview1--map_iframe"]'))
            )
            driver.switch_to.frame(iframe_1)

            print(f'Seleccionamos: Generacion de reportes.')

            print(f'Seleccionamos: Asistente de generacion  de reportes.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, '//button[contains(text(),"Generaci") and contains(text(),"n de reportes")]'))).click()
            time.sleep(4)
            driver.find_element(By.XPATH,
                                '//*[contains(text(),"Asistente de generaci") and contains(text(),"n  de reportes")]').click()
            driver.switch_to.frame(driver.find_element(By.XPATH, '//div[@class="Embed-container"]/iframe'))
            time.sleep(2)

            print(f'Seleccionamos Mes Anio: {months[mes]} {anio}')
            WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                By.XPATH, '//div[@aria-label="Meses"]')))
            driver.find_element(By.XPATH, '//div[@aria-label="Meses"]').click()
            time.sleep(3)
            try:
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, f'//span[text()="{months[mes]} {anio}"]')))
                mes_dimension = driver.find_element(By.XPATH, f'//span[text()="{months[mes]} {anio}"]/parent::div')
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
                            raise Exception(f'El mes {months[mes]} no se encontro en la lista')
                        accion.key_down(Keys.DOWN)
                        accion.perform()
            except:
                raise Exception(f'El mes {months[mes]} no se encontro en la lista')

            aplicar_cambios(driver)

            try:
                time.sleep(15)
                fecha_inicio = datetime.strptime(in_fechaini, '%d/%m/%Y').strftime('%d/%m/%Y')
                print(f'Rango de fechas a buscar: {fecha_inicio} - {fecha_inicio}.')
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, '//input[contains(@aria-label,"echa de fin")]/parent::div/button'))
                ).click()
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, f'//button[contains(@aria-description, ", {dia}")]'))
                ).click()
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, '//input[contains(@aria-label,"echa de inicio")]/parent::div/button'))
                ).click()
                WebDriverWait(driver, 200).until(EC.visibility_of_element_located((
                    By.XPATH, f'//button[contains(@aria-description, ", {dia}")]'))
                ).click()
            except Exception as e:
                print('Error en la seleccion de Fechas. Bottones', e)
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

            print(f"Verificando fecha seleccionada.")
            if fecha_inicio_seleccionada == fecha_final_seleccionada:
                print("Fechas seleccionadas correctamente.")
            else:
                raise Exception("Fechas seleccionadas NO correctas.")

            aplicar_cambios(driver)

            time.sleep(15)

            print("Apartado de Dimensiones a mostrar.")
            print("Limpiando apartado de Dimensiones a mostrar.")
            try:
                dimension_box = driver.find_element(By.XPATH,
                                                    '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]')
                ActionChains(driver).move_to_element(dimension_box).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//span[@aria-label="Borrar selecciones"]').click()
                time.sleep(2)
                print("Dimensiones a mostrar limpios.")
            except Exception as e:
                print("Error al limpiar los filtros de Dimensiones a mostrar.")
                raise Exception(f"Error al limpiar los filtros Dimensiones a mostrar: {e}.")

            print("Seleccionamos todas las Dimensiones.")
            try:
                driver.find_element(By.XPATH, '//div[@aria-label="Dimensiones a mostrar" and @role="combobox"]').click()
                time.sleep(5)
                dimensiones = driver.find_elements(By.XPATH,
                                                   '//div[@aria-label="Dimensiones a mostrar"]//div[@class="row"]/div')
                for dimencion in dimensiones:
                    valor_aria = dimencion.get_attribute("aria-selected")
                    if valor_aria == 'false':
                        time.sleep(1)
                        acciones = ActionChains(driver)
                        dimencion.click()
                        acciones.key_down(Keys.DOWN)
                        acciones.key_up(Keys.DOWN)
                        acciones.perform()

            except Exception as e:
                raise Exception('Error en la Seleccion de Dimensiones.')

            metricas = ['Inventario (Actual)']
            print(f"Seleccionamos las Metricas: {metricas}.")
            try:
                metricas_select = driver.find_elements(By.XPATH, '//div[@aria-label="Metricas"]//div[@class="row"]/div')

                for metric_select in metricas_select:
                    try:

                        if str(metric_select.text) in metricas:
                            if metric_select.get_attribute('aria-checked') == 'false':
                                metric_select.click()
                                time.sleep(1)

                        if str(metric_select.text) not in metricas:
                            if metric_select.get_attribute('aria-checked') == 'true':
                                metric_select.click()
                                time.sleep(1)

                    except TimeoutException:
                        print(f"Error: {metricas}, no se encontro en el apartado de Metricas.")
                        raise Exception(f'Error: {metricas}, no se encontro en el apartado de Metricas')
            except Exception as e:
                raise Exception('Error en la Seleccion de Metricas.')
            time.sleep(5)

            aplicar_cambios(driver)

            time.sleep(15)

            print("Apartado de Filtros.")
            print("Limpiando apartado de Division.")
            try:
                div_box = driver.find_element(By.XPATH, '//div[@aria-label="División" and @role="combobox"]')
                ActionChains(driver).move_to_element(div_box).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//h3[text()="División"]/parent::div/span').click()
                time.sleep(2)
                print("División limpios.")
                div_box.click()
            except Exception as e:
                print("Error al limpiar los filtros de División.")
                raise Exception('E', f"Error al limpiar los filtros División: {e}.")
            time.sleep(2)

            print(f"Seleccionando todos los División.")
            try:
                time.sleep(5)
                divisiones = driver.find_elements(By.XPATH,
                                                  '//div[@aria-label="División"]//div[@class="row"]/div')

                for division in divisiones:
                    valor_aria = division.get_attribute("aria-selected")
                    if valor_aria == 'false':
                        acciones = ActionChains(driver)
                        division.click()
                        acciones.key_down(Keys.DOWN)
                        acciones.key_up(Keys.DOWN)
                        acciones.perform()
                        time.sleep(1)
            except Exception as e:
                raise Exception('Error en la Seleccion de Dimensiones.')
            time.sleep(15)

            print("Limpiando apartado de Categoría.")
            try:
                categoria = driver.find_element(By.XPATH, '//div[@aria-label="Categoría" and @role="combobox"]')
                ActionChains(driver).move_to_element(categoria).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//h3[text()="Categoría"]/parent::div/span').click()
                time.sleep(2)
                print("Categoría limpios.")
                categoria.click()
            except Exception as e:
                print("Error al limpiar los filtros de Categoría.")
                raise Exception('E', f"Error al limpiar los filtros Categoría: {e}.")
            time.sleep(2)

            print(f"Seleccionando todas los Categoría.")
            try:
                time.sleep(5)
                driver.find_element(By.XPATH,
                                    '//div[@aria-label="Categoría"]//span[text()="Seleccionar todo"]').click()

            except Exception as e:
                raise Exception('Error en la Seleccion de Categoría.')
            time.sleep(15)

            print("Limpiando apartado de Formato.")
            try:
                formato_box = driver.find_element(By.XPATH, '//div[@aria-label="Formato" and @role="combobox"]')
                ActionChains(driver).move_to_element(formato_box).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//h3[text()="Formato"]/parent::div/span').click()
                time.sleep(2)
                print("Formatos limpios.")
                formato_box.click()
            except Exception as e:
                print("Error al limpiar los filtros de Formato.")
                raise Exception('E', f"Error al limpiar los filtros Formato: {e}.")
            time.sleep(2)

            print(f"Seleccionando todos los Formatos.")
            try:

                formatos = driver.find_elements(By.XPATH,
                                                '//div[@aria-label="Formato"]//div[@class="row"]/div')
                for formato in formatos:
                    valor_aria = formato.get_attribute("aria-selected")
                    if valor_aria == 'false':
                        acciones = ActionChains(driver)
                        formato.click()
                        acciones.key_down(Keys.DOWN)
                        acciones.key_up(Keys.DOWN)
                        acciones.perform()
                        time.sleep(1)
            except Exception as e:
                raise Exception('Error en la Seleccion de Dimensiones.')
            time.sleep(15)

            '''
            print("Limpiando apartado de Tienda.")
            try:
                tienda_box = driver.find_element(By.XPATH,
                                                 '//div[@aria-label="No. Nombre Tienda" and @role="combobox"]')
                ActionChains(driver).move_to_element(tienda_box).perform()
                time.sleep(2)
                driver.find_element(By.XPATH, '//h3[text()="Tienda"]/parent::div/span').click()
                time.sleep(2)
                print("Tiendas limpios.")
                tienda_box.click()
            except Exception as e:
                print("Error al limpiar los filtros de Tienda.")
                raise Exception('E', f"Error al limpiar los filtros Tienda: {e}.")
            time.sleep(2)

            print(f"Seleccionando todos los Tienda.")

            try:
                time.sleep(5)
                bandera = 0
                while True:
                    acciones = ActionChains(driver)
                    tiendas = driver.find_elements(By.XPATH,
                                                   '//div[@aria-label="No. Nombre Tienda"]//div[@class="row"]/div')
                    if bandera == len(tiendas):
                        break

                    for index, tienda in enumerate(tiendas):
                        valor_aria = tienda.get_attribute("aria-selected")

                        if valor_aria == 'true':
                            bandera += 1
                        else:
                            tienda.click()

                        acciones.key_down(Keys.DOWN)
                        acciones.key_up(Keys.DOWN)
                        acciones.perform()
                        time.sleep(1)


            except Exception as e:
                raise Exception('Error en la Seleccion de Tienda.')
            time.sleep(15)
            '''

            print(f'Descargamos Informacion.')
            ActionChains(driver).move_to_element(
                driver.find_element(By.XPATH, '//div[@class="top-viewport"]')  # div de tabla
            ).perform()
            time.sleep(2)

            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//*[@aria-label="Más opciones"]'))).click()

            time.sleep(2)

            print(f'Exportar Datos.')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//button[@aria-description="Exportar datos"]'))).click()

            time.sleep(2)

            print(f'Selecciona Datos con diseño actual(xlsx).')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((
                By.XPATH, f'//*[text()="Datos con diseño actual"]/parent::span/preceding-sibling::div/span'))).click()

            time.sleep(5)

            print(f'Exportando archivo.')
            btn_exportar = driver.find_element(By.XPATH, '//button[text()="Exportar"]')
            btn_exportar.location_once_scrolled_into_view
            btn_exportar.click()
            time.sleep(5)
            esperar_descarga(dir_fuentes, 1)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            driver.close()
            sys.exit(0)


def transformar_archivo(dir_fuentes, dir_temp, in_fechaini, in_descarga, in_variables):

    if 'fill_rate' in in_descarga:

        try:

            name_file = f'{in_variables}_{in_variables}.csv'
            file_path = os.path.join(dir_fuentes, name_file)

            archivos = [name for name in os.listdir(dir_fuentes)]

            for archivo in archivos:
                df = pd.read_excel(f'{dir_fuentes}/{archivo}', dtype=str)
                df = df.fillna(0)
                df = df[(df['Tienda'] != 'Total')]
                df = df[(df['Formato'] != 0)]

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

                    if columna == 'Codigo de barras':
                        df[f'CA {columna}'] = df[columna].apply(lambda x: int(str(x).split('-')[0].strip()))
                        df[f'CA Producto'] = df[columna].apply(
                            lambda x: str(x).split('-')[1].replace('            ', ' ').strip())

                    if columna == 'Material':
                        df[f'CA Codigo {columna}'] = df[columna].apply(lambda x: int(str(x).split('-')[0].strip()))
                        df[f'CA Decripcion {columna}'] = df[columna].apply(
                            lambda x: str(x).replace('            ', ' ').split('-')[1].strip())

                    if columna == 'Tienda':
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

        print("proceso de transformación finalizado")
        borrar_archivo(dir_temp)





def esperar_descarga(dir_fuentes, can_arch):
    print("Esperando descarga")
    t1 = datetime.now()
    archivos = [name for name in os.listdir(dir_fuentes) if name.endswith(".txt") or name.endswith(".xlsx") or name.endswith(".csv") or name.endswith(".xls")]
    #de 600 aumenté a 1200
    while len(archivos) < can_arch and (datetime.now()-t1).seconds <= 1200:
        time.sleep(5)
        archivos = [name for name in os.listdir(dir_fuentes) if name.endswith(".txt") or name.endswith(".xlsx") or name.endswith(".csv") or name.endswith(".xls")]
    if any(File.endswith(".txt") or File.endswith(".csv") or File.endswith(".xlsx")  or File.endswith(".xls") for File in os.listdir(dir_fuentes)):
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
    busqueda_reportes(driver, dir_fuentes, args.in_fechaini, args.in_descarga, args.in_variables)
    transformar_archivo(dir_fuentes, dir_temp, args.in_fechaini, args.in_descarga, args.in_variables)
    print('Tarea Finalizada Ok')



if __name__ == "__main__":
    main()

