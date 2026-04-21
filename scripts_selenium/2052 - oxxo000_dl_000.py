"""
-------------------------------------------------------
Script que automatiza el proceso "Oxxo"

2020 Ciudad de México, México
Todos los derechos pertenecen a Realmetrics
-------------------------------------------------------
-------------------------------------------------------
Parameters:
-in_direccion
https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf
-in_usuario
maria.pedroche@redbull.com|adrian.garza@redbull.com
-in_password
Redbull2022.|Lenon99345.,
-in_fechaini
09/05/2022
-in_mensaje_tipo
jvalle1_oxxoits_dl
-in_variables
1

-------------------------------------------------------
ITS_dl
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 07/06/2023 -in_mensaje_tipo monde01_oxxoits_dl -in_variables 0,1

VS
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 10/04/2023 -in_mensaje_tipo jvalle1_oxxo0vs_dl -in_variables 1

MVS
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 01/06/2023 -in_mensaje_tipo jvalle1_oxxo0mvs_dl -in_variables 1

MVM
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 02/05/2023 -in_mensaje_tipo jvalle1_oxxomvm_dl -in_variables 1

TVM
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 02/05/2023 -in_mensaje_tipo jvalle1_oxxotvm_dl -in_variables 1

IC_DR
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 14/06/2023 -in_mensaje_tipo jvalle1_oxxo0ic_dl -in_variables 1

IS_DL
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 25/05/2023 -in_mensaje_tipo jvalle1_oxxo0is_dl -in_variables 1

MIS_DL
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 31/05/2023 -in_mensaje_tipo jvalle1_oxxomis_dl -in_variables 1

TIM_DL
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 04/06/2023 -in_mensaje_tipo jvalle1_oxxotim_dl -in_variables 1

MIM_DL
-in_direccion https://proveedoresfemco.oxxo.com/AccessControl/pages/login_form.jsf -in_usuario LUIS.NAVA@MDLZ.COM -in_password MdlzJello35 -in_fechaini 04/06/2023 -in_mensaje_tipo jvalle1_oxxomim_dl -in_variables 1


"""

from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains
from selenium import webdriver
from datetime import date, timedelta
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
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


def comprimir_a_backup(dir_fuentes, dir_backup, in_mensaje_tipo, in_fechaini):
    fecha = in_fechaini.split('/')[2] + in_fechaini.split('/')[1] + in_fechaini.split('/')[0]
    shutil.make_archive(dir_backup + in_mensaje_tipo + '_' + fecha, 'zip', dir_fuentes)


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


def escritura_traza(path_file, in_transaccion, mensaje):
    print(mensaje)


def escritura_estado(path_file,in_transaccion,mensaje):
    print('ER_CODE:' + mensaje.split('_')[0] + '; ER_MSG:' + mensaje.split('_')[1] + ';')


def limpiar(path_data, dir_destino, dir_temp, dir_backup, dir_fuentes):
    carpetas = [path_data, dir_destino, dir_temp, dir_backup, dir_fuentes]
    for c in carpetas:
        f = [name for name in os.listdir(c)]
        for file in f:
            os.remove(c + file)


def setDriver(address, path, path_file, in_transaccion):
    escritura_traza(path_file, in_transaccion, 'Inicio Ejecucion')
    chrome_options = Options()
    if os.name != "nt":
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--window-size=1024,768")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("-allow-running-insecure-content")
    prefs = {"download.default_directory" : path}
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36")
    escritura_traza(path_file, in_transaccion, "Iniciando driver")

    if os.name == "nt":
        driver = webdriver.Chrome(chrome_options=chrome_options,
                                  executable_path='C:\\chromedriver.exe')
    else:
        driver = webdriver.Chrome(chrome_options=chrome_options)

    print("Driver listo")
    print("Accediendo a {}".format(address))

    try:
        print('Accediendo al portal')
        driver.get(address)
        time.sleep(10)
    except:
        print('Error al acceder al portal')
        escritura_estado(path_file, in_transaccion, '910_Error al acceder al portal')
        sys.exit(0)
    #driver.save_screenshot("screen1.png")
    return driver


def iniciar_sesion(driver, username_text, password_text, path_file, in_transaccion):

    try:

        username = driver.find_element_by_id('login:userName')
        password = driver.find_element_by_id('login:userPass')

        username.send_keys(username_text)
        password.send_keys(password_text)
        time.sleep(2)

        driver.find_element_by_id('login:loginButton').click()
        time.sleep(5)

    except Exception as e:
        print(e)
        print('Finalizado Error de Acceso')
        escritura_traza(path_file, in_transaccion, 'Finalizado Error de Acceso zona segura')
        escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
        driver.close()
        sys.exit(0)

    try:
        driver.find_element_by_xpath('//*[contains(text(), "Usuario y/o Contraseña Invalidos")]')
        print('Usuario o Pass Incorrectos.')
        escritura_traza(path_file, in_transaccion, 'Finalizado Error de Acceso')
        escritura_estado(path_file, in_transaccion, '999_Finalizado Error de Acceso')
        driver.quit()
        sys.exit(0)
    except:
        try:
            driver.find_element_by_xpath('//*[contains(text(), "Usuario bloqueado")]')
            print('Usuario Bloqueado.')
            escritura_traza(path_file, in_transaccion, 'Finalizado Error de Acceso')
            escritura_estado(path_file, in_transaccion, '999_Finalizado Error de Acceso')
            driver.quit()
            sys.exit(0)
        except:
            try:
                driver.find_element_by_xpath('//*[contains(text(), "La contraseña expiró, es necesario cambiarla")]')
                print('Pass Expiro, es necesario Cambiarlo.')
                escritura_traza(path_file, in_transaccion, 'Finalizado Error de Acceso')
                escritura_estado(path_file, in_transaccion, '999_Finalizado Error de Acceso')
                driver.quit()
                sys.exit(0)
            except:
                try:
                    driver.find_element_by_xpath('//*[contains(text(), "Proveedores")]')
                    escritura_traza(path_file, in_transaccion, 'Acceso Correcto')
                    time.sleep(5)
                    return driver
                except:
                    driver.quit()
                    sys.exit(0)


def busqueda_reportes(driver, dir_fuentes, in_fechaini, in_mensaje_tipo, path_file, in_transaccion, in_variables):
    print("Busqueda de Reporte")

    if 'vs_dl' in in_mensaje_tipo and 'mvs_dl' not in in_mensaje_tipo:

        print("Descarga Ventas Semanales Totales Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            ventas = driver.find_element_by_xpath('//*[contains(text(), "Ventas")]')
            ActionChains(driver).move_to_element(ventas).perform()
            time.sleep(3)

            print("Entrar Desempeno de Productos.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Desempe¿o de productos")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Semanal")]').click()
            time.sleep(1)

            print('Comprobando Anio-Semana.') #Comprueba si el jueves cae en el mismo año, si no se cambia el año al año anterior

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y')  # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            jueves = anterior + timedelta(days=3)
            separar = str(anterior).split(' ')[0].split('-')
            fecha_semana = separar[2] + '/' + separar[1] + '/' + separar[0]
            jueves_anio = str(jueves).split(' ')[0].split('-')[0]

            print("Seleccion Anio.")

            if jueves_anio !=  anio : anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)
            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)

            print("Seleccionando Semana.")

            mover_semana = driver.find_element_by_xpath("//li[contains(text(),'" + fecha_semana + "')]")
            ActionChains(driver).move_to_element(mover_semana).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Totales.")

            driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Totales")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:
                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'mvs_dl' in in_mensaje_tipo:

        print("Descarga Ventas Semanales Maduras Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            ventas = driver.find_element_by_xpath('//*[contains(text(), "Ventas")]')
            ActionChains(driver).move_to_element(ventas).perform()
            time.sleep(3)

            print("Entrar Desempeno de Productos.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Desempe¿o de productos")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
            time.sleep(2)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Semanal")]').click()
            time.sleep(1)

            print('Comprobando Anio-Semana.') #Comprueba si el jueves cae en el mismo año, si no se cambia el año al año anterior

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y')  # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            jueves = anterior + timedelta(days=3)
            separar = str(anterior).split(' ')[0].split('-')
            fecha_semana = separar[2] + '/' + separar[1] + '/' + separar[0]
            jueves_anio = str(jueves).split(' ')[0].split('-')[0]

            print("Seleccion Anio.")

            if jueves_anio != anio : anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)
            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)

            print("Seleccion Semana.")

            mover_semana = driver.find_element_by_xpath("//li[contains(text(),'" + fecha_semana + "')]")
            ActionChains(driver).move_to_element(mover_semana).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Totales")

            driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Maduras")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 900)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except TimeoutException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            send_email(driver, 'monitoreo_oxxoMVS', 'monitoreo_oxxoMVS')
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'tvm_dl' in in_mensaje_tipo:

        print("Descarga Ventas Mensuales Totales Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:

            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)
            ventas = driver.find_element_by_xpath('//*[contains(text(), "Ventas")]')
            ActionChains(driver).move_to_element(ventas).perform()
            time.sleep(3)

            print("Entrar Desempeno de Productos.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Desempe¿o de productos")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")
            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Mensual")]').click()
            time.sleep(1)

            print("Seleccion Anio.")

            if mes == '01': anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)

            print("Seleccion Mes.")

            dic_meses = {'02': 'Enero', '03': 'Febrero', '04': 'Marzo', '05': 'Abril', '06': 'Mayo', '07': 'Junio',
                         '08': 'Julio', '09': 'Agosto', '10': 'Septiembre', '11': 'Octubre', '12': 'Noviembre',
                         '01': 'Diciembre'}
            month = dic_meses[mes]
            mover_mes = driver.find_element_by_xpath("//li[contains(text(),'" + month + "')]")
            ActionChains(driver).move_to_element(mover_mes).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Totales.")

            driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Totales")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')

                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:

            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'mvm_dl' in in_mensaje_tipo:

        print("Descarga Ventas Mensuales Maduras Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            ventas = driver.find_element_by_xpath('//*[contains(text(), "Ventas")]')
            ActionChains(driver).move_to_element(ventas).perform()
            time.sleep(3)

            print("Entrar Desempeno de Productos.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Desempe¿o de productos")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Mensual")]').click()
            time.sleep(1)

            print("Seleccion Anio.")

            if mes == '01':
                anio = str(int(anio) - 1)
            else:
                anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)

            print("Seleccion Mes.")

            dic_meses = {'02': 'Enero', '03': 'Febrero', '04': 'Marzo', '05': 'Abril', '06': 'Mayo', '07': 'Junio',
                         '08': 'Julio', '09': 'Agosto', '10': 'Septiembre', '11': 'Octubre', '12': 'Noviembre',
                         '01': 'Diciembre'}
            month = dic_meses[mes]
            mover_mes = driver.find_element_by_xpath("//li[contains(text(),'" + month + "')]")
            ActionChains(driver).move_to_element(mover_mes).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Maduras.")
            driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Maduras")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'is_dl' in in_mensaje_tipo and 'mis_dl' not in in_mensaje_tipo:

            print("Descarga Inventarios Semanales Totales Plaza.")

            fecha = in_fechaini.split('/')
            anio = fecha[2]
            mes = fecha[1]
            dia = str(int(fecha[0]))

            try:
                print("Entrar Gestion de Proveedores.")

                scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
                scroll_ventas.click()
                time.sleep(5)

                abasto = driver.find_element_by_xpath('//*[contains(text(), "Abasto")]')
                ActionChains(driver).move_to_element(abasto).perform()
                time.sleep(3)

                print("Entrar Niveles de Inventario.")

                reporte = driver.find_element_by_xpath('//*[contains(text(), "Niveles de inventario")]')
                reporte.click()
                time.sleep(10)

                print("Seleccion. Metricas.")

                iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
                driver.switch_to.frame(iframe)
                print("Seleccion Reporte.")
                driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
                time.sleep(2)
                driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
                time.sleep(10)
                print("Seleccion Granularidad.")
                driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
                time.sleep(3)
                driver.find_element_by_xpath('//li[contains(text(), "Semanal")]').click()
                time.sleep(3)

                print('Comprobando Anio-Semana.')  # Comprueba si el jueves cae en el mismo año, si no se cambia el año al año anterior

                dt = datetime.strptime(in_fechaini, '%d/%m/%Y')  # vuelve in_fechaini en fecha
                start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
                anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
                jueves = anterior + timedelta(days=3)
                separar = str(anterior).split(' ')[0].split('-')
                fecha_semana = separar[2] + '/' + separar[1] + '/' + separar[0]
                jueves_anio = str(jueves).split(' ')[0].split('-')[0]

                print("Seleccion Anio.")

                if jueves_anio != anio: anio = str(int(anio) - 1)
                else: anio = anio

                driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
                time.sleep(2)
                Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
                ActionChains(driver).move_to_element(Periodo_anio).click().perform()
                time.sleep(2)
                driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
                time.sleep(2)

                print("Seleccion Semana.")

                mover_semana = driver.find_element_by_xpath("//li[contains(text(),'" + fecha_semana + "')]")
                ActionChains(driver).move_to_element(mover_semana).click().perform()
                time.sleep(5)

                print("Seleccion Tiendas Totales.")

                driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
                time.sleep(2)
                driver.find_element_by_xpath('//li[contains(text(), "Tiendas Totales")]').click()
                time.sleep(1)

                try:

                    print("Boton Buscar.")

                    driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                    wait = WebDriverWait(driver, 450)
                    print('Esperando Archivo.')
                    icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                    icono_excel.click()
                    esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

                except NoSuchElementException:

                    print('Tiempo de Espera Expirado.')
                    escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                    driver.close()
                    sys.exit(0)

            except Exception as e:
                print(e)
                print('Finalizado Error en Zona de descarga.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

    elif 'mis_dl' in in_mensaje_tipo:

        print("Descarga Inventarios Semanales Maduras Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            abasto = driver.find_element_by_xpath('//*[contains(text(), "Abasto")]')
            ActionChains(driver).move_to_element(abasto).perform()
            time.sleep(3)

            print("Entrar Niveles de Inventario.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Niveles de inventario")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Semanal")]').click()
            time.sleep(1)

            print('Comprobando Anio-Semana.')  # Comprueba si el jueves cae en el mismo año, si no se cambia el año al año anterior

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y')  # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            jueves = anterior + timedelta(days=3)
            separar = str(anterior).split(' ')[0].split('-')
            fecha_semana = separar[2] + '/' + separar[1] + '/' + separar[0]
            jueves_anio = str(jueves).split(' ')[0].split('-')[0]

            print("Seleccion Anio.")

            if jueves_anio != anio: anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(3)
            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)

            print("Seleccion Semana.")

            mover_semana = driver.find_element_by_xpath("//li[contains(text(),'" + fecha_semana + "')]")
            ActionChains(driver).move_to_element(mover_semana).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Maduras.")

            driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Maduras")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'tim_dl' in in_mensaje_tipo:

        print("Descarga Inventarios Mensuales Totales Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            abasto = driver.find_element_by_xpath('//*[contains(text(), "Abasto")]')
            ActionChains(driver).move_to_element(abasto).perform()
            time.sleep(3)

            print("Entrar Niveles de Inventario.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Niveles de inventario")]')
            reporte.click()
            time.sleep(20)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Mensual")]').click()
            time.sleep(1)

            print("Seleccion Anio.")

            if mes == '01':
                anio = str(int(anio) - 1)
            else:
                anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)

            print("Seleccion Mes.")

            dic_meses = {'02': 'Enero', '03': 'Febrero', '04': 'Marzo', '05': 'Abril', '06': 'Mayo', '07': 'Junio',
                         '08': 'Julio', '09': 'Agosto', '10': 'Septiembre', '11': 'Octubre', '12': 'Noviembre',
                         '01': 'Diciembre'}
            month = dic_meses[mes]
            mover_mes = driver.find_element_by_xpath("//li[contains(text(),'" + month + "')]")
            ActionChains(driver).move_to_element(mover_mes).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Totales.")

            driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Totales")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')

                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:

            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'mim_dl' in in_mensaje_tipo:

        print("Descarga Inventarios Mensuales Maduras Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            abasto = driver.find_element_by_xpath('//*[contains(text(), "Abasto")]')
            ActionChains(driver).move_to_element(abasto).perform()
            time.sleep(3)

            print("Entrar Niveles de Inventario.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Niveles de inventario")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            print("Seleccion Reportes.")
            driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Mensual")]').click()
            time.sleep(1)

            print("Seleccion Anio.")

            if mes == '01':
                anio = str(int(anio) - 1)
            else:
                anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)

            print("Seleccion Mes.")

            dic_meses = {'02': 'Enero', '03': 'Febrero', '04': 'Marzo', '05': 'Abril', '06': 'Mayo', '07': 'Junio',
                         '08': 'Julio', '09': 'Agosto', '10': 'Septiembre', '11': 'Octubre', '12': 'Noviembre',
                         '01': 'Diciembre'}
            month = dic_meses[mes]
            mover_mes = driver.find_element_by_xpath("//li[contains(text(),'" + month + "')]")
            ActionChains(driver).move_to_element(mover_mes).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Maduras.")

            driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Maduras")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'ic_dl' in in_mensaje_tipo:

        print("Descarga Inventarios Existentes Maduras Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            abasto = driver.find_element_by_xpath('//*[contains(text(), "Abasto")]')
            ActionChains(driver).move_to_element(abasto).perform()
            time.sleep(3)

            print("Entrar Inventario Existente.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Inventario existente")]')
            reporte.click()
            time.sleep(10)

            print("Seleccionando Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)

            print("Seleccionando Anio")

            driver.find_element_by_xpath('//*[@id="purchase-order-report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)

            print("Seleccion Mes.")

            dic_meses = {'01': 'Enero', '02': 'Febrero', '03': 'Marzo', '04': 'Abril', '05': 'Mayo', '06': 'Junio',
                         '07': 'Julio', '08': 'Agosto', '09': 'Septiembre', '10': 'Octubre', '11': 'Noviembre',
                         '12': 'Diciembre'}
            month = dic_meses[mes]
            driver.find_element_by_xpath('//*[@id="purchase-order-report-form:month"]').click()
            time.sleep(3)
            mover_mes = driver.find_element_by_xpath("//li[contains(text(),'" + month + "')]")
            ActionChains(driver).move_to_element(mover_mes).click().perform()
            time.sleep(5)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath("//span[contains(text(),'Buscar')]").click()
                time.sleep(30)
                print('Esperando Archivo.')
                icono_excel = driver.find_element_by_xpath('//*[@class="search-icon"]').click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'st_dl' in in_mensaje_tipo and 'mst_dl' not in in_mensaje_tipo:

        print("Descarga Stockout Semanales Totales Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            ventas = driver.find_element_by_xpath('//*[contains(text(), "Ventas")]')
            ActionChains(driver).move_to_element(ventas).perform()
            time.sleep(3)

            print("Entrar Stockout y Venta Perdida.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Stockout y venta perdida")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            driver.find_element_by_xpath('//*[@id="report-form:options_label"]').click()
            time.sleep(2)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Semanal")]').click()
            time.sleep(1)

            print('Comprobando Anio-Semana.')  # Comprueba si el jueves cae en el mismo año, si no se cambia el año al año anterior

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y')  # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            jueves = anterior + timedelta(days=3)
            separar = str(anterior).split(' ')[0].split('-')
            fecha_semana = separar[2] + '/' + separar[1] + '/' + separar[0]
            jueves_anio = str(jueves).split(' ')[0].split('-')[0]

            print("Seleccion Anio.")

            if jueves_anio != anio: anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)
            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)

            print("Seleccion Semana.")

            mover_semana = driver.find_element_by_xpath("//li[contains(text(),'" + fecha_semana + "')]")
            ActionChains(driver).move_to_element(mover_semana).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Totales")

            driver.find_element_by_xpath('//*[@id="report-form:tienda_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Totales")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'mst_dl' in in_mensaje_tipo and 'msm' not in in_mensaje_tipo:

        print("Descarga Stockout Semanales Maduras Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            ventas = driver.find_element_by_xpath('//*[contains(text(), "Ventas")]')
            ActionChains(driver).move_to_element(ventas).perform()
            time.sleep(3)

            print("Entrar Stockout y Venta Perdida.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Stockout y venta perdida")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            driver.find_element_by_xpath('//*[@id="report-form:options_label"]').click()
            time.sleep(2)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Semanal")]').click()
            time.sleep(1)

            print('Comprobando Anio-Semana.')  # Comprueba si el jueves cae en el mismo año, si no se cambia el año al año anterior

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y')  # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            jueves = anterior + timedelta(days=3)
            separar = str(anterior).split(' ')[0].split('-')
            fecha_semana = separar[2] + '/' + separar[1] + '/' + separar[0]
            jueves_anio = str(jueves).split(' ')[0].split('-')[0]

            print("Seleccion Anio.")

            if jueves_anio != anio: anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)
            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)

            print("Seleccion Semana.")

            mover_semana = driver.find_element_by_xpath("//li[contains(text(),'" + fecha_semana + "')]")
            ActionChains(driver).move_to_element(mover_semana).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Maduras")

            driver.find_element_by_xpath('//*[@id="report-form:tienda_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Maduras")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'omt_dl' in in_mensaje_tipo:

        print("Descarga Stockout Mensuales Totales Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            ventas = driver.find_element_by_xpath('//*[contains(text(), "Ventas")]')
            ActionChains(driver).move_to_element(ventas).perform()
            time.sleep(3)

            print("Entrar Stockout y Venta Perdida.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Stockout y venta perdida")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            driver.find_element_by_xpath('//*[@id="report-form:options_label"]').click()
            time.sleep(2)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Mensual")]').click()
            time.sleep(1)

            print("Seleccion Anio.")

            if mes == '01': anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)

            print("Seleccion Mes.")

            dic_meses = {'02': 'Enero', '03': 'Febrero', '04': 'Marzo', '05': 'Abril', '06': 'Mayo', '07': 'Junio',
                         '08': 'Julio', '09': 'Agosto', '10': 'Septiembre', '11': 'Octubre', '12': 'Noviembre',
                         '01': 'Diciembre'}
            month = dic_meses[mes]
            prueba = driver.find_element_by_xpath('//*[@id="report-form:months"]')
            ActionChains(driver).move_to_element(prueba)
            print(prueba)
            mover_mes = driver.find_element_by_xpath("//li[contains(text(),'" + month + "')]")
            ActionChains(driver).move_to_element(mover_mes).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Totales")

            driver.find_element_by_xpath('//*[@id="report-form:tienda_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Totales")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'omm_dl' in in_mensaje_tipo:

        print("Descarga Stockout VP Mensuales Maduras Plaza.")

        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:
            print("Entrar Gestion de Proveedores.")

            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            ventas = driver.find_element_by_xpath('//*[contains(text(), "Ventas")]')
            ActionChains(driver).move_to_element(ventas).perform()
            time.sleep(3)

            print("Entrar Stockout y Venta Perdida.")

            reporte = driver.find_element_by_xpath('//*[contains(text(), "Stockout y venta perdida")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")

            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)
            driver.find_element_by_xpath('//*[@id="report-form:options_label"]').click()
            time.sleep(2)
            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//li[contains(text(), "Plaza")]').click()
            time.sleep(1)
            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Mensual")]').click()
            time.sleep(1)

            print("Seleccion Anio.")

            if mes == '01': anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(2)

            print("Seleccion Mes.")

            dic_meses = {'02': 'Enero', '03': 'Febrero', '04': 'Marzo', '05': 'Abril', '06': 'Mayo', '07': 'Junio',
                         '08': 'Julio', '09': 'Agosto', '10': 'Septiembre', '11': 'Octubre', '12': 'Noviembre',
                         '01': 'Diciembre'}
            month = dic_meses[mes]
            prueba = driver.find_element_by_xpath('//*[@id="report-form:months"]')
            ActionChains(driver).move_to_element(prueba)
            print(prueba)
            mover_mes = driver.find_element_by_xpath("//li[contains(text(),'" + month + "')]")
            ActionChains(driver).move_to_element(mover_mes).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Maduras")

            driver.find_element_by_xpath('//*[@id="report-form:tienda_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Maduras")]').click()
            time.sleep(1)

            try:

                print("Boton Buscar.")

                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                wait = WebDriverWait(driver, 450)
                print('Esperando Archivo.')
                icono_excel = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@class="search-icon"]')))
                icono_excel.click()
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:
            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)

    elif 'its_dl' in in_mensaje_tipo:

        print("Descarga Inventarios Semanales Totales Tienda.")
        fecha = in_fechaini.split('/')
        anio = fecha[2]
        mes = fecha[1]
        dia = str(int(fecha[0]))

        try:

            print("Entrar Gestion de Proveedores.")
            scroll_ventas = driver.find_element_by_xpath('//*[contains(text(), "Gestión de Proveedores")]')
            scroll_ventas.click()
            time.sleep(5)

            abasto = driver.find_element_by_xpath('//*[contains(text(), "Abasto")]')
            ActionChains(driver).move_to_element(abasto).perform()
            time.sleep(3)

            print("Entrar Niveles de Inventario.")
            reporte = driver.find_element_by_xpath('//*[contains(text(), "Niveles de inventario")]')
            reporte.click()
            time.sleep(10)

            print("Seleccion Metricas.")
            iframe = driver.find_element_by_xpath('//*[@id="contentFrame"]/iframe')
            driver.switch_to.frame(iframe)

            print("Seleccion Reporte.")
            driver.find_element_by_xpath('//*[@id="report-form:options"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tienda")]').click()
            time.sleep(1)

            print("Seleccion Granularidad.")
            driver.find_element_by_xpath('//*[@id="report-form:period_label"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Semanal")]').click()
            time.sleep(1)

            print('Comprobando Anio-Semana.')  # Comprueba si el jueves cae en el mismo año, si no se cambia el año al año anterior
            dt = datetime.strptime(in_fechaini, '%d/%m/%Y')  # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            jueves = anterior + timedelta(days=3)
            separar = str(anterior).split(' ')[0].split('-')
            fecha_semana = separar[2] + '/' + separar[1] + '/' + separar[0]
            jueves_anio = str(jueves).split(' ')[0].split('-')[0]

            print("Seleccion Anio.")
            if jueves_anio != anio: anio = str(int(anio) - 1)
            else: anio = anio

            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)
            Periodo_anio = driver.find_element_by_xpath("//li[contains(text(),'" + anio + "')]")
            ActionChains(driver).move_to_element(Periodo_anio).click().perform()
            time.sleep(3)
            driver.find_element_by_xpath('//*[@id="report-form:year_label"]').click()
            time.sleep(2)

            print("Seleccion Semana.")
            mover_semana = driver.find_element_by_xpath("//li[contains(text(),'" + fecha_semana + "')]")
            ActionChains(driver).move_to_element(mover_semana).click().perform()
            time.sleep(5)

            print("Seleccion Tiendas Totales.")
            driver.find_element_by_xpath('//*[@id="report-form:tienda"]').click()
            time.sleep(2)
            driver.find_element_by_xpath('//li[contains(text(), "Tiendas Totales")]').click()
            time.sleep(1)

            try:

                print("Boton Exportat txt.")
                driver.find_element_by_xpath('//*[@class="ui-button-text ui-c"]').click()

                print('Esperando Archivo.')
                esperar_descarga(driver, dir_fuentes, 1, path_file, in_transaccion)

            except NoSuchElementException:

                print('Tiempo de Espera Expirado.')
                escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
                driver.close()
                sys.exit(0)

        except Exception as e:

            print(e)
            print('Finalizado Error en Zona de descarga.')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            driver.close()
            sys.exit(0)


def transformar_archivo(dir_fuentes, dir_temp, dir_destino, in_fechaini, in_transaccion, path_file):

    fecha = in_fechaini.split('/')
    year = fecha[2]
    month = fecha[1]
    day = fecha[0]

    id_file = in_transaccion + '_' + year + month + day

    dic_semanas = {'21/12/2020':'52',	'04/04/2022':'15',	'24/07/2023':'31',	'18/11/2024':'47',
                    '28/12/2020':'53',	'11/04/2022':'16',	'31/07/2023':'32',	'25/11/2024':'48',
                    '01/01/2020':'1',	'18/04/2022':'17',	'07/08/2023':'33',	'02/12/2024':'49',
                    '04/01/2021':'2',	'25/04/2022':'18',	'14/08/2023':'34',	'09/12/2024':'50',
                    '11/01/2021':'3',	'02/05/2022':'19',	'21/08/2023':'35',	'16/12/2024':'51',
                    '18/01/2021':'4',	'09/05/2022':'20',	'28/08/2023':'36',	'23/12/2024':'52',
                    '25/01/2021':'5',	'16/05/2022':'21',	'04/09/2023':'37',	'30/12/2024':'53',
                    '01/02/2021':'6',	'23/05/2022':'22',	'11/09/2023':'38',	'01/01/2025':'1',
                    '08/02/2021':'7',	'30/05/2022':'23',	'18/09/2023':'39',	'06/01/2025':'2',
                    '15/02/2021':'8',	'06/06/2022':'24',	'25/09/2023':'40',	'13/01/2025':'3',
                    '22/02/2021':'9',	'13/06/2022':'25',	'02/10/2023':'41',	'20/01/2025':'4',
                    '01/03/2021':'10',	'20/06/2022':'26',	'09/10/2023':'42',	'27/01/2025':'5',
                    '08/03/2021':'11',	'27/06/2022':'27',	'16/10/2023':'43',	'03/02/2025':'6',
                    '15/03/2021':'12',	'04/07/2022':'28',	'23/10/2023':'44',	'10/02/2025':'7',
                    '22/03/2021':'13',	'11/07/2022':'29',	'30/10/2023':'45',	'17/02/2025':'8',
                    '29/03/2021':'14',	'18/07/2022':'30',	'06/11/2023':'46',	'24/02/2025':'9',
                    '05/04/2021':'15',	'25/07/2022':'31',	'13/11/2023':'47',	'03/03/2025':'10',
                    '12/04/2021':'16',	'01/08/2022':'32',	'20/11/2023':'48',	'10/03/2025':'11',
                    '19/04/2021':'17',	'08/08/2022':'33',	'27/11/2023':'49',	'17/03/2025':'12',
                    '26/04/2021':'18',	'15/08/2022':'34',	'04/12/2023':'50',	'24/03/2025':'13',
                    '03/05/2021':'19',	'22/08/2022':'35',	'11/12/2023':'51',	'31/03/2025':'14',
                    '10/05/2021':'20',	'29/08/2022':'36',	'18/12/2023':'52',	'07/04/2025':'15',
                    '17/05/2021':'21',	'05/09/2022':'37',	'25/12/2023':'53',	'14/04/2025':'16',
                    '24/05/2021':'22',	'12/09/2022':'38',	'01/01/2024':'1',	'21/04/2025':'17',
                    '31/05/2021':'23',	'19/09/2022':'39',	'08/01/2024':'2',	'28/04/2025':'18',
                    '07/06/2021':'24',	'26/09/2022':'40',	'15/01/2024':'3',	'05/05/2025':'19',
                    '14/06/2021':'25',	'03/10/2022':'41',	'22/01/2024':'4',	'12/05/2025':'20',
                    '21/06/2021':'26',	'10/10/2022':'42',	'29/01/2024':'5',	'19/05/2025':'21',
                    '28/06/2021':'27',	'17/10/2022':'43',	'05/02/2024':'6',	'26/05/2025':'22',
                    '05/07/2021':'28',	'24/10/2022':'44',	'12/02/2024':'7',	'02/06/2025':'23',
                    '12/07/2021':'29',	'31/10/2022':'45',	'19/02/2024':'8',	'09/06/2025':'24',
                    '19/07/2021':'30',	'07/11/2022':'46',	'26/02/2024':'9',	'16/06/2025':'25',
                    '26/07/2021':'31',	'14/11/2022':'47',	'04/03/2024':'10',	'23/06/2025':'26',
                    '02/08/2021':'32',	'21/11/2022':'48',	'11/03/2024':'11',	'30/06/2025':'27',
                    '09/08/2021':'33',	'28/11/2022':'49',	'18/03/2024':'12',	'07/07/2025':'28',
                    '16/08/2021':'34',	'05/12/2022':'50',	'25/03/2024':'13',	'14/07/2025':'29',
                    '23/08/2021':'35',	'12/12/2022':'51',	'01/04/2024':'14',	'21/07/2025':'30',
                    '30/08/2021':'36',	'19/12/2022':'52',	'08/04/2024':'15',	'28/07/2025':'31',
                    '06/09/2021':'37',	'26/12/2022':'53',	'15/04/2024':'16',	'04/08/2025':'32',
                    '13/09/2021':'38',	'01/01/2023':'1',	'22/04/2024':'17',	'11/08/2025':'33',
                    '20/09/2021':'39',	'02/01/2023':'2',	'29/04/2024':'18',	'18/08/2025':'34',
                    '27/09/2021':'40',	'09/01/2023':'3',	'06/05/2024':'19',	'25/08/2025':'35',
                    '04/10/2021':'41',	'16/01/2023':'4',	'13/05/2024':'20',	'01/09/2025':'36',
                    '11/10/2021':'42',	'23/01/2023':'5',	'20/05/2024':'21',	'08/09/2025':'37',
                    '18/10/2021':'43',	'30/01/2023':'6',	'27/05/2024':'22',	'15/09/2025':'38',
                    '25/10/2021':'44',	'06/02/2023':'7',	'03/06/2024':'23',	'22/09/2025':'39',
                    '01/11/2021':'45',	'13/02/2023':'8',	'10/06/2024':'24',	'29/09/2025':'40',
                    '08/11/2021':'46',	'20/02/2023':'9',	'17/06/2024':'25',	'06/10/2025':'41',
                    '15/11/2021':'47',	'27/02/2023':'10',	'24/06/2024':'26',	'13/10/2025':'42',
                    '22/11/2021':'48',	'06/03/2023':'11',	'01/07/2024':'27',	'20/10/2025':'43',
                    '29/11/2021':'49',	'13/03/2023':'12',	'08/07/2024':'28',	'27/10/2025':'44',
                    '06/12/2021':'50',	'20/03/2023':'13',	'15/07/2024':'29',	'03/11/2025':'45',
                    '13/12/2021':'51',	'27/03/2023':'14',	'22/07/2024':'30',	'10/11/2025':'46',
                    '20/12/2021':'52',	'03/04/2023':'15',	'29/07/2024':'31',	'17/11/2025':'47',
                    '27/12/2021':'53',	'10/04/2023':'16',	'05/08/2024':'32',	'24/11/2025':'48',
                    '01/01/2022':'1',	'17/04/2023':'17',	'12/08/2024':'33',	'01/12/2025':'49',
                    '03/01/2022':'2',	'24/04/2023':'18',	'19/08/2024':'34',	'08/12/2025':'50',
                    '10/01/2022':'3',	'01/05/2023':'19',	'26/08/2024':'35',	'15/12/2025':'51',
                    '17/01/2022':'4',	'08/05/2023':'20',	'02/09/2024':'36',	'22/12/2025':'52',
                    '24/01/2022':'5',	'15/05/2023':'21',	'09/09/2024':'37',	'29/12/2025':'53',
                    '31/01/2022':'6',	'22/05/2023':'22',	'16/09/2024':'38',	'01/01/2026':'1',
                    '07/02/2022':'7',	'29/05/2023':'23',	'23/09/2024':'39',	'05/01/2026':'2',
                    '14/02/2022':'8',	'05/06/2023':'24',	'30/09/2024':'40',	'12/01/2026':'3',
                    '21/02/2022':'9',	'12/06/2023':'25',	'07/10/2024':'41',	'19/01/2026':'4',
                    '28/02/2022':'10',	'19/06/2023':'26',	'14/10/2024':'42',	'26/01/2026':'5',
                    '07/03/2022':'11',	'26/06/2023':'27',	'21/10/2024':'43',	'02/02/2026':'6',
                    '14/03/2022':'12',	'03/07/2023':'28',	'28/10/2024':'44',	'09/02/2026':'7',
                    '21/03/2022':'13',	'10/07/2023':'29',	'04/11/2024':'45',	'16/02/2026':'8',
                    '28/03/2022':'14',	'17/07/2023':'30',	'11/11/2024':'46'}

    if 'vs_dl' in in_transaccion and 'mvs' not in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o': dato_para_nombre = '0000'
            elif variable == '2': dato_para_nombre = '0001'
            elif variable == '3': dato_para_nombre = '0002'
            elif variable == '4': dato_para_nombre = '0003'
            elif variable == '5': dato_para_nombre = '0004'
            elif variable == '6': dato_para_nombre = '0005'
            elif variable == '7': dato_para_nombre = '0006'
            elif variable == '8': dato_para_nombre = '0007'
            elif variable == '9': dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLSX") or name.endswith(".xlsx") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y') # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            lun_anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            anterior = lun_anterior.strftime('%d/%m/%Y')
            anio_lun = anterior.split('/')[-1]
            semana = dic_semanas[anterior].zfill(2)
            f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')
            anio_dom = f_final_date_file.split('-')[0]
            if anio_lun != anio_dom:
                f_final_date = anio_lun + '1231'
                f_final_date_file = anio_lun + '-12-31'
                anio_dom = anio_lun
            else:
                f_final_date = (lun_anterior + timedelta(days=6)).strftime('%Y%m%d')
                f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        pos_import = line[1] + ' ' + line[2].upper()

                        prod_import = str(line[4])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_SALE_WEE_' + f_final_date + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + anio_dom + ",," + semana + ',' + f_final_date_file + ',' + pos_import + ',' + prod_import + ',' + line[-3] + ',' '\n')
                        f_out_sale.close()

        except Exception as e:
            print(e)
            print('Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        print("proceso de transformación finalizado")
        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'mvs_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLSX") or name.endswith(".xlsx") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y') # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            lun_anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            anterior = lun_anterior.strftime('%d/%m/%Y')
            anio_lun = anterior.split('/')[-1]
            semana = dic_semanas[anterior.zfill(2)]
            f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')
            anio_dom = f_final_date_file.split('-')[0]
            if anio_lun != anio_dom:
                f_final_date = anio_lun + '1231'
                f_final_date_file = anio_lun + '-12-31'
                anio_dom = anio_lun
            else:
                f_final_date = (lun_anterior + timedelta(days=6)).strftime('%Y%m%d')
                f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        pos_import = line[1] + ' ' + line[2].upper()

                        prod_import = str(line[4])

                        f_out_sale = open(dir_temp + '0020_' + 'MADU_SALE_WEE_' + f_final_date + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + anio_dom + ',,' + semana + ',' + f_final_date_file + ',' + pos_import + ',' + prod_import + ',' + line[-3] + ',' '\n')
                        f_out_sale.close()

        except:
             print('Error de Transformacion')
             escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
             sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'tvm_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLS") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        fecha = in_fechaini.split('/')
                        time_id = fecha[2] + fecha[1] + fecha[0]

                        d = line[-1].split(' ')
                        e = d[1].split('/')
                        mes = int((e[0]))
                        anio = int((e[1]))
                        dia = (calendar.monthrange(anio, mes)[1])
                        ultimo_dia = str(anio) + e[0] + str(dia)
                        ultimo_dia_file = ultimo_dia[0:4] + '-' + ultimo_dia[4:6] + '-' + ultimo_dia[-2:]

                        pos_import = line[1] + ' ' + line[2].upper()
                        prod_import = str(line[4])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_SALT_MON_' + ultimo_dia + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + str(anio) + ',' + e[0] + ',,' + ultimo_dia_file + ',' + pos_import + ',' + prod_import + ',' + line[-3] + ',' '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'mvm_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLS") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        fecha = in_fechaini.split('/')
                        time_id = fecha[2] + fecha[1] + fecha[0]

                        d = line[-1].split(' ')
                        e = d[1].split('/')
                        mes = int((e[0]))
                        anio = int((e[1]))
                        dia = (calendar.monthrange(anio, mes)[1])
                        ultimo_dia = str(anio) + e[0] + str(dia)
                        ultimo_dia_file = ultimo_dia[0:4] + '-' + ultimo_dia[4:6] + '-' + ultimo_dia[-2:]

                        pos_import = line[1] + ' ' + line[2].upper()
                        prod_import = str(line[4])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_SALM_MON_' + ultimo_dia + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + str(anio) + ',' + e[0] + ',,' + ultimo_dia_file + ',' + pos_import + ',' + prod_import + ',' + line[-3] + ',' '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'is_dl' in in_transaccion and 'mis_dl' not in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLS") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y') # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            lun_anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            anterior = lun_anterior.strftime('%d/%m/%Y')
            anio_lun = anterior.split('/')[-1]
            semana = dic_semanas[anterior.zfill(2)]
            f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')
            anio_dom = f_final_date_file.split('-')[0]
            if anio_lun != anio_dom:
                f_final_date = anio_lun + '1231'
                f_final_date_file = anio_lun + '-12-31'
                anio_dom = anio_lun
            else:
                f_final_date = (lun_anterior + timedelta(days=6)).strftime('%Y%m%d')
                f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        pos_import = line[1] + ' ' + line[4].upper()
                        prod_import = str(line[7])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_IPOH_WEE_' + f_final_date + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + anio_dom + ",," + semana + ',' + f_final_date_file + ',' + pos_import + ',' + prod_import + ',' + line[-2] + ',' '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'mis_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLS") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y') # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            lun_anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            anterior = lun_anterior.strftime('%d/%m/%Y')
            anio_lun = anterior.split('/')[-1]
            semana = dic_semanas[anterior.zfill(2)]
            f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')
            anio_dom = f_final_date_file.split('-')[0]
            if anio_lun != anio_dom:
                f_final_date = anio_lun + '1231'
                f_final_date_file = anio_lun + '-12-31'
                anio_dom = anio_lun
            else:
                f_final_date = (lun_anterior + timedelta(days=6)).strftime('%Y%m%d')
                f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        pos_import = line[1] + ' ' + line[4].upper()

                        prod_import = str(line[7])

                        f_out_sale = open(dir_temp + '0020_' + 'MADU_IPOH_WEE_' + f_final_date + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + anio_dom + ",," + semana + ',' + f_final_date_file + ',' + pos_import + ',' + prod_import + ',' + line[-2] + ',' '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'tim_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLS") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        fecha = in_fechaini.split('/')
                        time_id = fecha[2] + fecha[1] + fecha[0]

                        d = line[-1].split(' ')
                        e = d[1].split('/')
                        mes = int((e[0]))
                        anio = int((e[1]))
                        dia = (calendar.monthrange(anio, mes)[1])
                        ultimo_dia = str(anio) + e[0] + str(dia)
                        ultimo_dia_file = ultimo_dia[0:4] + '-' + ultimo_dia[4:6] + '-' + ultimo_dia[-2:]

                        pos_import = line[1] + ' ' + line[4].upper()
                        prod_import = str(line[7])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_ITOH_MON_' + ultimo_dia + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + str(anio) + ',' + e[0] + ',,' + ultimo_dia_file + ',' + pos_import + ',' + prod_import + ',' + line[-2] + ',' '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'mim_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLS") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        fecha = in_fechaini.split('/')
                        time_id = fecha[2] + fecha[1] + fecha[0]

                        d = line[-1].split(' ')
                        e = d[1].split('/')
                        mes = int((e[0]))
                        anio = int((e[1]))
                        dia = (calendar.monthrange(anio, mes)[1])
                        ultimo_dia = str(anio) + e[0] + str(dia)
                        ultimo_dia_file = ultimo_dia[0:4] + '-' + ultimo_dia[4:6] + '-' + ultimo_dia[-2:]

                        pos_import = line[1] + ' ' + line[4].upper()
                        prod_import = str(line[7])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_IMOH_MON_' + ultimo_dia + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + str(anio) + ',' + e[0] + ',,' + ultimo_dia_file + ',' + pos_import + ',' + prod_import + ',' + line[-2] + ',' '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'st_dl' in in_transaccion and 'mst' not in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLSX") or name.endswith(".xlsx")][0].strip().split('.')[0]
            csv_from_excel(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y') # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            lun_anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            anterior = lun_anterior.strftime('%d/%m/%Y')
            anio_lun = anterior.split('/')[-1]
            semana = dic_semanas[anterior.zfill(2)]
            f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')
            anio_dom = f_final_date_file.split('-')[0]
            if anio_lun != anio_dom:
                f_final_date = anio_lun + '1231'
                f_final_date_file = anio_lun + '-12-31'
                anio_dom = anio_lun
            else:
                f_final_date = (lun_anterior + timedelta(days=6)).strftime('%Y%m%d')
                f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        pos_import = line[1] + ' ' + line[2].upper()

                        prod_import = str(line[4])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_STOT_WEE_' + f_final_date + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + anio_dom + ",," + semana + ',' + f_final_date_file + ',' + pos_import + ',' + prod_import + ',' + str(line[-3]) + ',' + str(line[-2]) + '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'mst_dl' in in_transaccion and 'msm' not in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLSX") or name.endswith(".xlsx")][0].strip().split('.')[0]
            csv_from_excel(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y') # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            lun_anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            anterior = lun_anterior.strftime('%d/%m/%Y')
            anio_lun = anterior.split('/')[-1]
            semana = dic_semanas[anterior.zfill(2)]
            f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')
            anio_dom = f_final_date_file.split('-')[0]
            if anio_lun != anio_dom:
                f_final_date = anio_lun + '1231'
                f_final_date_file = anio_lun + '-12-31'
                anio_dom = anio_lun
            else:
                f_final_date = (lun_anterior + timedelta(days=6)).strftime('%Y%m%d')
                f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')

                        pos_import = line[1] + ' ' + line[2].upper()

                        prod_import = str(line[4])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_STOM_WEE_' + f_final_date + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + anio_dom + ",," + semana + ',' + f_final_date_file + ',' + pos_import + ',' + prod_import + ',' + str(line[-3])[:12] + ',' + str(line[-2])[:12] + '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'omt_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLSX") or name.endswith(".xlsx")][0].strip().split('.')[0]
            csv_from_excel(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')
                        fecha = in_fechaini.split('/')
                        time_id = fecha[2] + fecha[1] + fecha[0]

                        d = line[8].split(' ')
                        e = d[1].split('/')
                        mes = int((e[0]))
                        anio = int((e[1]))
                        dia = (calendar.monthrange(anio, mes)[1])
                        ultimo_dia = str(anio) + e[0] + str(dia)
                        ultimo_dia_file = ultimo_dia[0:4] + '-' + ultimo_dia[4:6] + '-' + ultimo_dia[-2:]

                        pos_import = line[1] + ' ' + line[2].upper()
                        prod_import = str(line[4])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_STOT_MON_' + ultimo_dia + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + e[1] + ",," + e[0] + ',' + ultimo_dia_file + ',' + pos_import + ',' + prod_import + ',' + str(line[6]) + ',' + str(line[7]) + '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'omm_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLSX") or name.endswith(".xlsx")][0].strip().split('.')[0]
            csv_from_excel(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')
                        fecha = in_fechaini.split('/')
                        time_id = fecha[2] + fecha[1] + fecha[0]

                        d = line[8].split(' ')
                        e = d[1].split('/')
                        mes = int((e[0]))
                        anio = int((e[1]))
                        dia = (calendar.monthrange(anio, mes)[1])
                        ultimo_dia = str(anio) + e[0] + str(dia)
                        ultimo_dia_file = ultimo_dia[0:4] + '-' + ultimo_dia[4:6] + '-' + ultimo_dia[-2:]

                        pos_import = line[1] + ' ' + line[2].upper()
                        prod_import = str(line[4])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_STOM_MON_' + ultimo_dia + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + e[1] + ",," + e[0] + ',' + ultimo_dia_file + ',' + pos_import + ',' + prod_import + ',' + str(line[6]) + ',' + str(line[7]) + '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'ic_dl' in in_transaccion:  

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            inf = [name for name in os.listdir(dir_fuentes) if name.endswith(".XLS") or name.endswith(".xls")][0].strip().split('.')[0]
            csv_from_xls(dir_fuentes, inf, inf)
            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".csv")]

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 5:
                        line = line.strip().split(',')
                        fecha = in_fechaini.split('/')
                        time_id = fecha[2] + fecha[1] + fecha[0]
                        time_id_file = fecha[2] + '-' + fecha[1] + '-' + fecha[0]

                        pos_import = line[1]
                        prod_import = str(line[3])

                        f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_ICOH_DAY_' + time_id + '.csv', 'a+',encoding='utf-8')
                        f_out_sale.write(id_file + ',' + "0020," + time_id_file + ',' + pos_import + ',' + prod_import + ',' + str(line[5]) + ',' '\n')
                        f_out_sale.close()

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)

    elif 'its_dl' in in_transaccion:

        try:

            print('Iniciando transformacion')
            variable = in_transaccion[-7]
            if variable == 'o':
                dato_para_nombre = '0000'
            elif variable == '2':
                dato_para_nombre = '0001'
            elif variable == '3':
                dato_para_nombre = '0002'
            elif variable == '4':
                dato_para_nombre = '0003'
            elif variable == '5':
                dato_para_nombre = '0004'
            elif variable == '6':
                dato_para_nombre = '0005'
            elif variable == '7':
                dato_para_nombre = '0006'
            elif variable == '8':
                dato_para_nombre = '0007'
            elif variable == '9':
                dato_para_nombre = '0008'

            archivo = [name for name in os.listdir(dir_fuentes) if name.endswith(".txt")]

            dt = datetime.strptime(in_fechaini, '%d/%m/%Y') # vuelve in_fechaini en fecha
            start = dt - timedelta(days=dt.weekday())  # A in_fechaini se le resta la cantidad de dias que lleva la semana dando como resulta la fecha del lunes (days=dt.weekday significa que dada una fecha te devuelve el numero de dia en el que esta Lunes=0 Domingo=6)
            lun_anterior = start - timedelta(days=7)  # Se le restan siete dias a start para tomar el lunes de la semana anterior
            anterior = lun_anterior.strftime('%d/%m/%Y')
            anio_lun = anterior.split('/')[-1]
            semana = dic_semanas[anterior.zfill(2)]
            f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')
            anio_dom = f_final_date_file.split('-')[0]
            if anio_lun != anio_dom:
                f_final_date = anio_lun + '1231'
                f_final_date_file = anio_lun + '-12-31'
                anio_dom = anio_lun
            else:
                f_final_date = (lun_anterior + timedelta(days=6)).strftime('%Y%m%d')
                f_final_date_file = (lun_anterior + timedelta(days=6)).strftime('%Y-%m-%d')

            for arc in archivo:
                f_in = open(dir_fuentes + archivo[0], 'r', encoding='latin-1')
                for i, line in enumerate(f_in):
                    if i > 3:
                        if len(line) == 10:
                            line = line.strip().split('\t')

                            pos_import = line[4] + ' - ' + line[3].upper()

                            prod_import = str(line[7])

                            f_out_sale = open(dir_temp + '0020_' + dato_para_nombre + '_INOH_WEE_' + f_final_date + '.csv', 'a+',encoding='utf-8')
                            f_out_sale.write(id_file + ',' + "0020," + anio_dom + ",," + semana + ',' + f_final_date_file + ',' + pos_import.strip() + ',' + prod_import + ',' + line[9] + ',' '\n')
                            f_out_sale.close()
                        else:
                            print('Renglon incompleto: {}.'.format(line))

        except:
            print(e)
            escritura_traza(path_file, in_transaccion, 'Error de Transformacion')
            escritura_estado(path_file, in_transaccion, '910_Finalizado con Error Desconocido')
            sys.exit(0)

        borrar_archivo(dir_temp)
        comprimir(dir_temp, dir_destino)


def esperar_descarga(driver, dir_fuentes, can_arch, path_file, in_transaccion):
    print("Esperando descarga")
    t1 = datetime.now()
    archivos = [name for name in os.listdir(dir_fuentes) if name.endswith(".txt") or name.endswith(".xlsx") or name.endswith(".csv") or name.endswith(".xls")]
    #de 600 aumenté a 1200
    while len(archivos) < can_arch and (datetime.now()-t1).seconds <= 1200:
        time.sleep(5)
        archivos = [name for name in os.listdir(dir_fuentes) if name.endswith(".txt") or name.endswith(".xlsx") or name.endswith(".csv") or name.endswith(".xls")]
    if any(File.endswith(".txt") or File.endswith(".csv") or File.endswith(".xlsx")  or File.endswith(".xls") for File in os.listdir(dir_fuentes)):
        print("Reporte Descargado.")
        driver.close()
    else:
        print('Error al descargar archivo.')
        escritura_estado(path_file, in_transaccion, '997_Error al descargar archivo.')
        driver.close()
        sys.exit(0)


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
    

def comprimir(dir_temp, dir_destino):
    archivo = [name for name in os.listdir(dir_temp) if name.endswith(".csv")]
    for arc in archivo:
        file = arc.split('.csv')[0]
        with open(dir_temp + file + '.csv', 'rb') as f_in, gzip.open(dir_destino + file + '.csv.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        os.rename(dir_destino + file + '.csv.gz', dir_destino + file + '.gz')


def parser_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-in_usuario", help="Usuario para iniciar sesion en el portal", required=True)
    parser.add_argument("-in_password", help="Contrasenia para iniciar sesion en el portal", required=True)
    parser.add_argument("-in_direccion", help="Url del portal", required=True)
    parser.add_argument("-in_fechaini", help="Fecha inicial para buscar en el portal", required=True)
    parser.add_argument("-in_fechafin", help="Fecha inicial para buscar en el portal", required=False)
    parser.add_argument("-in_mensaje_tipo", help="Nombre del archivo salida", required=True)## XXXXXXX_XXXXXXX_cityclq_dl # l = 0000 # 2 = 0001 #
    parser.add_argument("-in_variables", help="Tipo de archivo a descargar 'csv' o 'xls'", required=True)
    args = parser.parse_args()
    return args


def main():
    if os.name == "nt":
        path_data = os.path.dirname(os.path.abspath(__file__)) + '\data\\'# Aqui se guardan los archivos .est y .log
        path_images = os.path.dirname(os.path.abspath(__file__)) + '\images\\'  # Se guardan las imagenes en caso de requerir image recognition
        dir_destino = os.path.dirname(os.path.abspath(__file__)) + '\_Upload_gz\\'  # Archivo final a subir a BQ comprimido .gz
        dir_temp = os.path.dirname(os.path.abspath(__file__)) + '\_Temp\\'  # Aqui se realizan las transformaciones necesarias al archivo
        dir_backup = os.path.dirname(os.path.abspath(__file__)) + '\_Backup\\'  # Se guarda una copia del archivo original descargado si se requiere
        dir_fuentes = os.path.dirname(os.path.abspath(__file__)) + '\_Fuentes\\'  # Aqui se descargan los archivos
    else:

        path_data = os.path.dirname(os.path.abspath(__file__)) + '/data/'# Aqui se guardan los archivos .est y .log
        path_images = os.path.dirname(os.path.abspath(__file__)) + '/images/'  # Se guardan las imagenes en caso de requerir image recognition
        dir_destino = os.path.dirname(os.path.abspath(__file__)) + '/_Upload_gz/'  # Archivo final a subir a BQ comprimido .gz
        dir_temp = os.path.dirname(os.path.abspath(__file__)) + '/_Temp/'  # Aqui se realizan las transformaciones necesarias al archivo
        dir_backup = os.path.dirname(os.path.abspath(__file__)) + '/_Backup/'  # Se guarda una copia del archivo original descargado si se requiere
        dir_fuentes = os.path.dirname(os.path.abspath(__file__)) + '/_Fuentes/'  # Aqui se descargan los archivos

    args = parser_args()
    if '_vp' in args.in_mensaje_tipo:
        limpiar(path_data, dir_destino, dir_temp, dir_backup, dir_fuentes)
        driver = setDriver(args.in_direccion, dir_fuentes, path_data, args.in_mensaje_tipo)
        driver = iniciar_sesion(driver, args.in_usuario, args.in_password, path_data, args.in_mensaje_tipo)
        driver.close()
        escritura_traza(path_data, args.in_mensaje_tipo, 'Tarea Finalizada Ok.')
        escritura_estado(path_data, args.in_mensaje_tipo, '120_Tarea Finalizada Ok.')
    else:
        limpiar(path_data, dir_destino, dir_temp, dir_backup, dir_fuentes)
        driver = setDriver(args.in_direccion, dir_fuentes, path_data, args.in_mensaje_tipo)
        driver = iniciar_sesion(driver, args.in_usuario, args.in_password, path_data, args.in_mensaje_tipo)
        busqueda_reportes(driver, dir_fuentes, args.in_fechaini, args.in_mensaje_tipo, path_data, args.in_mensaje_tipo, args.in_variables)
        transformar_archivo(dir_fuentes, dir_temp, dir_destino, args.in_fechaini, args.in_mensaje_tipo, path_data)
        comprimir_a_backup(dir_fuentes, dir_backup, args.in_mensaje_tipo, args.in_fechaini)
        escritura_traza(path_data, args.in_mensaje_tipo, 'Tarea Finalizada Ok')
        escritura_estado(path_data, args.in_mensaje_tipo, '120_Tarea Finalizada Ok')


if __name__ == "__main__":
    main()

