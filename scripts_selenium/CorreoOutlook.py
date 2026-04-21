from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException

import argparse
import os
import re
import sys
import time
import random

tiempoAleatorio = random.randint(0, 5)
user = ''
password = ''
busca = 'Archivos prueba PDF'

""" CorreoOutlook.py -url https://outlook.live.com/owa/ -wait_time 5 -feini 16/04/2023 -fefin 21/04/2023 """

def presionarBoton(controlador,xpath,tiempoEspera):
    WebDriverWait(controlador, 30).until(EC.element_to_be_clickable((By.XPATH, xpath)))   
    controlador.find_element_by_xpath(xpath).click()
    time.sleep(tiempoEspera + tiempoAleatorio)
    
def config_Controlador(url,tiempoEspera):
    chromeOptions = Options()
    chromeOptions.add_argument('--incognito')
    if os.name != "nt":
        chromeOptions.add_argument('--headless')
    chromeOptions.add_argument('--window-size=1024,768')
    chromeOptions.add_argument('allow-running-insecure-content')
    chromeOptions.add_argument('--no-sandbox')

    if os.name == "nt":
        controlador = webdriver.Chrome(options=chromeOptions, executable_path='C:\ChromeDriver\chromedriver.exe')
    else:
        print("- Algo ocurrio en el Controlador -")
        controlador = webdriver.Chrome(options=chromeOptions)

    try:
        print('\n -- Driver Listo --\n')
        print('Accediendo a: {}'.format(url),'\n')
        controlador.get(url)
        time.sleep(tiempoEspera + tiempoAleatorio)

    except Exception as e:
        print('Error al acceder al portal')
        print(e)
        controlador.quit()
        sys.exit(0)
    
    return controlador

def llenadoFormulario(controlador,xpathF,info,tiempoEspera,xpathB):
    WebDriverWait(controlador, 30).until(EC.presence_of_element_located((By.XPATH,xpathF)))
    entrada_datos = controlador.find_element_by_xpath(xpathF)
    entrada_datos.send_keys(info)    
    presionarBoton(controlador,xpathB,tiempoEspera)

def extraccionContenido(controlador, xpath, tiempoEspera,fechaInicial,fechaFinal):
    
    try:
        #time.sleep(tiempoEspera + tiempoAleatorio)
        fecha_fin = datetime.strptime(fechaFinal, "%d/%m/%Y")
        fecha_fin = datetime.date(fecha_fin)    
        fecha_inicio = datetime.strptime(fechaInicial, "%d/%m/%Y")
        fecha_inicio = datetime.date(fecha_inicio)
        WebDriverWait(controlador, 30).until(EC.visibility_of_all_elements_located((By.XPATH,xpath)))
        print("\n-- Obteniendo contenido ---\n")
        soup = BeautifulSoup(controlador.page_source, "html.parser").find("div", {'class': 'Cz7T5 customScrollBar'})
        contenido = soup.find_all('div',{'class':'lulAg'})
        contenido = [i.text for i in contenido]
        print('Coincidieron ',str(len(contenido)),' Correos:')

        for contador in range(len(contenido)):
            presionarBoton(controlador,str(f'//span[@title="{contenido[contador]}"]'),tiempoEspera)
            soup = BeautifulSoup(controlador.page_source, "html.parser").find("div", {'class': 'AL_OM l8Tnu I1wdR'})
            fecha_archivo = (soup.text.split(' ')[1])
            fecha_archivo = datetime.strptime(fecha_archivo, "%d/%m/%Y")
            fecha_archivo = datetime.date(fecha_archivo)
           
            if fecha_archivo > fecha_inicio and fecha_archivo <= fecha_fin:
                
                try:
                    WebDriverWait(controlador, 10).until(EC.element_to_be_clickable((By.XPATH,'//button[@title="Más acciones"]')))
                    presionarBoton(controlador,'//button[@title="Más acciones"]',tiempoEspera)
                    presionarBoton(controlador,'//button[@name="Descargar"]',tiempoEspera)
                    print('Se encontro Archivo de la fecha', str(fecha_archivo))
                except:
                    print('No presento archivo')
            
            presionarBoton(controlador,'//button[@aria-label="Cerrar"]',tiempoEspera)


    except Exception as e:
        print('¡¡ Finalizado con Error de Descarga !!')
        print(e)
        controlador.quit()
        sys.exit(0)  
    



def navegacionPortal(controlador, tiempoEspera,fechaInicial,fechaFinal):
    try:
        presionarBoton(controlador,'//a[@data-task="signin"]',tiempoEspera)
        llenadoFormulario(controlador,'//input[@name="loginfmt"]',user,tiempoEspera,'//input[@value="Next"]')
        llenadoFormulario(controlador,'//input[@name="passwd"]',password,tiempoEspera,'//input[@value="Sign in"]')
        presionarBoton(controlador,'//input[@value="No"]',tiempoEspera)
        llenadoFormulario(controlador,'//input[@aria-label="Buscar"]',busca, tiempoEspera,'//input[@aria-label="Buscar"]')
        presionarBoton(controlador,'//button[@aria-label="Buscar"]',tiempoEspera)
        extraccionContenido(controlador,'//div[@class="Cz7T5 customScrollBar"]',tiempoEspera,fechaInicial,fechaFinal)        
        time.sleep(tiempoEspera + tiempoAleatorio) 
        controlador.quit()
        
    except Exception as e:
        print('¡¡ Finalizado con Error de Navegacion !!')
        print(e)
        controlador.quit()
        sys.exit(0)

def argumentos():
    parse = argparse.ArgumentParser()
    parse.add_argument("-pos_par_01", help="El ID de la tienda", required=False)
    parse.add_argument("-pos_par_02", help="No se usa", required=False)
    parse.add_argument("-pos_par_03", help="No se usa", required=False)
    parse.add_argument("-url", help="Introducir la url del Correo", required=True)
    parse.add_argument("-wait_time", help="Introduce un timpo de parasa para 'time.sleep'", required=True)
    parse.add_argument("-feini",help="Introduce la fecha inicial para la busqueda", required=True)
    parse.add_argument("-fefin",help="Introduce la fecha final para la busqueda", required=True)
    parse.add_argument("-o", help="El archivo resultante Bellisima.csv")    
    argumentos = parse.parse_args()
    return argumentos

def menu():
    print("\n- Iniciando Procesos -\n")
    argumento = argumentos()
    control = config_Controlador(argumento.url, int(argumento.wait_time))
    navegacionPortal(control, int(argumento.wait_time), str(argumento.feini), str(argumento.fefin))
    """ codigo_html = obtencion_Html(controlador, int(argumento.wait_time)) """

if __name__ == "__main__":
    start = datetime.now()
    menu()
    end = datetime.now()
    print('Proceso ejecutado en: {} seg.'.format(str((end - start).seconds)))