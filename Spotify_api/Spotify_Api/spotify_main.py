import os
import sys
import logging
#from dotenv import load_dotenv

# Importamos tus módulos de conexión y extracción locales
import Spotify_api.Conexiones.conection_api as spotify_api
import Spotify_api.Spotify_Api.extract_inf_api as inf_api

# Configuramos los logs para que puedas verlos desde la página de Airflow
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def spotify_api_disponible(sp):
    """Tu función original que verifica si la API de Spotify responde"""
    try:
        sp.search(q="test", type="artist", limit=1)
        logging.info("OK. La conexion con Spotify API ha sido Exitosa")
        return True
    except Exception as e:
        logging.error(f"ERROR. La conexion con Spotify API ha fallado: {e}")
        return False


def ejecutar_pipeline():
    # 1. Cargamos las credenciales usando la ruta interna de Docker
    #load_dotenv('/app/Spotify_api/env_var/varaibles_credential.env')

    # 2. Capturamos el artista que Airflow nos enviará de forma dinámica
    nombre_artista = os.getenv('ARTISTA_BUSQUEDA', 'Bad Bunny')
    logging.info(f"Iniciando extracción para el artista: {nombre_artista}")

    # 3. Conectamos a Spotify
    sp = spotify_api.conection_spotify()

    # 4. Validamos disponibilidad (Tu antiguo PythonSensor)
    if not spotify_api_disponible(sp):
        logging.error("Deteniendo el proceso porque la API de Spotify no está disponible.")
        sys.exit(1)  # Le avisa a Airflow que la tarea falló

    # 5. Extraemos el artista (Tu antigua @task)
    id_spotify, artistas = inf_api.identificador_artistas(sp, nombre_artista)
    logging.info(f"Artista extraido con éxito: {artistas[0]}")

    # Aquí abajo sigue tu lógica para guardar en la Base de Datos usando SQLAlchemy moderno...
    # ...

    logging.info("ETL finalizado con éxito.")
    sys.exit(0)  # Le avisa a Airflow que todo salió perfecto


if __name__ == "__main__":
    ejecutar_pipeline()
