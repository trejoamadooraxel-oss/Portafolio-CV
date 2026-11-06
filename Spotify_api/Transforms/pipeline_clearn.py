from datetime import datetime, timezone
import pandas as pd
import numpy as np
import unicodedata
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def normaliza_columnas(df: pd.DataFrame) -> pd.DataFrame:

    """
        Limpiamos las columnas de acentos y normalizamos en la ultima linea
    """
    logging.info('Procesando normalizacion de columnas.')

    try:
        df_copy = df.copy()
        columnas_sin_acentos = []
        for columna_df in df_copy.columns:
            texto_normalizado = unicodedata.normalize('NFD', columna_df)
            texto_sin_acentos = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')

            columnas_sin_acentos.append(texto_sin_acentos)
        df_copy.columns = columnas_sin_acentos
        df_copy.columns = df_copy.columns.str.strip().str.lower().str.replace(' ', '_')

        logging.info('OK. Procesando normalizacion de columnas.')

        return df_copy

    except Exception as e:
        logging.error(f"ERROR. Error crítico procesando el DataFrame: {str(e)}")
        raise e


def formateo_fechas(df: pd.DataFrame, columnas_fechas: list = None) -> pd.DataFrame:
    """
        Formatemaos las columnas de fechas a y-m-d
    """

    def normalizar_fecha(fecha_str):
        if pd.isna(fecha_str):
            return datetime(1998, 1, 27)

        fecha_str = str(fecha_str).strip()
        if not fecha_str or fecha_str.lower() == 'nan':
            return datetime(1998, 1, 27)

        try:
            # Fecha completa: "2003-06-15"
            if len(fecha_str) == 10:
                return datetime.strptime(fecha_str, "%Y-%m-%d")
            # Solo año y mes: "2003-06"
            elif len(fecha_str) == 7:
                return datetime.strptime(fecha_str, "%Y-%m")
            # Solo año: "2003"
            elif len(fecha_str) == 4:
                return datetime.strptime(fecha_str, "%Y")
            else:
                return datetime(1998, 1, 27)
        except Exception:
            return datetime(1998, 1, 27)

    logging.info('Procesando Columnas con fechas')

    try:

        if columnas_fechas is None:
            logging.info('No se encontraron columnas con fechas.')
            columnas_fechas = []

        df_copy = df.copy()
        for columna in columnas_fechas:
            if columna in df_copy.columns:
                # Corrección 2: Eliminamos la lambda innecesaria para mayor velocidad
                df_copy[columna] = df_copy[columna].apply(normalizar_fecha)

        #df_copy["fecha_ingesta"] = datetime.now(timezone.utc)

        logging.info('OK. Procesando Columnas con fechas')

        return df_copy

    except Exception as e:

        logging.error(f"ERROR. Error crítico procesando el DataFrame: {str(e)}")
        raise e


def normaliza_datos(df: pd.DataFrame) -> pd.DataFrame:

    logging.info('Procesando normalizacion de Datos.')

    try:

        df_copy = df.copy()
        for columna_df in df_copy.columns:
            logging.info(f'Procesando columna actual: {columna_df}')

            if columna_df == 'url':

                df_copy[columna_df] = (df_copy[columna_df]
                                       .str.get('spotify')
                                       .astype(str))

            else:
                df_copy[columna_df] = (df_copy[columna_df]
                    .astype(str)
                    .str.replace(r'[,"=]', '', regex=True)  # Borra coma, comilla y signo igual en un solo paso
                    .str.strip()
                )

        logging.info('OK. Procesando normalizacion de Datos.')

        return df_copy

    except Exception as e:
        logging.error(f"ERROR. Error crítico procesando el DataFrame: {str(e)}")
        raise e


def manejo_nulos_por_tipo(df: pd.DataFrame, columnas: list = None, formato_dato: str = 'str') -> pd.DataFrame:
    """
    Rellena valores nulos (NaN, None, NaT) en las columnas especificadas
    dependiendo del tipo de dato solicitado ('str', 'int', 'float').
    """
    def transformar_nulos(valor, formato):
        if pd.isna(valor):
            if formato == 'str':   return ''
            if formato == 'int':   return 0
            if formato == 'float': return 0.0

        #Si no es nulo, aseguramos el casteo limpio del dato existente
        try:
            if formato == 'str':
                texto = str(valor).strip()
                # Capturamos si el string literalmente dice 'nan' o 'none'
                if texto.lower() in ['nan', 'none', '']:
                    return ''
                return texto

            if formato == 'int':
                # Quitamos decimales si vienen de un float (ej: 12.0 -> 12)
                return int(float(valor))

            if formato == 'float':
                return float(valor)

        except (ValueError, TypeError) as e:
            logging.error(f"ERROR. Error crítico procesando el DataFrame: {str(e)}")
            raise e

        return valor

    logging.info('Procesando manejo de NULOS.')

    try:

        if columnas is None:
            columnas = []

        df_copy = df.copy()
        for columna in columnas:
            if columna in df_copy.columns:
                # Pasamos 'formato' como un argumento extra de .apply() después del nombre de la función
                df_copy[columna] = df_copy[columna].apply(transformar_nulos, formato=formato_dato)

        logging.info('OK. Procesando manejo de NULOS.')
        return df_copy

    except Exception as e:

        logging.error(f"ERROR. Error crítico procesando el DataFrame: {str(e)}")
        raise e

#Se dejo de ocupar
def eliminar_columnas_tablas(df: pd.DataFrame, tabla: str = 'str'):

    df_copy = df.copy()

    if tabla == 'artista':
        df_copy.drop('url', axis=1, inplace=True)

    if tabla == 'album':
        df_copy.drop(columns=['id_spotify_artist', 'url','tipo_album','fecha_precisa'], inplace=True)

    if tabla == 'track':
        df_copy.drop(columns=['id_spotify_artist', 'url', 'discos', 'explicito'], inplace=True)

    return df_copy