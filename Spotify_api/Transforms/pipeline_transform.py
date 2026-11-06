import pandas as pd
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
from pydantic import ValidationError

from Spotify_api.Transforms.pipeline_clearn import (
    normaliza_columnas, normaliza_datos, formateo_fechas, manejo_nulos_por_tipo, eliminar_columnas_tablas
)

from Spotify_api.Esquemas.esquemas import ArtistaSchema, AlbumSchema, TrackSchema, InfSchema, ArtistaSchemaBQ, \
    AlbumSchemaBQ, TrackSchemaBQ

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class DataPipeline:

    def __int__(self):
        pass

    def _transformar_datos(self, lista_entrada: List[Dict[str, Any]]) -> pd.DataFrame:

        logging.info('Iniciando Proceso de Tansformacion.')
        logging.info('Convertimos la lista de diccionarios a un df.')
        df_entrada_raw = pd.DataFrame(lista_entrada)

        logging.info('Obtenemos las columnas formateadas.')
        df_salida = (df_entrada_raw
                        .pipe(normaliza_columnas)
                        .pipe(normaliza_datos)
                    )

        df_salida = (df_salida
                        .pipe(formateo_fechas,['fecha_lanzamiento'])
                        .pipe(manejo_nulos_por_tipo,['id_spotify', 'id_album'],'str')
                        .pipe(manejo_nulos_por_tipo, ['id_artista','duracion','num_canciones','discos'], 'int')
                     )


        return  df_salida


    def _Postgres_data(self, df_clean: pd.DataFrame, tabla: str) -> list:

        logging.info(f'Generando la lista de registros para {tabla}.')

        list_dic = df_clean.to_dict(orient="records")

        registros_postgres = []
        for dic in list_dic:
            try:

                if tabla == 'artista':
                    registro_validado_postgres = ArtistaSchema(**dic)

                if tabla == 'album':
                    registro_validado_postgres = AlbumSchema(**dic)

                if tabla == 'track':
                    registro_validado_postgres = TrackSchema(**dic)

                if tabla == 'mas_inf':
                    registro_validado_postgres = InfSchema(**dic)

                registros_postgres.append(registro_validado_postgres.model_dump())

            except Exception as e:
                logging.warning(f"Warning. No se pudo validar la informacion de {dic}, {e}")

        logging.info(f'OK. Generando la lista de registros para {tabla}.')

        return registros_postgres

    def _Big_Query_data(self, df_clean: pd.DataFrame, tabla: str) -> pd.DataFrame:

        logging.info(f'Generando la lista de registros para {tabla}.')

        list_dic = df_clean.to_dict(orient="records")

        registros_bigquerry = []
        for dic in list_dic:
            try:
                if tabla == 'artista':
                    registro_validado_big_query = ArtistaSchemaBQ(**dic)

                if tabla == 'album':
                    registro_validado_big_query = AlbumSchemaBQ(**dic)

                if tabla == 'track':
                    registro_validado_big_query = TrackSchemaBQ(**dic)

                registros_bigquerry.append(registro_validado_big_query.model_dump(mode='json'))

            except Exception as e:
                logging.warning(f"Warning. No se pudo validar la informacion de {dic}, {e}")


        df_big_query = pd.DataFrame(registros_bigquerry)

        df_big_query["fecha_ingesta"] = datetime.now(timezone.utc)
        try:
            df_big_query['fecha_lanzamiento'] = pd.to_datetime(df_big_query['fecha_lanzamiento'] , utc=False)
        except:
            pass

        logging.info(f'OK. Generando la lista de registros para {tabla}.')

        return df_big_query