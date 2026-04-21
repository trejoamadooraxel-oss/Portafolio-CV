from pathlib import Path
import pandas as pd
import numpy
import os

class Acctios_csv():
    def __init__(self):
        self.base = Path(__file__).parent
        self.ruta = self.base / "files_portafolio"
        self.ruta.mkdir(exist_ok=True)    

    def creacion_csv_albunes(self, song_albunes):
        try:

            print(f'Iniciando creacion de csv de lista de albunes.')
            print(f'Iniciando creacion del csv de albunes.')
            df = pd.DataFrame(song_albunes)
            df.to_csv(f'{self.ruta}/albunes.csv',encoding='utf8', index= False)

        except Exception as e:
            print(f'ERROR. No se pudo crear archivo albunes.csv en {self.ruta}, {e}')

    def creation_csv_top_ten(self, song_ten):
        try:

            print(f'Iniciando creacion de csv de top 10.')
            df = pd.DataFrame(song_ten)
            df.to_csv(f'{self.ruta}/top_10.csv',encoding='utf8', index=False)
            print(f'EXITO. Archivo "top_10.csv" creado en {self.ruta}.')

        except Exception as e:
            print(f'ERROR. No se pudo crear archivo top_10.csv en {self.ruta}, {e}')

    def creation_csv_artist(self, columns):
        
        try:
            columns.pop(0)
            columns.pop(0)
            print(f'Iniciando creacion de csv de lista de artistas.')
            df = pd.read_csv(f'{self.ruta}/top_10.csv', encoding='utf-8', dtype=str)
            df = df[df["artist"] != "artist"]
            df['num reproduction'] = df['num reproduction'].astype(int)

            df_grouped = df.groupby("artist").agg({
                "num reproduction": "sum"
            }).reset_index()

            df_grouped = df_grouped.assign(**{col: '' for col in columns})
            df_grouped.to_csv(f'{self.ruta}/list_artist.csv', encoding='utf-8', index=False, header=True)
            print(f'EXITO. Archivo "list_artist.csv" creado en {self.ruta}.')

        except Exception as e:
            print(f'ERROR. No se pudo crear archivo list_artist.csv en {self.ruta}, {e}')

    def read_csv_artist(self):
        try:

            print(f'Leyendo archivo "list_artist.csv" extraer columnas e informacion:')
            archivos = [name for name in os.listdir(self.ruta) if name.endswith('list_artist.csv')]
            print(archivos)
            for archivo in archivos:
                df_artist = pd.read_csv(f'{self.ruta}/{archivo}', encoding='utf-8', dtype=str)
                df_artist = df_artist.where(pd.notnull(df_artist), None)
                data = list(df_artist.itertuples(index=False, name=None))

        
        except Exception as e:
            print(f'ERROR. No se pudo leer el archivo, {e}.')
            
        
        return data



    #def read_csv_top_ten(self,):
"""def main():
 ruta = Acctios_csv().ruta
 print(ruta)

if __name__ == "__main__":
    main()
"""