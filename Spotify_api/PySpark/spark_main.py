import random
from pyspark import SparkConf
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


#Configuramos la aplicacion de Spark
conf = SparkConf()


def conf_manual(conf):
    #.setAppName - Damos nombre a la aplicacion
    #.setMaster() - indica donde va hacer ejecutada nuestra aplicacion
    #   local[*] = maquina donde * es todos los nucleos
    #   clouster = ??

    conf.setAppName("DataSpotify").setMaster("local[*]").set("spark.driver.memory", "2g").set("spark.executor.memory","2g")

    print(dict(conf.getAll()))


def dfs_estruct():
    #objetos inmutables parecido a df de pandas de manera distribuida y no distribuida

    #Creacion de esquema de la data
    eschema = StructType([
        StructField("id",IntegerType(),False),
        StructField("nombre",StringType(),True),
        StructField("edad",StringType(),True)
    ])

    names = ["Sumi","Mozta","Minkie","Oliver","Werejero"]
    data = [(id, random.choice(names), random.randint(5,30)) for id in range(1,11)]

    return data

def dfs_paths():

    with open('/Users/axel/Documents/Portafolio/Spotify_api/PySpark/spark_conf.json') as spk:
        spark_config = json.load(spk)

    conf.setAll(spark_config.items())
    spark = (SparkSession.builder.config(conf=conf).getOrCreate())

    path = '/Users/axel/Documents/Portafolio/Spotify_api/PySpark/macrodata.csv'

    df = spark.read.csv(path)

    print(df)


def __main__():

    #conf_manual(conf)

    with open('/Users/axel/Documents/Portafolio/Spotify_api/PySpark/spark_conf.json') as spk:
        spark_config = json.load(spk)

    conf.setAll(spark_config.items())
    spark = (SparkSession.builder.config(conf=conf).getOrCreate())
    print(spark.getActiveSession())


    data = dfs_estruct()
    print(data)

    #dfs_paths()




if __name__ == '__main__':
    __main__()