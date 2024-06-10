from pyspark.sql import SparkSession
import os, glob, json, shutil


class SparkSessionHandler():

    @staticmethod
    def start_session(app_name='ingesta_ddos'):
        '''
        Comienza la sesión de Spark con los parámetros indicados y la devuelve

        :param app_name: Nombre de la sesión
        :return: Sesión de spark creada
        '''
        spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

        return spark
    
    @staticmethod
    def stop_session(spark):
        spark.stop()

class FileSystemHandler():

    @staticmethod
    def scan_directory(directory, extension):
        '''
        Escanea todos los archivos en el directorio especificado con la extensión dada.

        :param directory: Directorio en el que buscar archivos
        :param extension: Extensión de los archivos a buscar (por ejemplo, 'csv' para archivos .csv)
        :return: Lista de rutas a los archivos que coinciden con la extensión
        '''
        search_pattern = os.path.join(directory, f'*.{extension}')
        files = glob.glob(search_pattern)

        return files
    
    @staticmethod
    def dictionary_to_json(file, path, name):
        '''
        Guarda un diccionario en un archivo JSON.

        :param file: Diccionario a guardar
        :param path: Ruta del archivo donde guardar el JSON
        :param name: Nombre que recibirá el JSON
        '''
        with open(f'{path}/{name}.json', 'w', encoding='utf-8') as json_file:
            json.dump(file, json_file, ensure_ascii=False, indent=4)

    @staticmethod
    def dataframe_to_parquet(df, path, name):

        df.coalesce(1).write.parquet(path, mode='overwrite', compression='snappy')

        for file_name in os.listdir(path):
            if file_name.endswith(".parquet"):
                shutil.move(os.path.join(path, file_name), os.path.join("Archivos/Staging", "staging.parquet"))

        shutil.rmtree(path)