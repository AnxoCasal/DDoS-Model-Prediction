from pyspark.sql import SparkSession
from typing import List
import pyspark.sql.types as T
from py4j.java_gateway import JavaObject
from Utils.meta import SingletonMeta
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
    def spark_dataframe_to_parquet(df, path, name):
 
        file_path = f'{path}/{name}.parquet'
        df.coalesce(1).write.parquet(file_path, mode='overwrite', compression='snappy')
 
        for file_name in os.listdir(path):
            if file_name.endswith('.parquet'):
                shutil.move(os.path.join(path, file_name), os.path.join(path, file_path))
 
        shutil.rmtree(path)
       
    @staticmethod
    def pandas_dataframe_to_parquet(df, path, name):
       
        file_path = f'{path}/{name}.parquet'
        df.to_parquet(file_path, compression='snappy')
        
class SavePartitions(metaclass=SingletonMeta):
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def __configure_hadoop(self):
        hadoop = self.spark.sparkContext._jvm.org.apache.hadoop  # type: ignore
        conf = hadoop.conf.Configuration()
        fs = hadoop.fs.FileSystem.get(conf)
        return hadoop, conf, fs

    def __ensure_exists(self, file: str):
        hadoop, _, fs = self.__configure_hadoop()
        if not fs.exists(hadoop.fs.Path(file)):
            out_stream = fs.create(hadoop.fs.Path(file, False))
            out_stream.close()


    def __delete_location(self, location: str):
        hadoop, _, fs = self.__configure_hadoop()
        if fs.exists(hadoop.fs.Path(location)):
            fs.delete(hadoop.fs.Path(location), True)


    def __get_files(self, src_dir: str) -> List[JavaObject]:
        """Get list of files in HDFS directory"""
        hadoop, _, fs = self.__configure_hadoop()
        self.__ensure_exists(src_dir)
        files = []
        for f in fs.listStatus(hadoop.fs.Path(src_dir)):
            if f.isFile():
                files.append(f.getPath())
        if not files:
            raise ValueError("Source directory {} is empty".format(src_dir))

        return files


    def __copy_merge_into(
        self, src_dir: str, dst_file: str, delete_source: bool = True
    ):
        """Merge files from HDFS source directory into single destination file

        Args:
            spark: SparkSession
            src_dir: path to the directory where dataframe was saved in multiple parts
            dst_file: path to single file to merge the src_dir contents into
            delete_source: flag for deleting src_dir and contents after merging

        """
        hadoop, conf, fs = self.__configure_hadoop()

        # 1. Get list of files in the source directory
        files = self.__get_files(src_dir)

        # 2. Set up the 'output stream' for the final merged output file
        # if destination file already exists, add contents of that file to the output stream
        if fs.exists(hadoop.fs.Path(dst_file)):
            tmp_dst_file = dst_file + ".tmp"
            tmp_in_stream = fs.open(hadoop.fs.Path(dst_file))
            tmp_out_stream = fs.create(hadoop.fs.Path(tmp_dst_file), True)
            try:
                hadoop.io.IOUtils.copyBytes(
                    tmp_in_stream, tmp_out_stream, conf, False
                )  # False means don't close out_stream
            finally:
                tmp_in_stream.close()
                tmp_out_stream.close()

            tmp_in_stream = fs.open(hadoop.fs.Path(tmp_dst_file))
            out_stream = fs.create(hadoop.fs.Path(dst_file), True)
            try:
                hadoop.io.IOUtils.copyBytes(tmp_in_stream, out_stream, conf, False)
            finally:
                tmp_in_stream.close()
                fs.delete(hadoop.fs.Path(tmp_dst_file), False)
        # if file doesn't already exist, create a new empty file
        else:
            out_stream = fs.create(hadoop.fs.Path(dst_file), False)

        # 3. Merge files from source directory into the merged file 'output stream'
        try:
            for file in files:
                in_stream = fs.open(file)
                try:
                    hadoop.io.IOUtils.copyBytes(
                        in_stream, out_stream, conf, False
                    )  # False means don't close out_stream
                finally:
                    in_stream.close()
        finally:
            out_stream.close()

        # 4. Tidy up - delete the original source directory
        if delete_source:
            self.__delete_location(src_dir)
            
    
    def save_file(self, df, output_dir, output_file):
       
        df.write.parquet(output_dir, mode='overwrite', compression='snappy')

        # merge main files in folder into single file
        self.__copy_merge_into(
            output_dir,
            output_file,
            delete_source=True,
        )