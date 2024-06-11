from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, max as spark_max, lit
from pyspark.ml.feature import StringIndexer
import shutil
import os
import sys

# os.environ['HADOOP_HOME'] = "C:/Program Files/spark-3.3.0-bin-hadoop3"
# sys.path.append("C:/Program Files/spark-3.3.0-bin-hadoop3")


def ip_classification(col_name):
    '''
    Recibe una columna que contenga IPs, las lee y las clasifica en privadas (1) y públicas (0)
    
    :col_name: Nombre de la columna en el Dataset
    '''

    return when(
        (split(col(col_name), '\.')[0] == 172) & (split(col(col_name), '\.')[1].cast('int') >= 16) & (split(col(col_name), '\.')[1].cast('int') <= 31), 1
    ).when(
        (split(col(col_name), '\.')[0] == 192) & (split(col(col_name), '\.')[1] == 168), 1
    ).otherwise(0)

def ports_to_id(df, column, port_dict):
    '''
    Recibe un dataset, el nombre de la columna que contiene los puertos y un diccionario de puertos.
    Accede a la columna del dataset y transforma los puertos a sus valores respectivos del diccionario.
    Los que no contenga el diccionario los convierte a 0

    :df: Dataset
    :column: Columna de puertos
    :port_dict: Diccionario para los puertos (clave) y sus valores(valor)
    '''
    
    df = df.withColumn(column, when(~col(column).isin([p for sublist in port_dict.values() for p in sublist]), 0).otherwise(col(column)))
    
    for i, port_list in enumerate(port_dict.values(),start=1):
        df = df.withColumn(column, when(col(column).isin(port_list), i).otherwise(col(column)))
    
    return df

def index_colum(df, column, new_column, drop= True):
    '''
    Recibe un dataset, el nombre de la columna objetivo y el nombre de la nueva columna a crear, ademas de
    un booleano para indicar si se elimina la anterior o no.
    Crea un StringIndexer, un objeto que que recorre la columna y por cada valor nuevo le asigna un indice
    en una nueva columna.
    La nueva columna se castea a Integer y en caso de estar drop a True se elimina la columna original.

    :df: Dataset
    :column: Columna objetivo
    :new_column: Nombre que recibira la nueva columna creada
    :drop: Booleano para indicar si eliminamos la columna original
    '''

    indexer = StringIndexer(inputCol=column, outputCol=new_column)

    indexed_df = indexer.fit(df).transform(df)

    indexed_df = indexed_df.withColumn("LabelIndex", col("LabelIndex").cast("integer"))

    if drop:
        indexed_df = indexed_df.drop(column)

    return indexed_df

def fill_with_max(df, column):
    '''
    Recibe un dataset y una columna de este.
    Castea todos los "Infinity" a float, guarda el siguiente valor máximo y posteriormente reemplaza, tanto 
    los inf como los nulls con este último.

    :df: Dataset
    :column: Nombre de la columna objetivo
    :max_value: Siguiente valor máximo a infinity
    '''

    df = df.withColumn(column, when(col(column) == "Infinity", float('inf')).otherwise(col(column).cast("float")))

    max_value = df.filter(~col(column).isin([float('inf'), float('-inf')])) \
                            .select(spark_max(column)).first()[0]

    df = df.withColumn(column, 
        when((col(column) == float('inf')) | (col(column).isNull()), max_value).otherwise(col(column)))
    
    return df

def main_staging(file, spark):

    df = spark.read.csv(file, header=True, inferSchema=True)

    df = df.withColumn("Source IP", ip_classification("Source IP"))
    df = df.withColumn("Destination IP", ip_classification("Destination IP"))

    df = index_colum(df, "Label", "LabelIndex")

    df = df.filter(df["Protocol"] != 0)

    df = index_colum(df, "Protocol", "ProtocalIndex")

    df = df.drop("Unnamed: 0", "Flow ID")

    df = ports_to_id(df, "Source Port", port_dict)

    df = ports_to_id(df, "Destination Port", port_dict)

    df = fill_with_max(df, "Flow Packets/s")

    df = fill_with_max(df, "Flow Bytes/s")