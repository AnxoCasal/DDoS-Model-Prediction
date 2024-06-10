from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, max as spark_max, lit
from pyspark.ml.feature import StringIndexer
import shutil
import os
import sys

# os.environ['HADOOP_HOME'] = "C:/Program Files/spark-3.3.0-bin-hadoop3"
# sys.path.append("C:/Program Files/spark-3.3.0-bin-hadoop3")

port_dict = {
    "web": [80, 443, 8080],  # 0
    "netbios": [137, 138, 139, 445],  # 1
    "ssh": [22],  # 2
    "ftp": [21],  # 3
    "ldap": [389, 3268],  # 4
    "ntp": [123],  # 5
    "udp": [53],  # 6
    "kerberos": [88],  # 7
    "smtp": [465],  # 8
    "rpc": [135],  # 9
    "dns": [5353]  # 10
}

def ip_classification(col_name):
    return when(
        (split(col(col_name), '\.')[0] == 172) & (split(col(col_name), '\.')[1].cast('int') >= 16) & (split(col(col_name), '\.')[1].cast('int') <= 31), 1
    ).when(
        (split(col(col_name), '\.')[0] == 192) & (split(col(col_name), '\.')[1] == 168), 1
    ).otherwise(0)

def ports_to_id(df, column):
    df = df.withColumn(column, when(~col(column).isin([p for sublist in port_dict.values() for p in sublist]), 0).otherwise(col(column)))
    
    for i, port_list in enumerate(port_dict.values(),start=1):
        df = df.withColumn(column, when(col(column).isin(port_list), i).otherwise(col(column)))
    
    return df

def start_spark_file(path, appname="DDoS Data Processing"):

    spark = SparkSession.builder \
        .appName(appname) \
        .getOrCreate()

    return spark, spark.read.csv(path, header=True, inferSchema=True)

def index_colum(df, column, new_column, drop= True):

    indexer = StringIndexer(inputCol=column, outputCol=new_column)

    indexed_df = indexer.fit(df).transform(df)

    indexed_df = indexed_df.withColumn("LabelIndex", col("LabelIndex").cast("integer"))

    if drop:
        indexed_df = indexed_df.drop(column)

    return indexed_df

def fill_with_max(df, column):

    df = df.withColumn(column, when(col(column) == "Infinity", float('inf')).otherwise(col(column).cast("float")))

    max_value = df.filter(~col(column).isin([float('inf'), float('-inf')])) \
                            .select(spark_max(column)).first()[0]

    df = df.withColumn(column, 
        when((col(column) == float('inf')) | (col(column).isNull()), max_value).otherwise(col(column)))
    
    return column

def write_file(df):

    #df_final.write.parquet(os.path.join("Archivos/Staging", "staging.parquet"), mode='overwrite')
    
    ###### PARQUET
    output_dir = "temp_parquet_output"
    df.coalesce(1).write.parquet(output_dir, mode='overwrite', compression='snappy')

    for file_name in os.listdir(output_dir):
        if file_name.endswith(".parquet"):
            shutil.move(os.path.join(output_dir, file_name), os.path.join("Archivos/Staging", "staging.parquet"))

    shutil.rmtree(output_dir)

    ###### CSV
    # output_dir = "temp_csv_output"
    # df_final.coalesce(1).write.csv(output_dir, header=True, mode='overwrite')

    # for file_name in os.listdir(output_dir):
    #     if file_name.endswith(".csv"):
    #         shutil.move(os.path.join(output_dir, file_name), os.path.join("Archivos/Staging", "staging.csv"))

    # shutil.rmtree(output_dir)

def main():

    spark, df = start_spark_file("Archivos/Raw/raw.csv")

    df = df.withColumn("Source IP", ip_classification("Source IP"))
    df = df.withColumn("Destination IP", ip_classification("Destination IP"))

    df = index_colum(df, "Label", "LabelIndex")

    df = df.filter(df["Protocol"] != 0)

    df = index_colum(df, "Protocol", "ProtocalIndex")
    #df = df.withColumn("Protocolo", when(col("Protocolo") == 0, 0).otherwise(1)) LO COMENTE SIN PROBAR SI SIGUE FUNCIONANDO SIN EL!!!

    df = df.drop("Unnamed: 0", "Flow ID")

    df = ports_to_id(df, "Source Port")

    df = ports_to_id(df, "Destination Port")

    df = fill_with_max(df, "Flow Packets/s")

    df = fill_with_max(df, "Flow Bytes/s")

    write_file(df)

    spark.stop()
