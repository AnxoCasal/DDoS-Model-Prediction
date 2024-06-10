from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, max as spark_max, lit
from pyspark.ml.feature import StringIndexer
import shutil
import os
import sys

# os.environ['HADOOP_HOME'] = "C:/Program Files/spark-3.3.0-bin-hadoop3"
# sys.path.append("C:/Program Files/spark-3.3.0-bin-hadoop3")

spark = SparkSession.builder \
    .appName("DDoS Data Processing") \
    .getOrCreate()

data = spark.read.csv("Archivos/Raw/raw.csv", header=True, inferSchema=True)

def ip_classification(col_name):
    return when(
        (split(col(col_name), '\.')[0] == 172) & (split(col(col_name), '\.')[1].cast('int') >= 16) & (split(col(col_name), '\.')[1].cast('int') <= 31), 1
    ).when(
        (split(col(col_name), '\.')[0] == 192) & (split(col(col_name), '\.')[1] == 168), 1
    ).otherwise(0)

data = data.withColumn("Source IP", ip_classification("Source IP"))
data = data.withColumn("Destination IP", ip_classification("Destination IP"))

indexer = StringIndexer(inputCol="Label", outputCol="LabelIndex")

df_indexed = indexer.fit(data).transform(data)

df_indexed = df_indexed.withColumn("LabelIndex", col("LabelIndex").cast("integer"))

df_final = df_indexed.drop("Label")

df_final = df_final.filter(df_final["Protocol"] != 0)

indexer = StringIndexer(inputCol="Protocol", outputCol="Protocolo")
df_final = indexer.fit(df_final).transform(df_final)

df_final = df_final.withColumn("Protocolo", col("Protocolo").cast("integer"))
df_final = df_final.withColumn("Protocolo", when(col("Protocolo") == 0, 0).otherwise(1))

df_final = df_final.drop("Protocol", "Unnamed: 0", "Flow ID")

ports = {
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

def ports_to_id(df, column):
    df = df.withColumn(column, when(~col(column).isin([p for sublist in ports.values() for p in sublist]), 0).otherwise(col(column)))
    
    for i, port_list in enumerate(ports.values(),start=1):
        df = df.withColumn(column, when(col(column).isin(port_list), i).otherwise(col(column)))
    
    return df

df_final = ports_to_id(df_final, "Source Port")
df_final = ports_to_id(df_final, "Destination Port")

df_final = df_final.withColumn("Flow Packets/s", when(col("Flow Packets/s") == "Infinity", float('inf')).otherwise(col("Flow Packets/s").cast("float")))

max_flow_packets = df_final.filter(~col("Flow Packets/s").isin([float('inf'), float('-inf')])) \
                           .select(spark_max("Flow Packets/s")).first()[0]

df_final = df_final.withColumn("Flow Packets/s", 
    when(col("Flow Packets/s") == float('inf'), lit(max_flow_packets)).otherwise(col("Flow Packets/s")))

df_final = df_final.withColumn("Flow Bytes/s", when(col("Flow Bytes/s") == "Infinity", float('inf')).otherwise(col("Flow Bytes/s").cast("float")))

max_flow_bytes = df_final.filter(~col("Flow Bytes/s").isin([float('inf')])) \
                         .select(spark_max("Flow Bytes/s")).first()[0]

df_final = df_final.withColumn("Flow Bytes/s", 
    when((col("Flow Bytes/s") == float('inf')) | (col("Flow Bytes/s").isNull()), max_flow_bytes).otherwise(col("Flow Bytes/s")))

#df_final.write.parquet(os.path.join("Archivos/Staging", "staging.parquet"), mode='overwrite')

###### PARQUET
output_dir = "temp_parquet_output"
df_final.coalesce(1).write.parquet(output_dir, mode='overwrite', compression='snappy')

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

spark.stop()
