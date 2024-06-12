from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, max as spark_max
from pyspark.ml.feature import StringIndexer
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

# Crear un SparkContext y un GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


def ip_classification(col_name):
    return when(
        (split(col(col_name), '\.')[0] == 172) & (split(col(col_name), '\.')[1].cast('int') >= 16) & (split(col(col_name), '\.')[1].cast('int') <= 31), 1
    ).when(
        (split(col(col_name), '\.')[0] == 192) & (split(col(col_name), '\.')[1] == 168), 1
    ).otherwise(0)

def ports_to_id(df, column, port_dict):
    df = df.withColumn(column, when(~col(column).isin([p for sublist in port_dict.values() for p in sublist]), 0).otherwise(col(column)))
    for i, port_list in enumerate(port_dict.values(), start=1):
        df = df.withColumn(column, when(col(column).isin(port_list), i).otherwise(col(column)))
    return df

def index_colum(df, column, new_column, drop=True):
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
    return df

spark = SparkSession.builder.appName("AWS Glue Transform Job").getOrCreate()

args = getResolvedOptions(sys.argv, ['input_bucket', 'output_bucket'])
input_bucket = args['input_bucket']
output_bucket = args['output_bucket']

df = spark.read.parquet(f"s3://{input_bucket}/raw.parquet")

df = df.withColumn("Source_IP", ip_classification("Source_IP"))
df = df.withColumn("Destination_IP", ip_classification("Destination_IP"))

df = index_colum(df, "Label", "LabelIndex")
df = df.filter(df["Protocol"] != 0)
df = index_colum(df, "Protocol", "ProtocolIndex")

df = df.drop("Unnamed:_0", "Flow_ID")

PORTS_DICTIONARY = {
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

df = ports_to_id(df, "Source_Port", PORTS_DICTIONARY)
df = ports_to_id(df, "Destination_Port", PORTS_DICTIONARY)

df = fill_with_max(df, "Flow_Packets/s")
df = fill_with_max(df, "Flow_Bytes/s")

# Convertir DataFrame de Spark a DynamicFrame de Glue
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Escribir el DynamicFrame en S3
glueContext.write_dynamic_frame.from_options(
    frame = dynamic_frame,
    connection_type = "s3",
    connection_options = {
        "path": f"s3://{output_bucket}/staging.parquet",
    },
    format = "parquet"
)

# Detener SparkSession y SparkContext
spark.stop()
sc.stop()
