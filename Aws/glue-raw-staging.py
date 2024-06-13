from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, max as spark_max
from pyspark.ml.feature import StringIndexer
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

def fill_with_max(df, column):
    df = df.withColumn(column, when(col(column) == "Infinity", float('inf')).otherwise(col(column).cast("float")))
    max_value = df.filter(~col(column).isin([float('inf'), float('-inf')])) \
                  .select(spark_max(column)).first()[0]
    df = df.withColumn(column, 
        when((col(column) == float('inf')) | (col(column).isNull()), max_value).otherwise(col(column)))
    return df

spark = SparkSession.builder.appName("AWS Glue Transform Job").getOrCreate()

args = getResolvedOptions(sys.argv, ['raw_bucket', 'staging_bucket'])
input_bucket = args['raw_bucket']
output_bucket = args['staging_bucket']

df = spark.read.parquet(f"s3://{input_bucket}/raw.parquet")

df = df.filter(df["Protocol"] != 0)

df = df.drop("Unnamed:_0", "Flow_ID", "Timestamp")

df = fill_with_max(df, "Flow_Packets/s")
df = fill_with_max(df, "Flow_Bytes/s")

dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame = dynamic_frame,
    connection_type = "s3",
    connection_options = {
        "path": f"s3://{output_bucket}/staging.parquet",
    },
    format = "parquet"
)

spark.stop()
sc.stop()
