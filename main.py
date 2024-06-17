from Utils.utils import SparkSessionHandler, FileSystemHandler
from Ingesta.Raw import raw
from Ingesta.Staging import staging
from Ingesta.Business import business
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

TEMP_DIR = './tmp'
DOWNLOADED_DIR = "./Archivos/Downloaded"
RAW_DIR = './Archivos/Raw'
STAGING_DIR = './Archivos/Staging'
BUSINESS_DIR = './Archivos/Business'

PORTS_DICTIONARY = {
    "web": [80, 443, 8080],             # 0
    "netbios": [137, 138, 139, 445],    # 1
    "ssh": [22],                        # 2
    "ftp": [21],                        # 3
    "ldap": [389, 3268],                # 4
    "ntp": [123],                       # 5
    "udp": [53],                        # 6
    "kerberos": [88],                   # 7
    "smtp": [465],                      # 8
    "rpc": [135],                       # 9
    "dns": [5353]                       # 10
}

def main_raw(spark, raw_dir, downloaded_dir, file_name='raw', extension='csv'):

    raw_paths = FileSystemHandler.scan_directory(downloaded_dir, 'csv')

    assert len(raw_paths) > 0, f'No se encontró ningún archivo con extension {extension} en el directorio {downloaded_dir}'

    df = raw.merge_csv_files(spark, raw_paths)
    df = raw.stratify_dataframe(df, " Label", ignore=['WebDDoS'])
    df = raw.refactor_headers(df)

    df = df.drop("SimillarHTTP")

    raw_parquet_dir = f'{raw_dir}/{file_name}'

    df.repartition(1).write.format('parquet').mode('overwrite').save(raw_parquet_dir)

    return raw_parquet_dir

def main_staging(spark, raw_path, staging_dir, file_name='staging'):

    df = spark.read.parquet(raw_path, header=True, inferSchema=True)

    df = df.filter(df["Protocol"] != 0)

    df = df.drop("Unnamed:_0", "Flow_ID", "Timestamp")

    df = staging.fill_with_max(df, "Flow_Packets/s")
    df = staging.fill_with_max(df, "Flow_Bytes/s")

    df = staging.drop_single_value_columns(df)

    staging_parquet_dir = f'{staging_dir}/{file_name}'

    df.repartition(1).write.format('parquet').mode('overwrite').save(staging_parquet_dir)

    return staging_parquet_dir

def main_bussiness (spark, staging_path, business_dir, ports_dict, file_name='business'):

    df = spark.read.parquet(staging_path, header=True, inferSchema=True)

    df = df.withColumn("Source_IP", business.ip_classification("Source_IP"))
    df = df.withColumn("Destination_IP", business.ip_classification("Destination_IP"))

    df = business.index_colum(df, "Protocol", "Protocal_Index")

    df = business.index_colum(df, "Label", "Label_Index")

    df = business.ports_to_id(df, "Source_Port", ports_dict)
    df = business.ports_to_id(df, "Destination_Port", ports_dict)

    business_parquet_dir = f'{business_dir}/{file_name}'

    df.repartition(1).write.format('parquet').mode('overwrite').save(business_parquet_dir)

    return business_parquet_dir

###################################################
###################################################
###################################################

spark = SparkSessionHandler.start_session()
 
####################################################
#                 C A P A    R A W                 #
####################################################
 
raw_path = main_raw(spark, RAW_DIR, DOWNLOADED_DIR)

print('raw acabado')
 
######################################################
#              C A P A    S T A G I N G              #
######################################################
 
staging_path = main_staging(spark, raw_path, STAGING_DIR)

print('staging acabado')
 
########################################################
#              C A P A    B U S I N E S S              #
########################################################

business_park = main_bussiness(spark, staging_path, BUSINESS_DIR, PORTS_DICTIONARY)

print('business acabado')
 
SparkSessionHandler.stop_session(spark)