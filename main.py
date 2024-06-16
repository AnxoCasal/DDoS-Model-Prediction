from utils.utils import SparkSessionHandler, FileSystemHandler, SavePartitions
from ingesta.raw import raw
from ingesta.staging import staging
from ingesta.business import business

TEMP_DIR = './tmp'
DOWNLOADED_DIR = "./Archivos/Downloaded/"
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

def main_raw(spark, raw_dir, downloaded_dir, file_name='raw'):

    raw_paths = FileSystemHandler.scan_directory(downloaded_dir, 'csv')

    df = raw.merge_csv_files(spark, raw_paths)
    df = raw.stratify_dataframe(df, " Label", ignore=['WebDDoS'])
    df = raw.refactor_headers(df)

    df = df.drop("SimillarHTTP")

    raw_parquet_dir = f'{raw_dir}/{file_name}'

    df.repartition(1).write.format('parquet').mode('overwrite').save(raw_parquet_dir)

    return df

def main_staging(df, staging_dir, file_name='staging'):

    df = df.filter(df["Protocol"] != 0)

    df = df.drop("Unnamed:_0", "Flow_ID", "Timestamp")

    df = staging.fill_with_max(df, "Flow_Packets/s")
    df = staging.fill_with_max(df, "Flow_Bytes/s")

    df = staging.drop_single_value_columns(df)

    staging_parquet_dir = f'{staging_dir}/{file_name}'

    df.repartition(1).write.format('parquet').mode('overwrite').save(staging_parquet_dir)

    return df

def main_bussiness (df, business_dir, ports_dict, file_name='business'):

    df = df.withColumn("Source_IP", business.ip_classification("Source_IP"))
    df = df.withColumn("Destination_IP", business.ip_classification("Destination_IP"))

    df = business.index_colum(df, "Protocol", "Protocal_Index")

    df = business.index_colum(df, "Label", "Label_Index")

    df = business.ports_to_id(df, "Source_Port", ports_dict)
    df = business.ports_to_id(df, "Destination_Port", ports_dict)

    staging_parquet_dir = f'{business_dir}/{file_name}'

    df.repartition(1).write.format('parquet').mode('overwrite').save(staging_parquet_dir)

    return df

###################################################
###################################################
###################################################

spark = SparkSessionHandler.start_session()

####################################################
#                 C A P A    R A W                 #
####################################################

df = main_raw(spark, RAW_DIR, TEMP_DIR, DOWNLOADED_DIR)

######################################################
#              C A P A    S T A G I N G              #
######################################################

df = main_staging(df, STAGING_DIR, TEMP_DIR)

########################################################
#              C A P A    B U S I N E S S              #
########################################################
df = main_bussiness(df, BUSINESS_DIR, TEMP_DIR, PORTS_DICTIONARY)

SparkSessionHandler.stop_session(spark)