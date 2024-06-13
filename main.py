from Utils.utils import SparkSessionHandler, FileSystemHandler, SavePartitions
from Ingesta.raw import raw
from Ingesta.staging import staging

TEMP_DIR = './tmp'
DOWNLOADED_DIR = "./archivos/downloaded"
RAW_DIR = './archivos/raw'
STAGING_DIR = './archivos/staging'

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

spark = SparkSessionHandler.start_session()

file_writer = SavePartitions(spark)

####################################################
#                   C A P A    R A W               #
####################################################

raw_paths = FileSystemHandler.scan_directory(DOWNLOADED_DIR, 'csv')

df = raw.merge_csv_files(spark, raw_paths)
df = raw.stratify_dataframe(df)

raw_parquet_dir = f'{RAW_DIR}/raw.parquet'

file_writer.save_file(df, TEMP_DIR, raw_parquet_dir)

######################################################
#              C A P A    S T A G I N G              #
######################################################

df = df.withColumn("Source IP", staging.ip_classification("Source IP"))
df = df.withColumn("Destination IP", staging.ip_classification("Destination IP"))

df = staging.index_colum(df, "Label", "LabelIndex")
df = df.filter(df["Protocol"] != 0)
df = staging.index_colum(df, "Protocol", "ProtocalIndex")

df = df.drop("Unnamed: 0", "Flow ID")

df = staging.ports_to_id(df, "Source Port", PORTS_DICTIONARY)
df = staging.ports_to_id(df, "Destination Port", PORTS_DICTIONARY)

df = staging.fill_with_max(df, "Flow Packets/s")
df = staging.fill_with_max(df, "Flow Bytes/s")

staging_parquet_dir = f'{STAGING_DIR}/staging.parquet'

file_writer.save_file(df, TEMP_DIR, staging_parquet_dir)

SparkSessionHandler.stop_session(spark)

##########################################################
#              C A P A    B U S S I N E S S              #
##########################################################

df = df.drop("Timestamp", "SimillarHTTP")
