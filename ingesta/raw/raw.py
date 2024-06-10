from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.types as T
import saving_script as sc

def merge_csv_files(file_paths):
    """
    Lee varios archivos CSV y los combina en un solo DataFrame.
    
    :param file_paths: Lista de rutas a los archivos CSV
    :return: DataFrame combinado
    """
    # Leer cada archivo CSV en un DataFrame y almacenarlos en una lista
    dataframes = [spark.read.csv(file_path, header=True, inferSchema=True) for file_path in file_paths]
    
    # Combinar todos los DataFrames en uno solo
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df)
    
    return combined_df

def stratify_dataframe(df):

    label_counts = df.groupBy(" Label").count().collect()
    min_count = min(row['count'] for row in label_counts if row[' Label'] != 'WebDDoS')
    print(label_counts)
    print(min_count)

    # Tomar una muestra de tamaño igual al número mínimo de entradas para cada valor de 'label'
    sampled_dfs = [df.filter((col(" Label") == row[' Label'])).sample(False, min_count / row['count'], seed=2) for row in label_counts if (row[' Label'] != 'WebDDoS')]

    # Combinar las muestras en un DataFrame final estratificado
    balanced_df = sampled_dfs[0]
    for sdf in sampled_dfs[1:]:
        balanced_df = balanced_df.union(sdf)

    return balanced_df

spark = SparkSession.builder \
        .appName("Merge CSV Files") \
        .getOrCreate()

# Lista de rutas a los archivos CSV
file_paths = [
    "./01-12/DrDoS_LDAP.csv",
    "./01-12/DrDoS_MSSQL.csv",
    "./01-12/DrDoS_NetBIOS.csv",
    "./01-12/DrDoS_UDP.csv",
    "./01-12/DrDoS_SSDP.csv",
    "./01-12/DrDoS_SNMP.csv",
    "./01-12/DrDoS_NTP.csv",
    "./01-12/DrDoS_DNS.csv",
    "./01-12/Syn.csv",
    "./01-12/TFTP.csv",
    "./01-12/UDPLag.csv"
]

# Llamar a la función para combinar los CSVs
df = merge_csv_files(file_paths)
df = stratify_dataframe(df)

'''
# Ruta para el nuevo archivo CSV
output_path = "./03-11/COMBINADO.csv"

# Guardar el DataFrame en un nuevo archivo CSV
df.coalesce(1).write.option("header","true").csv(output_path)
'''


TMP_OUTPUT_DIR = "./test"
OUTPUT_FILE = "test.csv"

headers = spark.createDataFrame(
    data=[[f.name for f in df.schema.fields]],
    schema=T.StructType(
        [T.StructField(f.name, T.StringType(), False) for f in df.schema.fields]
    ),
)
headers.write.csv(TMP_OUTPUT_DIR)

# write csv headers to output file first
sc.copy_merge_into(
    spark,
    TMP_OUTPUT_DIR,
    OUTPUT_FILE,
    delete_source=True,
)

# Write main outputs
# dataframe written to TMP_OUTPUT_DIR folder in 5 separate csv files (one for each partition)
df.write.csv(TMP_OUTPUT_DIR)

# merge main csv files in folder into single file
sc.copy_merge_into(
    spark,
    TMP_OUTPUT_DIR,
    OUTPUT_FILE,
    delete_source=True,
)

spark.stop()
