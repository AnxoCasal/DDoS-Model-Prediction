from utils import SparkSessionHandler, FileSystemHandler

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