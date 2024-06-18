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

if __name__ == '__main__':
    
    spark = SparkSessionHandler.start_session()
    
    ####################################################
    #                 C A P A    R A W                 #
    ####################################################
    target = ' Label'
    ignore_columns = ['UDPLag']
    raw_drop = ['SimilarHTTP']
    
    raw_path = raw.main(spark, RAW_DIR, DOWNLOADED_DIR, target=' Label', ignore_columns=ignore_columns)

    print('raw acabado')
    
    ######################################################
    #              C A P A    S T A G I N G              #
    ######################################################
    

    staging_drop = ["Unnamed:_0", "Flow_ID", "Timestamp"]
    filter_cols = ['Protocol']
    fill_max = ['Flow_Packets/s', 'Flow_Bytes/s']

    staging_path = staging.main(spark, raw_path, STAGING_DIR, filter_columns=filter_cols, drop_columns=staging_drop, fill_max=fill_max)

    print('staging acabado')
    
    ########################################################
    #              C A P A    B U S I N E S S              #
    ########################################################

    ip_columns = ['Source_IP', 'Destination_IP']
    index_columns = [('Protocol', 'Protocal_Index'), ('Label', 'Label_Index')]
    port_columns = ['Source_Port', 'Destination_Port']

    business_path = business.main(spark, staging_path, BUSINESS_DIR, ports_dict=PORTS_DICTIONARY, ip_columns=ip_columns, index_columns=index_columns, port_columns=port_columns)

    print('business acabado')
    
    SparkSessionHandler.stop_session(spark)
