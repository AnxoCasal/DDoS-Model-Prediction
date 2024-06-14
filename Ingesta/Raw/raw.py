from pyspark.sql.functions import col

def merge_csv_files(spark, file_paths):
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

    # Tomar una muestra de tamaño igual al número mínimo de entradas para cada valor de 'label'
    sampled_dfs = [df.filter((col(" Label") == row[' Label'])).sample(False, min_count / row['count'], seed=2) for row in label_counts if (row[' Label'] != 'WebDDoS')]

    # Combinar las muestras en un DataFrame final estratificado
    balanced_df = sampled_dfs[0]
    for sdf in sampled_dfs[1:]:
        balanced_df = balanced_df.union(sdf)

    return balanced_df

def refactor_headers(df):

    #Los jobs de GLUE no pueden tener archivos cuyos headers contengan espacios
    '''
    for column in df.columns:
        new_column = column.strip().replace(' ', '_')
        df = df.withColumnRenamed(column, new_column)
    '''
    for column in df.columns:
        new_column = column.strip().replace(' ', '_')
        df = df.withColumnRenamed(column, new_column)

    #Esta columna causa problemas en aws
    df = df.drop("SimillarHTTP")

    return df
