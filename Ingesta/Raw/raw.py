from pyspark.sql.functions import col
from Utils.utils import FileSystemHandler

def merge_csv_files(spark, file_paths):
    """
    Lee varios archivos CSV y los combina en un solo DataFrame.
    
    :param file_paths: Lista de rutas a los archivos CSV
    :return: DataFrame combinado
    """
    dataframes = [spark.read.csv(file_path, header=True, inferSchema=True) for file_path in file_paths]
    
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df)
    
    return combined_df

def stratify_dataframe(df, target, ignore=[]):
    """
    Agrupa las filas en base al target, excluye las filas que en ese target tenga los valores a ignorar indicados.
    Luego coge el grupo con menor cantidad de valores e iguala todos los grupos para que tenga esa misma cantidad de instancias.
    Finalmente unifica los dataframes y los devuelve

    :df: Dataframe
    :target: columna por la cual se quiere agrupar
    :ignore: valores de la columna que se quieren ignorar
    """

    label_counts = df.groupBy(" Label").count().collect()
    min_count = min(row['count'] for row in label_counts if row[target] not in ignore)

    sampled_dfs = [df.filter((col(target) == row[target])).sample(False, min_count / row["count"], seed=2) for row in label_counts if (row[target] not in ignore)]

    balanced_df = sampled_dfs[0]
    for sdf in sampled_dfs[1:]:
        balanced_df = balanced_df.union(sdf)

    return balanced_df

def refactor_headers(df):

    '''
    Se reemplazan, en los nombres de las columnas, los espacios por _

    :df: Dataframe
    '''
    for column in df.columns:
        new_column = column.strip().replace(' ', '_')
        df = df.withColumnRenamed(column, new_column)

    return df

def main(spark, raw_dir, downloaded_dir, target=None, ignore_columns=[], drop_columns=None, file_name='raw', extension='csv'):

    raw_paths = FileSystemHandler.scan_directory(downloaded_dir, 'csv')

    assert len(raw_paths) > 0, f'No se encontró ningún archivo con extension {extension} en el directorio {downloaded_dir}'

    df = merge_csv_files(spark, raw_paths)

    if target is not None:
        df = stratify_dataframe(df, target, ignore=ignore_columns)

    df = refactor_headers(df)

    if drop_columns is not None:
        df = df.drop(*drop_columns)

    raw_parquet_dir = f'{raw_dir}/{file_name}'

    df.repartition(1).write.format('parquet').mode('overwrite').save(raw_parquet_dir)

    return raw_parquet_dir
