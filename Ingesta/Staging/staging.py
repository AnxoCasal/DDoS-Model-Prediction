from pyspark.sql.functions import col, when, max as spark_max
from Utils.utils import FileSystemHandler

def fill_with_max(df, column):
    '''
    Recibe un dataset y una columna de este.
    Castea todos los "Infinity" a float, guarda el siguiente valor máximo y posteriormente reemplaza, tanto 
    los inf como los nulls con este último.

    :df: Dataset
    :column: Nombre de la columna objetivo
    :max_value: Siguiente valor máximo a infinity
    '''

    df = df.withColumn(column, when(col(column) == "Infinity", float('inf')).otherwise(col(column).cast("float")))

    max_value = df.filter(~col(column).isin([float('inf'), float('-inf')])) \
                            .select(spark_max(column)).first()[0]

    df = df.withColumn(column, 
        when((col(column) == float('inf')) | (col(column).isNull()), max_value).otherwise(col(column)))
    
    return df

def drop_single_value_columns(df):
    '''
    Elimina todas las columnas que solo contengan un valor
    '''
    columns_to_drop = []
    df_pandas = df.toPandas()

    for column in df_pandas.columns:
        if df_pandas[column].value_counts().count() == 1:
           columns_to_drop.append(column)
    
    df = df.drop(*columns_to_drop)
    
    return df

def main(spark, raw_path, staging_dir, filter_columns=[], drop_columns=None, fill_max=[], file_name='staging'):

    assert len(FileSystemHandler.scan_directory(raw_path, 'parquet')), f'No se ha podido cargar el parquet {raw_path}. Verifica la ruta especificada o vuelve a ejecutar la etl'

    df = spark.read.parquet(raw_path, header=True, inferSchema=True)

    for column in filter_columns:
        df = df.filter(df[column] != 0)

    if drop_columns is not None:
        df = df.drop(*drop_columns)

    for column in fill_max:
        df = fill_with_max(df, column)

    df = drop_single_value_columns(df)

    staging_parquet_dir = f'{staging_dir}/{file_name}'

    df.repartition(1).write.format('parquet').mode('overwrite').save(staging_parquet_dir)

    return staging_parquet_dir