import pandas as pd
from training import DataPreprocessing
import json

RUTA = "./Archivos/Business/business.parquet"
TARGET = "Label_Index"

def dictionary_to_json(file, path, name):
    '''
    Guarda un diccionario en un archivo JSON.
    :param file: Diccionario a guardar
    :param path: Ruta del archivo donde guardar el JSON
    :param name: Nombre que recibirá el JSON
    '''
    with open(f'{path}/{name}.json', 'w', encoding='utf-8') as json_file:
        json.dump(file, json_file, ensure_ascii=False, indent=4)

df = pd.read_parquet(RUTA)
df = DataPreprocessing.apply_scaler(df, categorical_cols=["Label_Index", "Source_Port", "Destination_Port"])

variance_ratio = DataPreprocessing.lda_variance_ratio(df.drop(TARGET, axis=1), df['Label_Index'])
dictionary_to_json(variance_ratio, './Utils', 'variance_ratio')

df = DataPreprocessing.apply_lda(df, "Label_Index", 6)
df.to_parquet("./Entrenamiento/training_set.parquet", compression='snappy')