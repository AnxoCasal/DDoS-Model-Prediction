import unittest
import pandas as pd
from Utils.utils import FileSystemHandler

class TestCSVEstructure(unittest.TestCase):
    def test_csv_exists(self):
        ruta_downloaded = './Archivos/Downloaded'
        self.assertGreater(len(FileSystemHandler.scan_directory(ruta_downloaded, 'csv')), 0,
                        f'No hay ningún archivo csv en el directorio {ruta_downloaded}')
        
    def test_csv_structure(self):
        ruta_downloaded = './Archivos/Downloaded'
        files = FileSystemHandler.scan_directory(ruta_downloaded, 'csv')

        headers = []
        for file in files:
            df = pd.read_csv(file, nrows=0)
            headers.append(df.columns.tolist())
        
        self.assertEqual(len(set(tuple(sublista) for sublista in headers)), 1 ,
                           f'Los archivos del directorio {ruta_downloaded} tienen estructuras diferentes')


if __name__ == '__main__':
    unittest.main()