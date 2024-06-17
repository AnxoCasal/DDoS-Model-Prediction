from Tests.test_merge_files import TestMergeCSV
from Tests.test_dirs import TestFileStructure
from Tests.test_parquets import TestParquetPersistence
from Tests.test_aws_credentials import TestAWSCredentials
from Tests.test_csv_structure import TestCSVEstructure
import unittest

def run_tests(test):
    # Crear un test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(test)
    
    # Ejecutar el test suite
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    
    # Puedes manejar los resultados aquí
    if result.wasSuccessful():
        print("Todos los tests pasaron")
    else:
        print(f'El test {test.__name__} ha fallado')
        result.printErrors()

if __name__ == "__main__":
    #run_tests(TestMergeCSV)
    #run_tests(TestFileStructure)
    #run_tests(TestParquetPersistence)
    #run_tests(TestAWSCredentials)
    run_tests(TestCSVEstructure)
