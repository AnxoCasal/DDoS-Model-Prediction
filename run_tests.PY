from Tests.test_merge_files import TestMergeCSV
from Tests.test_dirs import test_file_structure
import unittest

def run_tests():
    # Crear un test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMergeCSV)
    
    # Ejecutar el test suite
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    
    # Puedes manejar los resultados aquí
    if result.wasSuccessful():
        print("Todos los tests pasaron")
    else:
        print("Algunos tests fallaron")

if __name__ == "__main__":
    run_tests()