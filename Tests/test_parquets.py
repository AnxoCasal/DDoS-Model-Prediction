import unittest
from main import main_staging, main_business

class TestParquetPersistence(unittest.TestCase):

    def test_no_raw_parquet_raises_exc(self):
        with self.assertRaises(AssertionError):
            main_staging(None, 'NO_EXISTS', None)

    def test_no_staging_parquet(self):
        with self.assertRaises(AssertionError):
            main_business(None, 'NO_EXISTS', None, None)

if __name__ == '__main__':
    unittest.main()