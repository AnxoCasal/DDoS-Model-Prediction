import unittest
from Ingesta.Staging import staging
from Ingesta.Business import business

class TestParquetPersistence(unittest.TestCase):

    def test_no_raw_parquet_raises_exc(self):
        with self.assertRaises(AssertionError):
            staging.main(None, 'NO_EXISTS', None)

    def test_no_staging_parquet(self):
        with self.assertRaises(AssertionError):
            business.main(None, 'NO_EXISTS', None)

if __name__ == '__main__':
    unittest.main()