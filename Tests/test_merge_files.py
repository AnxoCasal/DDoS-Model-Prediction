import unittest
from Ingesta.Raw.raw import merge_csv_files

class TestMergeCSV(unittest.TestCase):
    def test_merge_csv_empty_list(self):
        with self.assertRaises(AssertionError):
            merge_csv_files(None, [])

def test_merge_csv():
    unittest.main()

