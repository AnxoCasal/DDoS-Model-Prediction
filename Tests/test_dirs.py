import os
import unittest

class TestFileStructure(unittest.TestCase):
    
    def setUp(self):
        self.base_directory = ""
        self.required_structure = [
            ('', ['Ingesta', 'Archivos', 'Utils'], []),
            ('Ingesta', ['Raw', 'Staging', 'Business'], []),
            ('Ingesta/Raw', [], ['raw.py']),
            ('Ingesta/Staging', [], ['staging.py']),
            ('Ingesta/Business', [], ['business.py']),
            ('Utils', [], ['utils.py']),
            ('Archivos', ['Downloaded'], []),
        ]
    
    def check_directory(self, current_dir, subdirs, files):
        for subdir in subdirs:
            subdir_path = os.path.join(current_dir, subdir)
            self.assertTrue(os.path.isdir(subdir_path), f"Missing subdirectory: {subdir_path}")
        
        for file in files:
            file_path = os.path.join(current_dir, file)
            self.assertTrue(os.path.isfile(file_path), f"Missing file: {file_path}")
    
    def test_file_structure(self):
        for path, subdirs, files in self.required_structure:
            current_dir = os.path.join(self.base_directory, path)
            self.assertTrue(os.path.isdir(current_dir), f"Missing directory: {current_dir}")
            self.check_directory(current_dir, subdirs, files)

def test_file_structure():
    unittest.main()

test_file_structure()