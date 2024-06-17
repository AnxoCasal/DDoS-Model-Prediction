import unittest, os

class TestAWSCredentials(unittest.TestCase):
    def test_aws_directory_exists(self):
        self.assertTrue(os.path.isdir('./Aws'), "Falta el directorio de AWS")

    def test_dot_env_exists(self):
        self.assertTrue(os.path.isfile('./Aws/.env'), "No existe el fichero .env con las credenciales para aws")

    def test_aws_credentials(self):
        credentials = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'GLUE_ROLE_ARN']
        lineas = []
        with open('./Aws/.env', 'r') as env:
            lineas_raw = env.readlines()

        for linea in lineas_raw:
            lineas.append(linea.split('=')[0])

        for credential in credentials:
            self.assertIn(credential, lineas, f'Falta la credential {credential} para AWS en el fichero .env')

if __name__ == '__main__':
    unittest.main()