import boto3
import time
import os

class AWS_GLUE_S3():
    def __init__(self) -> None:
        self.s3_client = boto3.client('s3')
        self.glue_client = boto3.client('glue', region_name='us-east-1')

        buckets = self.get_buckets()

        s3_buckets = ["raw-data","staging-data","business-data"]

        if not s3_buckets in buckets:
        
            timestmp = int(time.time())
            for s3 in s3_buckets:
                self.bucket_name = f"{s3}-alex-{timestmp}"
                self.create_s3_bucket()
            
                buckets = self.get_buckets()

        raw_data = self.file("../Archivos/Raw/raw.parquet")
        self.upload_file(raw_data,f"raw-data-alex-{timestmp}")
        glue_role_arn = os.getenv('GLUE_ROLE_ARN')
        self.create_glue_job(glue_role_arn,buckets)
        self.run_glue_job()

    def delete_bucket(self,bucket_name):       
        try:
            objects = self.s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    self.s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                
                print("Archivos eliminados exitosamente del bucket.")
            
            self.s3_client.delete_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} eliminado exitosamente.")
        except Exception as e:
            print(f"Error al eliminar el bucket: {e}")

    def get_buckets(self):
        response = self.s3_client.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        return bucket_names
    
    def create_s3_bucket(self):
        try:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} creado exitosamente.")
        except Exception as e:
            print(f"Error al crear el bucket {self.bucket_name}: {e}")

    def upload_file(self, file, buck):
        try:
            print(f"Se va a cargar el archivo 'raw.parquet' en el bucket {buck}")
            self.s3_client.put_object(Bucket=buck, Key='raw.parquet', Body=file)
            print(f"Archivo 'raw.parquet' cargado exitosamente en el bucket {buck}.")
        except Exception as e:
            print(f"Error al cargar el archivo 'raw.parquet' en el bucket {buck}: {e}")

    def file(self, path):
        try:
            with open(path, 'rb') as file:
                raw_data = file.read()
                return raw_data
        except FileNotFoundError:
            print(f"El archivo {path} no se encontró.")
            return None
        except Exception as e:
            print(f"Error al leer el archivo {path}: {e}")
            return None

    def create_glue_job(self,glue_role_arn,buckets):
        for buck in buckets:
            if "raw-data" in buck:
                raw_buck = buck
            if "staging-data" in buck:
                staging_buck = buck
            if "scripts" in buck:
                script_buck = buck

        try:
            self.glue_client.create_job(
                Name='transform-job3',
                Role=glue_role_arn,
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{script_buck}/glue-scripts/glue-raw-staging.py', 
                    'PythonVersion': '3'
                },
                DefaultArguments={
                    '--input_bucket': raw_buck,
                    '--output_bucket': staging_buck
                },
                GlueVersion='2.0',
                MaxRetries=0,
                Timeout=240,
                Description='Script de transformación para AWS Glue',
                ExecutionProperty={
                    'MaxConcurrentRuns': 2
                }
            )
            print("Trabajo de AWS Glue creado exitosamente.")
        except Exception as e:
            print(f"Error al crear el trabajo de AWS Glue: {e}")

    def run_glue_job(self):
        try:
            self.glue_client.start_job_run(JobName="transform-job3")
            print("Job de AWS Glue iniciado exitosamente.")
        except Exception as e:
            print(f"Error al iniciar el job de AWS Glue: {e}")

aws_glue_s3 = AWS_GLUE_S3()
