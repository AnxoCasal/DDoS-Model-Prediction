import boto3
import time
import os

class AWS_GLUE_S3():
    def __init__(self) -> None:
        region_aws = os.getenv('AWS_REGION')
        self.s3_client = boto3.client('s3')
        self.glue_client = boto3.client('glue', region_name=region_aws)

        buckets = self.get_buckets()

        if not any("glue-scripts" in bucket for bucket in buckets):
            bucket_name = "glue-scripts-rsb"
            self.create_s3_bucket(bucket_name)
            staging_file = "glue-raw-staging.py"
            staging = self.file(staging_file)
            self.upload_file(staging_file,staging,bucket_name) 
            business_file = "glue-staging-business.py"
            business =  self.file(business_file)
            self.upload_file(business_file,business,bucket_name) 

        timestmp = int(time.time())

        s3_buckets = ["raw-data","staging-data","business-data"]
        for s3 in s3_buckets: 
            if not any(s3 in bucket for bucket in buckets): 
                bucket_name = f"{s3}-alex-{timestmp}"
                self.create_s3_bucket(bucket_name)
                buckets = self.get_buckets()
            
        raw_file = "raw.parquet"
        raw_data = self.file(raw_file)
        self.upload_file(raw_file,raw_data,f"raw-data-alex-{timestmp}")

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
    
    def create_s3_bucket(self,bucket_name):
        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} creado exitosamente.")
        except Exception as e:
            print(f"Error al crear el bucket {bucket_name}: {e}")

    def upload_file(self,filename,file,buck):
        try:
            print(f"Se va a cargar el archivo {filename} en el bucket {buck}")
            self.s3_client.put_object(Bucket=buck, Key=filename, Body=file)
            print(f"Archivo {filename} cargado exitosamente en el bucket {buck}.")
        except Exception as e:
            print(f"Error al cargar el archivo {filename} en el bucket {buck}: {e}")

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
                Name='default-job',
                Role=glue_role_arn,
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{script_buck}/glue-raw-staging.py', 
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
            self.glue_client.start_job_run(JobName="default-job")
            print("Job de AWS Glue iniciado exitosamente.")
        except Exception as e:
            print(f"Error al iniciar el job de AWS Glue: {e}")

aws_glue_s3 = AWS_GLUE_S3()
