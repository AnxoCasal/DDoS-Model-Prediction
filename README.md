# Proyecto

Este proyecto se ha creado con la finalidad de, a partir de un PCAP de Packet Tracer, predecimos si se está sufriendo algún tipo de ataque, tanto Ddos como DrDos y los diferentes tipos entre ellos.

Hemos utilizado un dataset (CSVs/CSV-01-12.zip) de la universidad de New Brunswick que contiene 87 columnas y millones de filas de las cuales hemos limpiado y nos hemos quedado con, alrededor de, 680 mil columnas. 
Los datos contienen valores como IP’s, protocolos, puertos, tamaño de paquetes, tiempo entre paquetes, etc. Estos se obtuvieron de un PCAP a través de la aplicación Packet Tracer durante 2 días. Se simuló actividad benigna de 25 usuarios a través de protocolos HTTP, HTTPS, FTP, SSH y email. Contienen datos de 11 tipos de ataques diferentes además de datos de tránsito de red benignos.

Hemos utilizado el siguiente Dataset para el entrenamiento del modelo:

[Dataset](https://www.unb.ca/cic/datasets/ddos-2019.html)

## Anaconda

En el directorio raíz encontraremos un archivo "enviroment.yml" que contiene todas las dependencias necesarias para la ejecución de los diferentes scripts.

```bash
conda env create --name envname --file=environment.yml
```

## Modo de Uso

Este repositorio tiene dos finalidades principales según el tipo de archivo que vamos a subir. En caso de subir archivos CSV con la columna "Label", que identifica el tipo de ataque, el conjunto de scripts detectará que ese CSV tiene la finalidad de entrenar uno de los tipos de modelos del repositorio y aplicará la ETL respecto a eso. Por otra parte, en caso de no existir la columna "Label" en ese archivo, será de la parte de predicción, donde se podrá seleccionar entre los diferentes modelos existentes para obtener el resultado del mismo, tanto si es benigno o maligno (y sus tipos).

En caso de que en la columna "Label" tengamos más tipos de ataques que el Dataset original, deberemos actualizar el archivo "utils.json" dentro del directorio "Utils", para después obtener predicciones en base a ello.

### Python

En caso de no querer utilizar o no disponer de la posibilidad de usar Anaconda, en el directorio raíz existe un archivo "requirements.txt" que contiene las dependencias necesarias con las versiones específicas, y usando el siguiente comando las instalaremos:

```bash
pip install -r requirements.txt
```

A continuación, ejecutaremos el archivo main.py si tenemos archivos nuevos en la carpeta "Downloaded", para aplicar la ETL sobre ellos.

```bash
python3 main.py
```

### Docker

Para la utilización del Docker necesitaremos unas credenciales de una cuenta de Aws que tenga asignado un Rol de IAM que le permita, crear y ejecutar jobs de Glue y crear y escribir Buckets S3. Estas credenciales estarán almacenadas en el directorio "Aws", en un archivo ".env" que deberá contener las siguientes variables:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION (por defecto, está asignado "us-east-1")
- GLUE_ROLE_ARN (deberemos introducir la url completa)

A continuación, ya podremos utilizar el docker ejecutando los siguientes comandos:

```bash
docker build -t my-glue-job .
docker run --env-file .env my-glue-job
```
## Requisitos

En caso de estar utilizando un sistema operativo Windows, necesitaremos:

- WSL
- Docker Desktop
- Spark
- Cuenta de Aws

Y en caso de estar utilizando Linux, no necesitaremos WSL.

## Autores

[Alejandro García López](https://www.linkedin.com/in/alejandro-garcia-lopez-3450041a3/)
[Anxo Casal Rodríguez](https://www.linkedin.com/in/anxo-casal-rodr%C3%ADguez-44b84630b/)
[Iago Núñez Lourés](https://www.linkedin.com/in/iago-nl-237a85299/)

## Contribución

Se agradecen las pull requests. Para cambios importantes, por favor abre primero un issue para discutir lo que te gustaría cambiar.

Por favor, asegúrate de actualizar las pruebas según corresponda.

## Licencia

[MIT](https://choosealicense.com/licenses/mit/)
