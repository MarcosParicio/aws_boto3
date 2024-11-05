import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

import requests

from pymongo import MongoClient

# Carga las variables de entorno del archivo .env
# boto3 automáticamente detectará las credenciales desde las variables de entorno
load_dotenv()

def list_ec2_instances():
    # Crear el cliente de EC2
    ec2 = boto3.client('ec2')

    # Llamar a la API de EC2 para obtener la lista de instancias
    response = ec2.describe_instances()

    # Recorrer la respuesta para obtener los detalles de las instancias
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            # Imprimir el ID de la instancia y su estado
            instance_id = instance['InstanceId']
            instance_state = instance['State']['Name']
            print(f'Instance ID: {instance_id}, State: {instance_state}')
        
def create_bucket(bucket_name):
    s3_client = boto3.client('s3')
    
    # Verificar si el bucket ya existe
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f'El bucket "{bucket_name}" ya existe, no se realizará ninguna acción.')
    except ClientError as e:
        # Si el error es porque no existe, lo creamos
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            # Crear el bucket si no existe
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
            print(f'Bucket "{bucket_name}" creado exitosamente.')
        else:
            # Otro error, imprimir mensaje
            print(f'Error al verificar el bucket: {e}')

def list_buckets():
    s3 = boto3.resource('s3')

    # Listar todos los buckets de la cuenta
    for bucket in s3.buckets.all():
        print(bucket.name)
        
def upload_file_to_bucket(bucket_name, file_name, object_name=None):
    s3_client = boto3.client('s3')
    
    # Si no se especifica un nombre de objeto, se usa el nombre del archivo
    if object_name is None:
        object_name = file_name

    try:
        # Subir el archivo al bucket, dentro de la carpeta especificada
        s3_client.upload_file(file_name, bucket_name, object_name)
        print(f'{file_name} ha sido subido al bucket {bucket_name} como {object_name}')
    except Exception as e:
        print(f'Error subiendo archivo: {e}')
        
def list_documentdb_clusters():
    # Crear el cliente de DocumentDB
    docdb_client = boto3.client('docdb')
    
    try:
        # Llamar a la API de DocumentDB para obtener los clusters
        response = docdb_client.describe_db_clusters()
        
        # Recorrer la respuesta para obtener los detalles de los clusters
        for cluster in response['DBClusters']:
            cluster_id = cluster['DBClusterIdentifier']
            status = cluster['Status']
            print(f'Cluster ID: {cluster_id}, Status: {status}')
    except ClientError as e:
        print(f'Error al obtener los clusters de DocumentDB: {e}')
        
def download_documentdb_ca_certificate():
    url = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
    file_path = "global-bundle.pem"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verifica si hubo algún error durante la descarga

        # Guardar el contenido del certificado en un archivo
        with open(file_path, 'wb') as f:
            f.write(response.content)

        print(f'Certificado descargado correctamente y guardado en {file_path}')
    except requests.exceptions.RequestException as e:
        print(f'Error al descargar el certificado: {e}')
        
def connect_to_documentdb():
    # Configura los detalles de conexión
    username = "admin1"
    password = "pepe1234"  # Reemplaza con tu contraseña
    host = "cluster1documentdb.cluster-cjq0yqk464hb.eu-west-3.docdb.amazonaws.com"
    ca_file = "global-bundle.pem"  # El certificado que descargaste

    # Crear la cadena de conexión
    connection_string = (
        f"mongodb://{username}:{password}@{host}:27017/"
        "?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0"
        "&readPreference=secondaryPreferred&retryWrites=false"
    )

    try:
        # Crear la conexión al cluster de DocumentDB
        client = MongoClient(connection_string, ssl=True, tlsCAFile=ca_file)
        print("Conectado exitosamente a DocumentDB")

        # Listar las bases de datos en el cluster
        db_list = client.list_database_names()
        print(f"Bases de datos en el cluster: {db_list}")

    except Exception as e:
        print(f"Error al conectarse a DocumentDB: {e}")
        
def send_message_to_sqs(queue_name, message_body):
    # Crear el cliente de SQS
    sqs = boto3.client('sqs')
    
    try:
        # Obtener la URL de la cola SQS por su nombre
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        
        # Enviar el mensaje a la cola
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )
        print(f"Mensaje enviado a la cola {queue_name}: {message_body}")
    
    except ClientError as e:
        print(f"Error al enviar mensaje a la cola {queue_name}: {e}")
        
def receive_messages_from_sqs(queue_name, max_messages):
    # Crear el cliente de SQS
    sqs = boto3.client('sqs')
    
    try:
        # Obtener la URL de la cola SQS por su nombre
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        
        # Leer mensajes de la cola
        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=10  # Tiempo de espera para recibir mensajes
        )
        
        # Comprobar si hay mensajes
        if 'Messages' in messages:
            for message in messages['Messages']:
                print(f"Mensaje recibido de la cola {queue_name}: {message['Body']}")
                
                # Borrar el mensaje después de leerlo
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                print(f"Mensaje borrado de la cola {queue_name}")
        else:
            print(f"No se encontraron mensajes en la cola {queue_name}")
    
    except ClientError as e:
        print(f"Error al recibir mensajes de la cola {queue_name}: {e}")

if __name__ == "__main__":
    list_ec2_instances()
    
    bucket_name = 'mi-bucket-creado-desde-python-112135376'
    create_bucket(bucket_name=bucket_name)
    
    list_buckets()
    
    file_path = 'data/subir_al_bucket.txt'
    object_name = 'carpeta_subida_desde_vscode/subir_al_bucket.txt'
    upload_file_to_bucket(bucket_name, file_path, object_name)
    
    # no dejaba crear el cluster DocumentDB en eu-north-1, por lo que tuve que crearlo en eu-west-3 y cambiar en .env: AWS_DEFAULT_REGION='eu-west-3'
    # list_documentdb_clusters()
    # download_documentdb_ca_certificate()
    # connect_to_documentdb()
    
    # Enviar mensaje a cola de SQS
    #send_message_to_sqs(queue_name='cola1', message_body='Este es un mensaje de prueba')

    # Leer mensajes de cola de SQS
    #receive_messages_from_sqs(queue_name='cola1', max_messages=3)