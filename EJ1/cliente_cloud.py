import boto3
import json
import random
import uuid

# AWS Configuración
sqs = boto3.client('sqs', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

# sustituir valores
QUEUE_NAME = '' 
BUCKET_NAME = ''
INSULTS_FILE = ''
FILTERED_FILE = ''

# Crear o recuperar URL de la cola
def get_queue_url():
    response = sqs.create_queue(QueueName=QUEUE_NAME)
    return response['QueueUrl']

# Enviar mensaje a SQS
def enviar_peticion(action, data=""):
    queue_url = get_queue_url()
    mensaje = {
        "id": str(uuid.uuid4()),
        "action": action,
        "data": data
    }
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(mensaje))
    print("Petición enviada a la nube.")

# Leer archivo JSON desde S3
def leer_json_s3(filename):
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=filename)
        contenido = response['Body'].read().decode('utf-8')
        return json.loads(contenido)
    except s3.exceptions.NoSuchKey:
        return [] if 'filtered' in filename else []

# Menú principal
def menu():
    while True:
        print("""
1. Añadir un insulto.
2. Ver toda la lista de insultos disponibles.
3. Obtener un insulto aleatorio.
4. Filtrar un texto.
5. Ver textos filtrados.
6. Salir.
""")
        opcion = input("Seleccione una opción: ")

        if opcion == "1":
            insulto = input("Ingrese un insulto: ")
            enviar_peticion("add_insult", insulto)

        elif opcion == "2":
            insultos = leer_json_s3(INSULTS_FILE)
            print("Lista de insultos:", insultos)

        elif opcion == "3":
            insultos = leer_json_s3(INSULTS_FILE)
            if insultos:
                print("Insulto aleatorio:", random.choice(insultos))
            else:
                print("No hay insultos disponibles.")

        elif opcion == "4":
            texto = input("Ingrese un texto: ")
            enviar_peticion("filter_text", texto)

        elif opcion == "5":
            textos = leer_json_s3(FILTERED_FILE)
            print("Textos filtrados:")
            for t in textos:
                print("-", t)

        elif opcion == "6":
            print("Saliendo...")
            break

        else:
            print("Opción inválida.")

# Iniciar el cliente
print("Cliente iniciado (AWS SQS + S3)...")
menu()
