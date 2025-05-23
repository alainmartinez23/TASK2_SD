import boto3
import json
import uuid
import random
import time

# Configuración
sqs = boto3.client('sqs', region_name='us-east-1')

# sustituir
QUEUE_NAME = ''

# Frases base (mezcla de normales e insultantes)
BASE_TEXTOS = [
    "Eres un payaso de los grandes",
    "Qué bonito día hace hoy",
    "Ese payaso no sabe nada",
    "Hola, ¿cómo estás?",
    "Qué gilipollas te has vuelto últimamente",
    "Programar es divertido",
    "Este código está bien hecho",
    "No seas tan subnormal",
    "Voy al gimnasio luego",
    "Qué bien lo haces"
]

# Obtener URL de la cola
def get_queue_url():
    response = sqs.create_queue(QueueName=QUEUE_NAME)
    return response['QueueUrl']

# Enviar un mensaje a la cola
def enviar_texto(queue_url, texto):
    mensaje = {
        "id": str(uuid.uuid4()),
        "action": "filter_text",
        "data": texto
    }
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(mensaje))

# Stress test con medición de tiempo
def stress_test(n=700):
    queue_url = get_queue_url()
    print(f"Enviando {n} textos a la cola...")

    start_time = time.time()

    for i in range(n):
        texto = random.choice(BASE_TEXTOS)
        enviar_texto(queue_url, texto)
        if (i + 1) % 100 == 0 or i == n - 1:
            print(f"[{i+1}/{n}] Enviado")

    end_time = time.time()
    duration = round(end_time - start_time, 2)

    print(f"\nStress test completado en {duration} segundos.")
    print(f"Promedio: {round(duration / n, 4)} segundos por mensaje.")

# Ejecutar
if __name__ == "__main__":
    stress_test()
