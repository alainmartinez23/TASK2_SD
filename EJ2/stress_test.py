import boto3
import json
import uuid

# Configuración AWS (ajusta si cambias de región o nombres)
sqs = boto3.client('sqs', region_name='us-east-1')
QUEUE_NAME = '' # sustituir

# Recuperar URL de la cola
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

# Stress test
def stress_test(TOTAL_INSULTOS):
    print(f"Enviando {TOTAL_INSULTOS} insultos a SQS...")

    for i in range(TOTAL_INSULTOS):
        insulto = f"insulto-{i}"
        enviar_peticion("add_insult", insulto)

    print("Stress test finalizado.")

if __name__ == "__main__":
    TOTAL_INSULTOS = 200

    stress_test(TOTAL_INSULTOS)
