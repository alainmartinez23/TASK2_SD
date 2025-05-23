import boto3
import time
import json

sqs = boto3.client('sqs', region_name='us-east-1')
lambda_client = boto3.client('lambda', region_name='us-east-1')

def stream(function, maxfunc, queue_url, intervalo=2):
    print("Escuchando la cola...")

    while True:
        try:
            # Ver cuántos mensajes hay en la cola
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            disponibles = int(attrs['Attributes']['ApproximateNumberOfMessages'])
            n_lambdas = min(disponibles, maxfunc)

            if n_lambdas == 0:
                print("No hay mensajes nuevos.")
                time.sleep(intervalo)
                continue

            print(f"Cola con {disponibles} mensajes. {n_lambdas} Lambdas...")

            for _ in range(n_lambdas):
                # Recibir mensaje real de la cola
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=0
                )

                if 'Messages' not in response:
                    continue

                mensaje = response['Messages'][0]
                receipt = mensaje['ReceiptHandle']
                body = mensaje['Body']

                # Enviar mensaje a Lambda en formato esperado
                event = {
                    "Records": [
                        {
                            "body": body
                        }
                    ]
                }

                lambda_client.invoke(
                    FunctionName=function,
                    InvocationType='Event',
                    Payload=json.dumps(event).encode('utf-8')
                )

                # Eliminar mensaje de la cola para evitar duplicados
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)

        except Exception as e:
            print("Error en stream():", str(e))

        time.sleep(intervalo)

# Ejemplo de uso
if __name__ == "__main__":
    cola_url = sqs.get_queue_url(QueueName='sustituir')['QueueUrl'] #sustituir valores
    stream(function='sustituir', maxfunc=10, queue_url=cola_url) #sustituir valores
