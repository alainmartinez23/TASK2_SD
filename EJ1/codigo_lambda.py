import json
import boto3
import re

s3 = boto3.client('s3')

# sustituir valores
BUCKET_NAME = ''
INSULTS_FILE = ''
FILTERED_FILE = ''

# Cargar JSON desde S3
def cargar_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        return []  # Si no existe, se parte de vacío

# Guardar JSON en S3
def guardar_json(key, data):
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(data))

# Filtrar texto con lista de insultos
def censurar_texto(texto, insultos):
    for insulto in insultos:
        texto = re.sub(rf"\b{re.escape(insulto)}\b", "CENSORED", texto, flags=re.IGNORECASE)
    return texto

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            mensaje = json.loads(record['body'])
            action = mensaje.get("action")
            data = mensaje.get("data", "")
            
            if action == "add_insult":
                insultos = cargar_json(INSULTS_FILE)
                if data not in insultos:
                    insultos.append(data)
                    guardar_json(INSULTS_FILE, insultos)
                    print(f"Insulto '{data}' añadido a la lista.")
                else:
                    print(f"Insulto '{data}' ya estaba en la lista.")

            elif action == "filter_text":
                insultos = cargar_json(INSULTS_FILE)
                texto_censurado = censurar_texto(data, insultos)
                textos_filtrados = cargar_json(FILTERED_FILE)
                textos_filtrados.append(texto_censurado)
                guardar_json(FILTERED_FILE, textos_filtrados)
                print("Texto filtrado y guardado en S3.")

            else:
                print("Acción no reconocida:", action)

        except Exception as e:
            print("Error procesando mensaje:", str(e))
