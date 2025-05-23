import json
import boto3
import re
import time
import datetime

s3 = boto3.client('s3')

# Sustituir valores
BUCKET_NAME = ''
INSULTS_FILE = ''
FILTERED_FILE = ''

def cargar_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        return []

def guardar_json(key, data):
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(data), ContentType='application/json')

def censurar_texto(texto, insultos):
    for insulto in insultos:
        texto = re.sub(rf"\b{re.escape(insulto)}\b", "CENSORED", texto, flags=re.IGNORECASE)
    return texto

def lambda_handler(event, context):
    print("Lambda ID:", context.aws_request_id, datetime.datetime.now())

    try:
        print("[DEBUG] Evento recibido:", event)

        for record in event.get("Records", []):
            raw_body = record.get("body", "")
            mensaje = json.loads(raw_body)
            print("[DEBUG] Mensaje procesado:", mensaje)

            action = mensaje.get("action")
            data = mensaje.get("data", "")

            if action == "add_insult":
                insultos = cargar_json(INSULTS_FILE)
                if data not in insultos:
                    insultos.append(data)
                    guardar_json(INSULTS_FILE, insultos)
                    print(f"[OK] Insulto '{data}' añadido.")
                else:
                    print(f"[INFO] Insulto '{data}' ya existe.")

            elif action == "filter_text":
                insultos = cargar_json(INSULTS_FILE)
                censurado = censurar_texto(data, insultos)
                textos = cargar_json(FILTERED_FILE)
                textos.append(censurado)
                guardar_json(FILTERED_FILE, textos)
                print("[OK] Texto filtrado guardado.")

            else:
                print(f"[WARN] Acción no reconocida: {action}")
        time.sleep(3)
    except Exception as e:
        print("Error procesando el evento:", str(e))
