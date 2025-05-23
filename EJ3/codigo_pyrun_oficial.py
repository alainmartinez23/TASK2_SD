import lithops
import boto3
import json
import re

BUCKET_NAME = 'sustituir'
INSULTS_FILE = 'insultos.json'
INPUT_FILES = [
    'input/texto1.txt',
    'input/texto2.txt',
    'input/texto3.txt',
    'input/texto4.txt',
    'input/texto5.txt'
]
OUTPUT_PREFIX = 'output/'

# Cargar insultos desde S3
def get_insults():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=INSULTS_FILE)
    return json.loads(obj['Body'].read().decode('utf-8'))

# MAP: censura y cuenta insultos
def map_function(file_key, insults):
    import lithops
    import re

    print(f"[DEBUG] Procesando archivo: {file_key}")
    storage = lithops.Storage()
    texto = storage.get_object(bucket=BUCKET_NAME, key=file_key).decode('utf-8')

    insult_pattern = re.compile(r'\b(' + '|'.join(map(re.escape, insults)) + r')\b', re.IGNORECASE)
    
    # Buscar y censurar
    matches = insult_pattern.findall(texto)
    num_censurados = len(matches)
    censurado = insult_pattern.sub("CENSORED", texto)

    # Guardar texto censurado en S3
    output_key = file_key.replace('input/', OUTPUT_PREFIX)
    storage.put_object(bucket=BUCKET_NAME, key=output_key, body=censurado)

    print(f"[DEBUG] Terminado {file_key} con {num_censurados} insultos censurados")
    return num_censurados

# REDUCE: suma total
def reduce_function(results):
    return sum(results)

# Lanzar con Lithops
def main():
    insults = get_insults()
    fexec = lithops.FunctionExecutor(runtime_memory=1024)
    fexec.map_reduce(map_function, INPUT_FILES, reduce_function, extra_args=(insults,))
    total = fexec.get_result()
    print(f"\nTotal de insultos censurados: {total}")

if __name__ == "__main__":
    main()