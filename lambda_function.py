import boto3
import pandas as pd
import json
import io

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # Parámetros del archivo de entrada
    bucket_name = 'my-bucket-s3upeu'
    input_key = 'DataCovid_LimpioCLOUD.xlsx'
    output_key = 'salida/archivo_transformado.json'

    # Leer archivo Excel desde S3
    obj = s3.get_object(Bucket=bucket_name, Key=input_key)
    df = pd.read_excel(io.BytesIO(obj['Body'].read()))

    # APLICAR REGLAS AQUÍ (solo 1 como ejemplo)
    df = df.dropna(subset=['Departamento'])  # Regla 1: eliminar vacíos en columna clave

    # Convertir a JSON
    json_data = df.to_dict(orient='records')
    json_str = json.dumps(json_data)

    # Guardar en S3
    s3.put_object(Bucket=bucket_name, Key=output_key, Body=json_str)

    return {
        'statusCode': 200,
        'body': f'Archivo procesado y guardado en s3://{bucket_name}/{output_key}'
    }
