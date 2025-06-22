import boto3
import pandas as pd
import json
import io

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # Parámetros del archivo de entrada
    bucket_name = 'my-bucket-s3upeu'
    input_key = 'DataCovid_LimpioCLOUD.xlsx'
    output_key = 'salida/DataCovid_PROCESADO.json'

    # Leer el archivo Excel desde S3
    obj = s3.get_object(Bucket=bucket_name, Key=input_key)
    df = pd.read_excel(io.BytesIO(obj['Body'].read()))

    # 🔧 APLICAR REGLAS (ejemplo de las primeras 5, puedes añadir más)
    df = df.dropna()  # Regla 1: eliminar filas con NaN
    df.columns = df.columns.str.strip()  # Regla 2: limpiar nombres de columnas
    df = df[df['Edad'] >= 0]  # Regla 3: eliminar edades negativas
    df['Fallecido'] = df['Fallecido'].fillna('NO')  # Regla 4: reemplazar nulos
    df = df[df['Departamento'].notnull()]  # Regla 5: eliminar sin departamento

    # Convertir a JSON
    json_data = df.to_dict(orient='records')
    json_str = json.dumps(json_data, indent=2)

    # Subir archivo JSON resultante a S3
    s3.put_object(Bucket=bucket_name, Key=output_key, Body=json_str)

    return {
        'statusCode': 200,
        'body': f'Datos procesados y guardados como {output_key}'
    }
