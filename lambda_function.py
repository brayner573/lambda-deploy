import json
import boto3
import pandas as pd
import io
import unicodedata

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'my-bucket-s3upeu'  # ← reemplaza si es otro
    file_key = 'DataCovid_LimpioCLOUD.xlsx'
    
    print(f"🟡 Iniciando lectura desde S3: {bucket_name}/{file_key}")
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_excel(response['Body'])
    print(f"✅ Archivo leído: {df.shape[0]} filas, {df.shape[1]} columnas")

    # Reglas de limpieza
    print("🧹 Aplicando reglas...")

    df.drop_duplicates(inplace=True)  # Regla 1
    df.dropna(how='all', inplace=True)  # Regla 2
    df.fillna('Desconocido', inplace=True)  # Regla 3
    df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)  # Regla 4
    df.replace(['none', 'nan', 'None', 'NaN'], 'Desconocido', inplace=True)  # Regla 5
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # Regla 6

    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.strftime('%Y-%m-%d')  # Regla 7

    df = df.applymap(lambda x: ''.join(e for e in x if e.isalnum() or e.isspace()) if isinstance(x, str) else x)  # Regla 8

    for col in df.select_dtypes(include=['int64', 'object']):
        try:
            df[col] = df[col].astype(float)  # Regla 9
        except:
            pass

    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]  # Regla 10
    df = df.loc[:, ~df.columns.duplicated()]  # Regla 11
    df = df[df.isnull().mean(axis=1) < 0.5]  # Regla 12
    df = df.applymap(lambda x: None if isinstance(x, (int, float)) and x == 0 else x)  # Regla 13
    df = df.applymap(lambda x: unicodedata.normalize('NFKD', x).encode('ascii','ignore').decode('utf-8') if isinstance(x, str) else x)  # Regla 14
    df = df.applymap(lambda x: 0 if isinstance(x, (int, float)) and x < 0 else x)  # Regla 15

    if 'departamento' in df.columns:
        df = df.groupby('departamento').first().reset_index()  # Regla 16

    df = df.head(10000)  # Regla 17
    df = df[sorted(df.columns)]  # Regla 18
    df = df.loc[:, ~df.columns.str.contains('id')]  # Regla 19
    df['procesado'] = True  # Regla 20

    print("✅ Reglas aplicadas exitosamente.")
    
    # Subida a S3
    output_key = 'cleaned/DataCovid_CLEANED.json'
    json_buffer = io.StringIO()
    df.to_json(json_buffer, orient='records', force_ascii=False)

    s3.put_object(Bucket=bucket_name, Key=output_key, Body=json_buffer.getvalue(), ContentType='application/json')
    print(f"📤 JSON subido a: s3://{bucket_name}/{output_key}")

    return {
        'statusCode': 200,
        'body': f'Archivo limpio guardado en s3://{bucket_name}/{output_key}'
    }
