import boto3
import os
import pandas as pd

#Configuração de acesso a AWS
df_rootkey = pd.read_csv("rootkey.csv")
df_accesskey = df_rootkey["Access key ID"].values[0]
df_secret_key = df_rootkey["Secret access key"].values[0]

# Configurações
BUCKET_NAME = 'projeto-lakehouse'
CADASTROS_PATH = 'data/raw_data/cadastros'
PEDIDOS_PATH = 'data/raw_data/pedidos'

# Inicialize o cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id= df_accesskey,
    aws_secret_access_key= df_secret_key,
    region_name='eu-west-1'
)

def upload_files(folder_path, s3_prefix):
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}/{file}"
                print(f"Uploading {file_path} to s3://{BUCKET_NAME}/{s3_key}")
                s3_client.upload_file(file_path, BUCKET_NAME, s3_key)

# Envio dos arquivos para as pastas específicas no bucket S3
upload_files(CADASTROS_PATH, 'lakehouse-clientes')
upload_files(PEDIDOS_PATH, 'lakehouse-pedidos')

