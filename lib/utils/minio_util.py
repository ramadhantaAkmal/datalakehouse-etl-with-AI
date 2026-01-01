import os
from minio import Minio
from dotenv import load_dotenv

load_dotenv("../.env")

def connect_minio()->Minio:
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    
    client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False 
        )
    
    return client
            