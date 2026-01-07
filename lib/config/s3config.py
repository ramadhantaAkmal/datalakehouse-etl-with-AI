import os
from dotenv import load_dotenv

load_dotenv("../.env")

access_key = os.getenv('MINIO_ACCESS_KEY')
secret_key = os.getenv('MINIO_SECRET_KEY')
minio_endpoint = os.getenv('MINIO_ENDPOINT')

storage_options = {
    "aws_access_key_id": access_key,  # your MinIO access key
    "aws_secret_access_key": secret_key,  # your MinIO secret key
    "endpoint_url": f"http://{minio_endpoint}",  # MinIO endpoint
    "allow_http": "true",  # for http (use https in prod)
}