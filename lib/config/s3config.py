# import os
# from dotenv import load_dotenv

# load_dotenv("../.env")

access_key = "admin"
secret_key = "password"
minio_endpoint = "localhost:9000"

storage_options = {
    "aws_access_key_id": access_key,  # your MinIO access key
    "aws_secret_access_key": secret_key,  # your MinIO secret key
    "endpoint_url": f"http://{minio_endpoint}",  # MinIO endpoint
    "allow_http": "true",  # for http (use https in prod)
}