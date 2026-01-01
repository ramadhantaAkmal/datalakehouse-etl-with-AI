import os
import sys
import datetime
from minio.error import S3Error
from dotenv import load_dotenv
from utils.minio_util import connect_minio

utils_path = '/opt/airflow/lib'
sys.path.append(utils_path)

load_dotenv()

current_date = datetime.date.today()

bucket_name = os.getenv('BUCKET_NAME')
object_name = f"jobs-result-{current_date}.json"
file_path = f"/opt/airflow/lib/jobs-result-weekly/jobs-result-{current_date}.json"
    
def upload_file_to_minio():
    """
    Uploads a file to a MinIO bucket.
    """
    try:
        # Create a client with the MinIO server playground, its access key
        # and secret key.
        client = connect_minio()

        # Make the bucket if it doesn't exist.
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # Upload the file.
        client.fput_object(
            bucket_name,
            object_name,
            file_path,
        )
        print(f"'{file_path}' is successfully uploaded as '{object_name}' to bucket '{bucket_name}'.")

    except S3Error as e:
        print(f"Error uploading file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
