import json
import io
import os
import sys
import datetime
import polars as pl

from dotenv import load_dotenv

utils_path = '../utils'
sys.path.append(utils_path)

from minio_util import connect_minio

load_dotenv("../.env")

current_date = datetime.date.today()

bucket_name = os.getenv('BUCKET_NAME')
object_name = f"jobs-result-{current_date}.json"

def extract_json():
    client = connect_minio()
    
    response = client.get_object(bucket_name, object_name)

    buffer = io.BytesIO(response.read())
    df = pl.read_json(buffer)
    response.close()
    response.release_conn()
    return df
