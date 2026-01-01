
import polars as pl
from extract_minio import extract_json


def transform():
    jobs_df = extract_json()
    print(jobs_df)
    
transform()