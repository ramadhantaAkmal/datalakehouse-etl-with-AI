import polars as pl
import datetime
from jobs_scrape import ingest
from utils.api_util import extract_with_n8n
from config.s3config import storage_options
from store_json import store_json
from iceberg.iceberg_append import transform_load_brz
from iceberg.iceberg_transform import transform_load_slv_gld

def main():
    current_date = datetime.date.today()
    df = ingest()
    df = df.rename({"job_type": "schedule_type"})
    df = df.rename({"company": "company_name"})
    df = extract_with_n8n(df)
    df = df.with_columns(pl.lit(current_date).alias("ingestion_date"))
    print("Data Extracted successfully")
    
    json_string = df.write_json(file=None)
    store_json(json_string)
    
    df.drop_in_place('job_url')
     
    df.write_parquet(
        "s3://jobs-results-lake/",
        storage_options=storage_options,
        compression="zstd",
        partition_by="ingestion_date"
    )
    
    transform_load_brz(df)
    transform_load_slv_gld(df)
    print("Data transformed and loaded successfully")

main()