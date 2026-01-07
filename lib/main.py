import polars as pl
import datetime
from jobs_scrape import scrape_jobs
from extract import extract_essential_data
from utils.api_util import extract_with_n8n
from config.s3config import storage_options
from store_json import store_json

def main():
    i=0
    next_page_token="" 
    df=pl.DataFrame()
    current_date = datetime.date.today()
    
    #Limit the ingest data loop to only 3 times
    while i < 3:
        data = scrape_jobs(next_page_token)
        next_page_token=data["serpapi_pagination"]["next_page_token"]
        clean_data = extract_essential_data(data)
        df = pl.concat([df, clean_data], how="vertical")
        i += 1
    df = extract_with_n8n(df)
    
    df = df.with_columns(pl.lit(current_date).alias("ingestion_date"))
    
    json_string = df.write_json(file=None)
    store_json(json_string)
    
    df.write_parquet(
        "s3://jobs-results-lake/",
        storage_options=storage_options,
        compression="zstd",
        partition_by="ingestion_date"
    )
